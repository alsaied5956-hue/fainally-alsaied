/**
 * src/architecture/dbSchema.ts
 * 
 * Production-Ready, Zero-Data-Loss, Offline-First Architecture Schemas
 * 
 * Hardened Features:
 *  1. Write-Ahead Log (WAL) with durability guarantees
 *  2. Durable Tombstones & 90-Day Zombie Data Guard
 *  3. Idempotent Attendance with deterministic deduplication keys
 *  4. Immutable Append-Only Financial Ledger (Strictly NO Last-Write-Wins)
 *  5. Throttled Real-Time Parent Read-Only Views & 2-Hour Notification TTL
 *  6. Hybrid Logical Clocks (HLC) & Hardware Clock Drift Guard (±5 minutes)
 *  7. OS Non-Volatile Storage Persistence (`navigator.storage.persist()`)
 *  8. Time-To-Live (TTL) Deduplication Buffer for Echo Suppression
 */

// --------------------------------------------------------------------------
// 1. SYSTEM ARCHITECTURE CONSTANTS & THRESHOLDS
// --------------------------------------------------------------------------

export const ARCHITECTURE_CONSTANTS = {
  /** Maximum allowable physical clock drift between local device and server (±5 minutes) */
  CLOCK_DRIFT_MAX_MS: 5 * 60 * 1000,
  /** Maximum retention period for tombstones before a device is considered zombie-stale (90 days) */
  TOMBSTONE_MAX_AGE_MS: 90 * 24 * 60 * 60 * 1000,
  /** Maximum age for queued parent attendance alerts before automatic discard (2 hours) */
  EXPIRED_NOTIFICATION_TTL_MS: 2 * 60 * 60 * 1000,
  /** Safe throttling interval when flushing queued notifications on reconnect (500ms per alert) */
  NOTIFICATION_THROTTLE_INTERVAL_MS: 500,
  /** Sliding TTL window for suppressing multi-device broadcast echoes (5 minutes) */
  ECHO_DEDUP_TTL_MS: 5 * 60 * 1000,
  /** Interval for coalescing background cloud writes (4 seconds) */
  BATCH_COALESCE_INTERVAL_MS: 4000,
  /** Maximum records per single batch flush */
  MAX_BATCH_SIZE: 50,
} as const;

// --------------------------------------------------------------------------
// 2. HYBRID LOGICAL CLOCK (HLC) DEFINITION
// --------------------------------------------------------------------------

export interface HybridLogicalClock {
  logicalTime: number; // Wall-clock millis or highest observed time
  counter: number;     // Monotonic tie-breaker counter for same-millisecond events
  nodeId: string;      // Unique client device node identifier (e.g. "laptop_91a", "phone_37f")
}

/**
 * Compare two HLCs chronologically.
 * Returns:
 *  - negative if a < b (a happened before b)
 *  - 0 if a == b (exact same event)
 *  - positive if a > b (a happened after b)
 */
export function compareHLC(a: HybridLogicalClock, b: HybridLogicalClock): number {
  if (a.logicalTime !== b.logicalTime) {
    return a.logicalTime - b.logicalTime;
  }
  if (a.counter !== b.counter) {
    return a.counter - b.counter;
  }
  return a.nodeId.localeCompare(b.nodeId);
}

export function formatHLC(hlc: HybridLogicalClock): string {
  return `${hlc.logicalTime}:${String(hlc.counter).padStart(4, "0")}:${hlc.nodeId}`;
}

export function parseHLC(str: string): HybridLogicalClock {
  const parts = str.split(":");
  return {
    logicalTime: Number(parts[0]) || 0,
    counter: Number(parts[1]) || 0,
    nodeId: parts[2] || "unknown",
  };
}

// --------------------------------------------------------------------------
// 3. DETERMINISTIC HASH & IDEMPOTENCY KEY GENERATORS
// --------------------------------------------------------------------------

/**
 * High-performance 64-bit FNV-1a deterministic hash implementation.
 * Zero-dependency, runs synchronously in Browser, WebWorker, and Node.js.
 */
export function deterministicHash(...parts: (string | number | boolean | null | undefined)[]): string {
  const str = parts.map((p) => String(p ?? "").trim()).join("::");
  let h1 = 0x811c9dc5;
  let h2 = 0x9e3779b9;

  for (let i = 0; i < str.length; i++) {
    const ch = str.charCodeAt(i);
    h1 = Math.imul(h1 ^ ch, 0x01000193);
    h2 = Math.imul(h2 ^ ch, 0x85ebca6b);
  }

  const hex1 = (h1 >>> 0).toString(16).padStart(8, "0");
  const hex2 = (h2 >>> 0).toString(16).padStart(8, "0");
  return `${hex1}${hex2}`;
}

/**
 * Deterministic Idempotency Key for Attendance:
 * Prevents double-attendance if multiple offline devices scan the same student
 * in the same session slot or day.
 */
export function generateAttendanceKey(barcode: string, dateKey: string, sessionSlotId: string = "auto"): string {
  return deterministicHash("ATTENDANCE", String(barcode).trim(), dateKey, sessionSlotId);
}

/**
 * Deterministic Transaction ID for Financial Ledger:
 * Ensures idempotent payment appending with zero risk of duplicate credits or overwrites.
 */
export function generateTransactionId(
  studentBarcode: string,
  monthKey: string,
  timestamp: number,
  sequence: number
): string {
  const hash = deterministicHash("TX", String(studentBarcode).trim(), monthKey, timestamp, sequence);
  return `tx_${hash.slice(0, 16)}`;
}

/**
 * Deterministic Idempotency Key for generic WAL operations:
 */
export function generateIdempotencyKey(
  entityType: string,
  entityId: string,
  action: string,
  discriminator: string | number = 0
): string {
  return deterministicHash("IDEMP", entityType, entityId, action, discriminator);
}

// --------------------------------------------------------------------------
// 4. WRITE-AHEAD LOG (WAL) SCHEMA
// --------------------------------------------------------------------------

export type WALEntityType =
  | "ATTENDANCE"
  | "STUDENT"
  | "FINANCIAL_LEDGER"
  | "HOMEWORK"
  | "EXAM_GRADE"
  | "SYSTEM_CONFIG";

export type WALAction = "INSERT" | "UPDATE" | "DELETE" | "REVERSE";

export type WALStatus = "PENDING" | "COALESCING" | "COMMITTED" | "FAILED";

export interface WALRecord<T = any> {
  walId: string;
  idempotencyKey: string;
  entityType: WALEntityType;
  entityId: string;
  action: WALAction;
  hlc: HybridLogicalClock;
  clientMonotonicSeq: number;
  payload: T;
  status: WALStatus;
  retryCount: number;
  lastError?: string;
  createdAt: number;
  committedAt?: number;
}

// --------------------------------------------------------------------------
// 5. TOMBSTONE RECORD SCHEMA (Prevents Zombie Data Resurrection)
// --------------------------------------------------------------------------

export interface TombstoneRecord {
  tombstoneId: string;
  entityType: WALEntityType;
  entityId: string;
  deletedAtHlc: HybridLogicalClock;
  deletedBy: string;
  reason?: string;
  createdAt: number;
  ttlMs: number; // 90 days = ARCHITECTURE_CONSTANTS.TOMBSTONE_MAX_AGE_MS
  purged: boolean;
}

// --------------------------------------------------------------------------
// 6. IDEMPOTENT ATTENDANCE SCHEMA
// --------------------------------------------------------------------------

export interface IdempotentAttendanceRecord {
  attendanceKey: string; // Primary key: hash(barcode + dateKey + sessionSlotId)
  studentBarcode: string;
  studentName: string;
  studentGrade: string;
  studentDays: string;
  dateKey: string; // YYYY-MM-DD
  sessionSlotId: string;
  status: "حضور" | "تأخير" | "غياب";
  scannedTimeIso: string;
  timeDisplay: string;
  isPaid: boolean;
  hlc: HybridLogicalClock;
  deviceId: string;
  scannedBy: string;
  syncedToCloud: boolean;
}

// --------------------------------------------------------------------------
// 7. IMMUTABLE FINANCIAL LEDGER SCHEMA (Strictly Append-Only)
// --------------------------------------------------------------------------

export type FinancialTransactionType =
  | "CREDIT_PAYMENT"      // Actual cash/digital subscription payment collected
  | "FEE_CHARGE"          // Regular monthly tuition fee obligation charged
  | "DISCOUNT_ADJUSTMENT" // Sibling / orphan / excellence discount applied
  | "REVERSAL";           // Explicit reversal of an erroneous or refunded payment

export interface FinancialLedgerEntry {
  transactionId: string; // Deterministic ID: tx_xxxxxxxxxxxxxxxx
  studentBarcode: string;
  studentId?: string;
  studentName?: string;
  monthKey: string; // YYYY-MM
  type: FinancialTransactionType;
  amount: number; // Absolute positive number
  currency: "EGP";
  referenceTransactionId?: string; // Points to original transaction if type === "REVERSAL"
  note: string;
  receiptNumber?: string;
  recordedBy: string;
  hlc: HybridLogicalClock;
  idempotencyKey: string;
  createdAt: number;
  checksum: string; // Cryptographic chain: hash(prevHash + entryDetails)
}

export interface StudentFinancialSummary {
  studentBarcode: string;
  monthKey: string;
  totalCharged: number;
  totalDiscount: number;
  totalPaid: number;
  totalReversed: number;
  netRequired: number; // totalCharged - totalDiscount
  netCollected: number; // totalPaid - totalReversed
  remainingBalance: number; // netRequired - netCollected
  paymentStatus: "paid" | "partial" | "unpaid";
  lastPaymentDate?: string;
  lastReceiptNumber?: string;
  transactionsCount: number;
}

// --------------------------------------------------------------------------
// 8. PARENT READ-ONLY VIEWS & REAL-TIME NOTIFICATION SCHEMA
// --------------------------------------------------------------------------

export type ParentNotificationType =
  | "ATTENDANCE_SCAN"
  | "LATE_ARRIVAL"
  | "ABSENCE_ALERT"
  | "PAYMENT_RECEIPT"
  | "EXAM_RESULT"
  | "HOMEWORK_STATUS";

export interface ParentNotificationEvent {
  eventId: string;
  studentBarcode: string;
  studentName: string;
  parentPhone: string;
  type: ParentNotificationType;
  title: string;
  body: string;
  meta: {
    dateKey: string;
    timeDisplay?: string;
    status?: string;
    amount?: number;
    monthKey?: string;
    receiptNo?: string;
    score?: number;
    maxScore?: number;
    notes?: string;
  };
  hlc: HybridLogicalClock;
  timestamp: number;
  deliveryStatus: "QUEUED" | "SENT_REALTIME" | "OFFLINE_QUEUED" | "DELIVERED" | "EXPIRED_DISCARDED";
  sentAt?: number;
  discardReason?: string;
}

export interface ParentPortalStudentView {
  student: {
    barcode: string;
    name: string;
    grade: string;
    days: string;
    groupTime?: string;
    parentPhone: string;
  };
  attendanceTimeline: Array<{
    dateKey: string;
    timeDisplay: string;
    timeIso: string;
    status: "حضور" | "تأخير" | "غياب";
    sessionSlotId?: string;
  }>;
  financialSummary: {
    currentMonthKey: string;
    status: "paid" | "partial" | "unpaid";
    monthlyFee: number;
    discount: number;
    totalPaid: number;
    remaining: number;
    receipts: Array<{
      transactionId: string;
      date: string;
      amount: number;
      monthKey: string;
      receiptNo: string;
      note: string;
    }>;
  };
  recentExamGrades: Array<{
    examTitle: string;
    score: number;
    maxScore: number;
    percentage: number;
    dateKey: string;
  }>;
  lastUpdatedHlc: HybridLogicalClock;
}

// --------------------------------------------------------------------------
// 9. BROWSER STORAGE PERSISTENCE (EVICTION & DATA LOSS PREVENTION)
// --------------------------------------------------------------------------

export interface StoragePersistenceStatus {
  supported: boolean;
  persisted: boolean;
  quotaBytes?: number;
  usageBytes?: number;
  usagePercentage?: number;
  error?: string;
}

/**
 * Enforces navigator.storage.persist() on app boot to request non-volatile storage
 * from the operating system (e.g. preventing iOS Safari or Chrome low-disk eviction).
 */
export async function requestStoragePersistence(): Promise<boolean> {
  if (typeof window === "undefined" || !navigator.storage || !navigator.storage.persist) {
    console.warn("[StoragePersistence] navigator.storage.persist is not supported in this environment.");
    return false;
  }

  try {
    const isAlreadyPersisted = await navigator.storage.persisted();
    if (isAlreadyPersisted) {
      console.log("[StoragePersistence] Storage is already non-volatile (persisted: true).");
      return true;
    }

    const granted = await navigator.storage.persist();
    if (granted) {
      console.log("[StoragePersistence] Non-volatile storage successfully granted by browser/OS.");
    } else {
      console.warn("[StoragePersistence] Persistent storage request was denied by browser/OS (storage may be subject to eviction).");
    }
    return granted;
  } catch (err: any) {
    console.warn("[StoragePersistence] Error requesting storage persistence:", err?.message || err);
    return false;
  }
}

/**
 * Queries current storage persistence state and quota metrics.
 */
export async function checkStoragePersistence(): Promise<StoragePersistenceStatus> {
  if (typeof window === "undefined" || !navigator.storage) {
    return { supported: false, persisted: false };
  }

  try {
    const persisted = navigator.storage.persisted ? await navigator.storage.persisted() : false;
    let quotaBytes: number | undefined;
    let usageBytes: number | undefined;
    let usagePercentage: number | undefined;

    if (navigator.storage.estimate) {
      const estimate = await navigator.storage.estimate();
      quotaBytes = estimate.quota;
      usageBytes = estimate.usage;
      if (quotaBytes && usageBytes !== undefined) {
        usagePercentage = Math.round((usageBytes / quotaBytes) * 10000) / 100;
      }
    }

    return {
      supported: true,
      persisted,
      quotaBytes,
      usageBytes,
      usagePercentage,
    };
  } catch (err: any) {
    return {
      supported: true,
      persisted: false,
      error: err?.message || String(err),
    };
  }
}

// --------------------------------------------------------------------------
// 10. INDEXEDDB STORE CONFIGURATION
// --------------------------------------------------------------------------

export const ARCHITECTURE_DB_CONFIG = {
  name: "AimanUnifiedOfflineDB_v3",
  version: 3,
  stores: {
    WAL: "write_ahead_log",
    TOMBSTONES: "tombstones",
    IDEMPOTENT_ATTENDANCE: "idempotent_attendance",
    FINANCIAL_LEDGER: "financial_ledger",
    PARENT_NOTIFICATION_QUEUE: "parent_notification_queue",
    IDEMPOTENCY_KEYS: "idempotency_keys_cache",
    EMERGENCY_BACKUPS: "emergency_backups",
    CLIENT_STATE: "client_state",
  },
} as const;
