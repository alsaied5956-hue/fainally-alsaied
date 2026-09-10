/**
 * src/architecture/dbSchema.ts
 * 
 * Production-Ready, Zero-Data-Loss, Offline-First Architecture Schemas
 * 
 * Supports:
 *  1. Write-Ahead Log (WAL) with durability guarantees
 *  2. Durable Tombstones to prevent "Zombie Data Resurrection"
 *  3. Idempotent Attendance with deterministic deduplication keys
 *  4. Immutable Append-Only Financial Ledger (Strictly NO Last-Write-Wins)
 *  5. Real-Time Parent Read-Only Views & Instant Notification Pipelines
 *  6. Hybrid Logical Clocks (HLC) for monotonic cross-device ordering
 */

// --------------------------------------------------------------------------
// 1. HYBRID LOGICAL CLOCK (HLC) DEFINITION
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
// 2. DETERMINISTIC HASH & IDEMPOTENCY KEY GENERATORS
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
// 3. WRITE-AHEAD LOG (WAL) SCHEMA
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
// 4. TOMBSTONE RECORD SCHEMA (Prevents Zombie Data Resurrection)
// --------------------------------------------------------------------------

export interface TombstoneRecord {
  tombstoneId: string;
  entityType: WALEntityType;
  entityId: string;
  deletedAtHlc: HybridLogicalClock;
  deletedBy: string;
  reason?: string;
  createdAt: number;
  ttlMs: number; // 90 days = 90 * 86400 * 1000
  purged: boolean;
}

// --------------------------------------------------------------------------
// 5. IDEMPOTENT ATTENDANCE SCHEMA
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
// 6. IMMUTABLE FINANCIAL LEDGER SCHEMA (Strictly Append-Only)
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
// 7. PARENT READ-ONLY VIEWS & REAL-TIME NOTIFICATION SCHEMA
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
  deliveryStatus: "QUEUED" | "SENT_REALTIME" | "OFFLINE_QUEUED" | "DELIVERED";
  sentAt?: number;
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
// 8. INDEXEDDB STORE CONFIGURATION
// --------------------------------------------------------------------------

export const ARCHITECTURE_DB_CONFIG = {
  name: "AimanUnifiedOfflineDB_v2",
  version: 2,
  stores: {
    WAL: "write_ahead_log",
    TOMBSTONES: "tombstones",
    IDEMPOTENT_ATTENDANCE: "idempotent_attendance",
    FINANCIAL_LEDGER: "financial_ledger",
    PARENT_NOTIFICATION_QUEUE: "parent_notification_queue",
    IDEMPOTENCY_KEYS: "idempotency_keys_cache",
    CLIENT_STATE: "client_state",
  },
} as const;
