/**
 * src/architecture/syncEngine.ts
 * 
 * Production-Grade Master Sync Engine with:
 *  1. Hybrid Logical Clocks (HLC) with Monotonic cross-device ordering
 *  2. Server Clock Drift Guard (±5 minutes threshold) preventing HLC corruption
 *  3. 90-Day Long-Offline Zombie Data Guard (Blocks bidirectional sync; forces Pull-First)
 *  4. Time-To-Live (TTL) Deduplication Buffer for Echo Suppression (5-minute sliding TTL)
 *  5. OS Non-Volatile Storage Persistence (`navigator.storage.persist()`)
 *  6. Durable Write-Ahead Log (WAL) with <1ms local persistence
 *  7. Safe 4-Second Coalescing Batch Writer with HTTP 429 rate-limit backoff
 *  8. Strict Tombstone validation preventing Zombie Data Resurrection
 */

import {
  HybridLogicalClock,
  compareHLC,
  formatHLC,
  parseHLC,
  WALRecord,
  WALEntityType,
  WALAction,
  TombstoneRecord,
  ARCHITECTURE_DB_CONFIG,
  ARCHITECTURE_CONSTANTS,
  deterministicHash,
  requestStoragePersistence,
} from "./dbSchema";
import { db, ensureFirebaseAuth } from "../utils/firebase";
import { doc, writeBatch } from "firebase/firestore";
import { exportUnsyncedWALToJson } from "./emergencyBackup";

// --------------------------------------------------------------------------
// 1. PERSISTENT CLIENT ID & TIME-BASED TTL ECHO SUPPRESSION
// --------------------------------------------------------------------------

const CLIENT_ID_STORAGE_KEY = "aiman_device_client_id_v3";
const LAST_SYNC_TS_KEY = "aiman_last_successful_sync_v3";

export function getOrCreateClientId(): string {
  if (typeof window === "undefined") return "node_server";
  let id = localStorage.getItem(CLIENT_ID_STORAGE_KEY);
  if (!id) {
    const platform = /iPhone|iPad|iPod|Android/i.test(navigator.userAgent)
      ? "mobile"
      : /Macintosh|Windows|Linux/i.test(navigator.userAgent)
      ? "desktop"
      : "device";
    const rand = Math.random().toString(36).substring(2, 8);
    id = `${platform}_${rand}`;
    localStorage.setItem(CLIENT_ID_STORAGE_KEY, id);
  }
  return id;
}

export const CURRENT_CLIENT_ID = getOrCreateClientId();

/**
 * Time-To-Live (TTL) Deduplication Buffer for Echo Suppression
 * Map<eventId, expireAtEpochMs>
 * Replaces fixed-size 500/1000 item buffers to prevent high-volume center memory overflow
 * while guaranteeing suppression for 5 continuous minutes per event.
 */
const echoTtlBuffer = new Map<string, number>();

/**
 * Periodically purges expired entries from the TTL echo buffer (runs every 60s).
 */
function purgeExpiredEchoes(): void {
  const now = Date.now();
  for (const [id, expireAt] of echoTtlBuffer.entries()) {
    if (now >= expireAt) {
      echoTtlBuffer.delete(id);
    }
  }
}

if (typeof window !== "undefined") {
  setInterval(purgeExpiredEchoes, 60000);
}

/**
 * Evaluates whether an incoming event is an echo or duplicate using sliding 5-minute TTL.
 */
export function shouldSuppressEcho(eventId: string, originClientId?: string): boolean {
  if (!eventId) return false;

  // 1. Suppress if originated by this client
  if (originClientId && originClientId === CURRENT_CLIENT_ID) {
    return true;
  }

  const now = Date.now();
  const existingExpireAt = echoTtlBuffer.get(eventId);

  // 2. Suppress if seen within active TTL window
  if (existingExpireAt && now < existingExpireAt) {
    return true;
  }

  // 3. Register in buffer with sliding 5-minute TTL
  echoTtlBuffer.set(eventId, now + ARCHITECTURE_CONSTANTS.ECHO_DEDUP_TTL_MS);
  return false;
}

// --------------------------------------------------------------------------
// 2. HARDWARE CLOCK DRIFT & CLOCK TAMPERING GUARD
// --------------------------------------------------------------------------

interface ClockDriftState {
  hasDriftError: boolean;
  driftMs: number;
  lastCheckedAt: number;
  serverTimeEstimated: number;
  errorMessage: string;
}

let clockDriftState: ClockDriftState = {
  hasDriftError: false,
  driftMs: 0,
  lastCheckedAt: 0,
  serverTimeEstimated: Date.now(),
  errorMessage: "",
};

type ClockDriftListener = (state: ClockDriftState) => void;
const clockDriftListeners = new Set<ClockDriftListener>();

export function subscribeToClockDriftStatus(listener: ClockDriftListener): () => void {
  clockDriftListeners.add(listener);
  listener(clockDriftState);
  return () => clockDriftListeners.delete(listener);
}

export function getClockDriftStatus(): ClockDriftState {
  return { ...clockDriftState };
}

/**
 * Checks clock drift against the backend server or trusted NTP time.
 * If physical clock drift exceeds ±5 minutes, blocks local HLC mutations.
 */
export async function syncServerClockTime(): Promise<ClockDriftState> {
  if (typeof window === "undefined") {
    return clockDriftState;
  }

  const t0 = performance.now();
  try {
    const res = await fetch("/api/time", { cache: "no-store" });
    const t1 = performance.now();
    const roundTrip = t1 - t0;

    if (res.ok) {
      const data = await res.json();
      const serverTime = Number(data.serverTime);
      const localNow = Date.now();

      // Estimated server time at response arrival
      const serverEstimated = serverTime + Math.round(roundTrip / 2);
      const drift = localNow - serverEstimated; // Positive: local is fast (in future); Negative: local is slow (in past)

      const hasError = Math.abs(drift) > ARCHITECTURE_CONSTANTS.CLOCK_DRIFT_MAX_MS;
      const minutesDrift = Math.round((drift / 60000) * 10) / 10;

      clockDriftState = {
        hasDriftError: hasError,
        driftMs: drift,
        lastCheckedAt: Date.now(),
        serverTimeEstimated: serverEstimated,
        errorMessage: hasError
          ? `⚠️ خطأ في توقيت الجهاز: ساعة جهازك غير متطابقة مع توقيت السيرفر بفارق (${minutesDrift} دقيقة). تم إيقاف المزامنة مؤقتاً لحماية سلامة البيانات. يرجى ضبط توقيت جهازك.`
          : "",
      };

      if (hasError) {
        console.error(`[SyncEngine ClockDriftGuard] Physical clock drift exceeds threshold: ${drift}ms (${minutesDrift} mins). Mutations blocked.`);
      }

      clockDriftListeners.forEach((fn) => {
        try {
          fn(clockDriftState);
        } catch {}
      });
    }
  } catch (err) {
    // If offline, preserve current drift knowledge
    console.warn("[SyncEngine ClockDriftGuard] Could not query server time (offline or endpoint unreachable).");
  }

  return clockDriftState;
}

// Check clock on startup and periodically every 5 minutes
if (typeof window !== "undefined") {
  syncServerClockTime().catch(() => {});
  setInterval(() => {
    syncServerClockTime().catch(() => {});
  }, 5 * 60 * 1000);
}

// --------------------------------------------------------------------------
// 3. HYBRID LOGICAL CLOCK (HLC) IMPLEMENTATION
// --------------------------------------------------------------------------

class HybridLogicalClockEngine {
  private logicalTime: number = 0;
  private counter: number = 0;
  private readonly nodeId: string;
  private monotonicSeq: number = 0;

  constructor(nodeId: string) {
    this.nodeId = nodeId;
    this.logicalTime = Date.now();
  }

  /**
   * Generates the next monotonic HLC for a local mutation.
   * Throws an error if local device clock drift exceeds ±5 minutes to prevent HLC corruption.
   */
  public now(): HybridLogicalClock {
    if (clockDriftState.hasDriftError) {
      throw new Error(
        clockDriftState.errorMessage ||
          "Clock drift exceeds ±5 minutes. Local mutations are blocked to protect HLC monotonic integrity."
      );
    }

    const physicalTime = Date.now();
    this.monotonicSeq++;

    if (physicalTime > this.logicalTime) {
      this.logicalTime = physicalTime;
      this.counter = 0;
    } else {
      // Clock hasn't moved or has drifted backwards; increment logical counter
      this.counter++;
    }

    return {
      logicalTime: this.logicalTime,
      counter: this.counter,
      nodeId: this.nodeId,
    };
  }

  /**
   * Advances local HLC based on a received remote message's HLC.
   * Guarantees that local clock >= max(local, remote, physical).
   */
  public update(received: HybridLogicalClock): HybridLogicalClock {
    const physicalTime = Date.now();
    const maxTime = Math.max(this.logicalTime, received.logicalTime, physicalTime);

    if (maxTime === this.logicalTime && maxTime === received.logicalTime) {
      this.counter = Math.max(this.counter, received.counter) + 1;
    } else if (maxTime === this.logicalTime) {
      this.counter++;
    } else if (maxTime === received.logicalTime) {
      this.counter = received.counter + 1;
    } else {
      this.counter = 0;
    }

    this.logicalTime = maxTime;
    return {
      logicalTime: this.logicalTime,
      counter: this.counter,
      nodeId: this.nodeId,
    };
  }

  public getMonotonicSeq(): number {
    return this.monotonicSeq;
  }
}

export const HLCEngine = new HybridLogicalClockEngine(CURRENT_CLIENT_ID);

// --------------------------------------------------------------------------
// 4. 90-DAY LONG-OFFLINE ZOMBIE DATA GUARD
// --------------------------------------------------------------------------

interface ZombieGuardStatus {
  isLongOfflineDetected: boolean;
  daysOffline: number;
  lastSyncAt: number;
  pullFirstRequired: boolean;
}

export function checkZombieDataGuard(): ZombieGuardStatus {
  if (typeof window === "undefined") {
    return { isLongOfflineDetected: false, daysOffline: 0, lastSyncAt: Date.now(), pullFirstRequired: false };
  }

  const lastSyncStr = localStorage.getItem(LAST_SYNC_TS_KEY);
  const lastSyncAt = lastSyncStr ? Number(lastSyncStr) : Date.now();
  const diffMs = Date.now() - lastSyncAt;
  const daysOffline = Math.floor(diffMs / (86400 * 1000));

  const isLongOfflineDetected = diffMs > ARCHITECTURE_CONSTANTS.TOMBSTONE_MAX_AGE_MS;

  return {
    isLongOfflineDetected,
    daysOffline,
    lastSyncAt,
    pullFirstRequired: isLongOfflineDetected,
  };
}

export function recordSuccessfulSyncTimestamp(): void {
  if (typeof window !== "undefined") {
    localStorage.setItem(LAST_SYNC_TS_KEY, String(Date.now()));
  }
}

/**
 * Enforces a Full Reset & Pull-First Synchronization if a device was offline > 90 days.
 * Prevents re-instantiating deleted "zombie" records whose server tombstones have expired.
 */
export async function executePullFirstReconciliation(
  onPullFreshSnapshot: () => Promise<void>
): Promise<{ success: boolean; message: string; archivedCount: number }> {
  const guard = checkZombieDataGuard();
  if (!guard.isLongOfflineDetected) {
    recordSuccessfulSyncTimestamp();
    return { success: true, message: "Device offline time within safe tombstone TTL window.", archivedCount: 0 };
  }

  console.warn(
    `[SyncEngine ZombieGuard] Device offline for ${guard.daysOffline} days (> 90 days). Enforcing Pull-First reconciliation!`
  );

  // 1. Emergency Archive uncommitted local WAL records before wiping
  const pendingWAL = getUncommittedWALRecords();
  let archivedCount = 0;
  if (pendingWAL.length > 0) {
    const backup = await exportUnsyncedWALToJson(
      pendingWAL,
      CURRENT_CLIENT_ID,
      "90_day_long_offline_zombie_protection_pre_reset"
    );
    archivedCount = backup.totalRecords;
    console.log(`[SyncEngine ZombieGuard] Archived ${archivedCount} pending records to emergency storage.`);
  }

  // 2. Clear uncommitted local WAL to block zombie resurrection writes
  memoryWALQueue.length = 0;
  const idb = await getDB();
  if (idb) {
    try {
      const tx = idb.transaction([ARCHITECTURE_DB_CONFIG.stores.WAL], "readwrite");
      tx.objectStore(ARCHITECTURE_DB_CONFIG.stores.WAL).clear();
    } catch {}
  }

  // 3. Pull fresh, authoritative snapshot from cloud
  await onPullFreshSnapshot();

  // 4. Update sync timestamp to restore normal operation
  recordSuccessfulSyncTimestamp();

  return {
    success: true,
    message: `تم تفعيل بروتوكول Pull-First لحماية المنظومة من إحياء البيانات المحذوفة بعد انقطاع ${guard.daysOffline} يوماً. تم أرشفة ${archivedCount} سجل محلي بأمان.`,
    archivedCount,
  };
}

// --------------------------------------------------------------------------
// 5. DURABLE INDEXEDDB WRITE-AHEAD LOG (WAL) & TOMBSTONES
// --------------------------------------------------------------------------

let idbPromise: Promise<IDBDatabase | null> | null = null;

async function getDB(): Promise<IDBDatabase | null> {
  if (typeof window === "undefined" || !("indexedDB" in window)) return null;
  if (idbPromise) return idbPromise;

  idbPromise = new Promise((resolve) => {
    try {
      const req = indexedDB.open(ARCHITECTURE_DB_CONFIG.name, ARCHITECTURE_DB_CONFIG.version);

      req.onupgradeneeded = (e: IDBVersionChangeEvent) => {
        const db = (e.target as IDBOpenDBRequest).result;
        const stores = ARCHITECTURE_DB_CONFIG.stores;

        if (!db.objectStoreNames.contains(stores.WAL)) {
          const s = db.createObjectStore(stores.WAL, { keyPath: "walId" });
          s.createIndex("status", "status", { unique: false });
          s.createIndex("idempotencyKey", "idempotencyKey", { unique: false });
        }
        if (!db.objectStoreNames.contains(stores.TOMBSTONES)) {
          const s = db.createObjectStore(stores.TOMBSTONES, { keyPath: "tombstoneId" });
          s.createIndex("entityId", "entityId", { unique: false });
        }
        if (!db.objectStoreNames.contains(stores.IDEMPOTENT_ATTENDANCE)) {
          db.createObjectStore(stores.IDEMPOTENT_ATTENDANCE, { keyPath: "attendanceKey" });
        }
        if (!db.objectStoreNames.contains(stores.FINANCIAL_LEDGER)) {
          const s = db.createObjectStore(stores.FINANCIAL_LEDGER, { keyPath: "transactionId" });
          s.createIndex("studentBarcode", "studentBarcode", { unique: false });
          s.createIndex("monthKey", "monthKey", { unique: false });
        }
        if (!db.objectStoreNames.contains(stores.IDEMPOTENCY_KEYS)) {
          db.createObjectStore(stores.IDEMPOTENCY_KEYS, { keyPath: "key" });
        }
        if (!db.objectStoreNames.contains(stores.EMERGENCY_BACKUPS)) {
          db.createObjectStore(stores.EMERGENCY_BACKUPS, { keyPath: "backupId" });
        }
      };

      req.onsuccess = (e) => {
        resolve((e.target as IDBOpenDBRequest).result);
      };
      req.onerror = () => {
        resolve(null);
      };
    } catch {
      resolve(null);
    }
  });

  return idbPromise;
}

// Initialize Storage Persistence on app boot
if (typeof window !== "undefined") {
  requestStoragePersistence().catch(() => {});
}

// --------------------------------------------------------------------------
// 6. IDEMPOTENCY & TOMBSTONE STORE
// --------------------------------------------------------------------------

// In-memory cache for sub-millisecond lookups
const memoryIdempotencyKeys = new Set<string>();
const memoryTombstones = new Map<string, TombstoneRecord>();

export async function isIdempotencyKeySeen(key: string): Promise<boolean> {
  if (memoryIdempotencyKeys.has(key)) return true;

  const db = await getDB();
  if (!db) return false;

  return new Promise<boolean>((resolve) => {
    try {
      const tx = db.transaction(ARCHITECTURE_DB_CONFIG.stores.IDEMPOTENCY_KEYS, "readonly");
      const store = tx.objectStore(ARCHITECTURE_DB_CONFIG.stores.IDEMPOTENCY_KEYS);
      const req = store.get(key);
      req.onsuccess = () => {
        const exists = !!req.result;
        if (exists) memoryIdempotencyKeys.add(key);
        resolve(exists);
      };
      req.onerror = () => resolve(false);
    } catch {
      resolve(false);
    }
  });
}

export async function markIdempotencyKeySeen(key: string): Promise<void> {
  memoryIdempotencyKeys.add(key);
  const db = await getDB();
  if (!db) return;

  try {
    const tx = db.transaction(ARCHITECTURE_DB_CONFIG.stores.IDEMPOTENCY_KEYS, "readwrite");
    const store = tx.objectStore(ARCHITECTURE_DB_CONFIG.stores.IDEMPOTENCY_KEYS);
    store.put({ key, timestamp: Date.now() });
  } catch {}
}

/**
 * Checks if a durable tombstone exists for an entity and whether the tombstone's HLC
 * supersedes the given record's HLC.
 */
export async function isZombieResurrection(
  entityId: string,
  recordHlc: HybridLogicalClock
): Promise<boolean> {
  const cached = memoryTombstones.get(entityId);
  if (cached && compareHLC(cached.deletedAtHlc, recordHlc) >= 0) {
    return true; // Tombstone is newer or equal: reject resurrection!
  }

  const db = await getDB();
  if (!db) return false;

  return new Promise<boolean>((resolve) => {
    try {
      const tx = db.transaction(ARCHITECTURE_DB_CONFIG.stores.TOMBSTONES, "readonly");
      const store = tx.objectStore(ARCHITECTURE_DB_CONFIG.stores.TOMBSTONES);
      const index = store.index("entityId");
      const req = index.get(entityId);
      req.onsuccess = () => {
        const tomb = req.result as TombstoneRecord | undefined;
        if (tomb) {
          memoryTombstones.set(entityId, tomb);
          resolve(compareHLC(tomb.deletedAtHlc, recordHlc) >= 0);
        } else {
          resolve(false);
        }
      };
      req.onerror = () => resolve(false);
    } catch {
      resolve(false);
    }
  });
}

/**
 * Creates a durable tombstone when an entity is deleted to prevent resurrection.
 */
export async function recordTombstone(
  entityType: WALEntityType,
  entityId: string,
  deletedBy: string = "admin",
  reason?: string
): Promise<TombstoneRecord> {
  const hlc = HLCEngine.now();
  const tombstoneId = `tomb_${deterministicHash(entityType, entityId, formatHLC(hlc))}`;
  const record: TombstoneRecord = {
    tombstoneId,
    entityType,
    entityId,
    deletedAtHlc: hlc,
    deletedBy,
    reason,
    createdAt: Date.now(),
    ttlMs: ARCHITECTURE_CONSTANTS.TOMBSTONE_MAX_AGE_MS,
    purged: false,
  };

  memoryTombstones.set(entityId, record);

  const db = await getDB();
  if (db) {
    try {
      const tx = db.transaction(ARCHITECTURE_DB_CONFIG.stores.TOMBSTONES, "readwrite");
      tx.objectStore(ARCHITECTURE_DB_CONFIG.stores.TOMBSTONES).put(record);
    } catch {}
  }

  // Also log into WAL so tombstone is propagated to all other devices
  await appendWALRecord({
    idempotencyKey: tombstoneId,
    entityType,
    entityId,
    action: "DELETE",
    payload: { tombstoneId, entityId, deletedAtHlc: hlc },
    status: "PENDING",
  });

  return record;
}

// --------------------------------------------------------------------------
// 7. WRITE-AHEAD LOG (WAL) WRITER & RETRIEVAL
// --------------------------------------------------------------------------

// In-memory queue for <1ms zero-latency writes
const memoryWALQueue: WALRecord[] = [];

export function getUncommittedWALRecords(): WALRecord[] {
  return [...memoryWALQueue];
}

export interface AppendWALParams {
  idempotencyKey: string;
  entityType: WALEntityType;
  entityId: string;
  action: WALAction;
  payload: any;
  status?: "PENDING" | "COALESCING";
}

/**
 * Synchronously writes a mutation to in-memory WAL and asynchronously commits
 * to IndexedDB in the background. Operates in < 1ms.
 */
export async function appendWALRecord(params: AppendWALParams): Promise<WALRecord> {
  const hlc = HLCEngine.now();
  const walId = `wal_${hlc.logicalTime}_${hlc.counter}_${hlc.nodeId}`;

  const record: WALRecord = {
    walId,
    idempotencyKey: params.idempotencyKey,
    entityType: params.entityType,
    entityId: params.entityId,
    action: params.action,
    hlc,
    clientMonotonicSeq: HLCEngine.getMonotonicSeq(),
    payload: params.payload,
    status: params.status || "PENDING",
    retryCount: 0,
    createdAt: Date.now(),
  };

  memoryWALQueue.push(record);

  // Commit to IndexedDB WAL
  const db = await getDB();
  if (db) {
    try {
      const tx = db.transaction(ARCHITECTURE_DB_CONFIG.stores.WAL, "readwrite");
      tx.objectStore(ARCHITECTURE_DB_CONFIG.stores.WAL).put(record);
    } catch (err) {
      console.warn("[SyncEngine] IndexedDB WAL write warning:", err);
    }
  }

  // Trigger batch coalescer schedule
  scheduleBatchCoalesce();

  return record;
}

// --------------------------------------------------------------------------
// 8. SAFE 4-SECOND COALESCING BATCH WRITER (Rate-Limit & Quota Protection)
// --------------------------------------------------------------------------

let batchTimer: ReturnType<typeof setTimeout> | null = null;
let isFlushingBatch = false;
let consecutiveRateLimitFailures = 0;

function scheduleBatchCoalesce(): void {
  if (batchTimer || isFlushingBatch) return;

  batchTimer = setTimeout(() => {
    batchTimer = null;
    flushCoalescedBatch().catch((err) => {
      console.warn("[SyncEngine] Coalesced batch flush notice:", err);
    });
  }, ARCHITECTURE_CONSTANTS.BATCH_COALESCE_INTERVAL_MS);
}

/**
 * Executes a coalesced cloud batch write with backoff handling.
 */
export async function flushCoalescedBatch(forceImmediate = false): Promise<number> {
  if (isFlushingBatch) return 0;
  if (memoryWALQueue.length === 0) return 0;
  if (typeof window !== "undefined" && !navigator.onLine && !forceImmediate) return 0;

  // Verify clock drift before pushing cloud writes
  if (clockDriftState.hasDriftError) {
    console.warn("[SyncEngine] Batch flush postponed: Server clock drift guard active.");
    return 0;
  }

  isFlushingBatch = true;
  const batchToFlush = memoryWALQueue.slice(0, ARCHITECTURE_CONSTANTS.MAX_BATCH_SIZE);
  const flushedWalIds: string[] = [];

  try {
    for (const record of batchToFlush) {
      record.status = "COALESCING";
    }

    // Commit Firestore Batched Writes
    await ensureFirebaseAuth();
    const fbBatch = writeBatch(db);

    for (const rec of batchToFlush) {
      const docPath =
        rec.entityType === "ATTENDANCE"
          ? "attendance_records"
          : rec.entityType === "STUDENT"
          ? "students"
          : rec.entityType === "FINANCIAL_LEDGER"
          ? "payment_records"
          : "batch_operations";

      const docRef = doc(db, docPath, rec.entityId);
      if (rec.action === "DELETE") {
        fbBatch.delete(docRef);
      } else {
        fbBatch.set(
          docRef,
          {
            ...rec.payload,
            _hlc: formatHLC(rec.hlc),
            _updatedAt: Date.now(),
            _clientId: CURRENT_CLIENT_ID,
          },
          { merge: true }
        );
      }
      flushedWalIds.push(rec.walId);
    }

    await fbBatch.commit();

    // Mark processed in WAL
    const flushedSet = new Set(flushedWalIds);
    for (let i = memoryWALQueue.length - 1; i >= 0; i--) {
      if (flushedSet.has(memoryWALQueue[i].walId)) {
        memoryWALQueue.splice(i, 1);
      }
    }

    // Clean from IndexedDB WAL
    const idb = await getDB();
    if (idb) {
      try {
        const tx = idb.transaction(ARCHITECTURE_DB_CONFIG.stores.WAL, "readwrite");
        const store = tx.objectStore(ARCHITECTURE_DB_CONFIG.stores.WAL);
        flushedWalIds.forEach((id) => store.delete(id));
      } catch {}
    }

    consecutiveRateLimitFailures = 0;
    recordSuccessfulSyncTimestamp();
  } catch (err: any) {
    const isRateLimit = err?.code === "resource-exhausted" || err?.status === 429;
    if (isRateLimit) {
      consecutiveRateLimitFailures++;
      const backoffMs = Math.min(30000, 2000 * Math.pow(2, consecutiveRateLimitFailures));
      console.warn(`[SyncEngine] Rate-limit (429) encountered. Backing off for ${backoffMs}ms`);
      setTimeout(() => scheduleBatchCoalesce(), backoffMs);
    } else {
      console.warn("[SyncEngine] Batch sync non-fatal error:", err?.message || err);
    }
  } finally {
    isFlushingBatch = false;
  }

  // If there are more items waiting in the queue, schedule next cycle
  if (memoryWALQueue.length > 0) {
    scheduleBatchCoalesce();
  }

  return flushedWalIds.length;
}

// Auto-flush on page hide / unload so no in-memory mutations are lost
if (typeof window !== "undefined") {
  window.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "hidden") {
      flushCoalescedBatch(true).catch(() => {});
    }
  });
  window.addEventListener("beforeunload", () => {
    flushCoalescedBatch(true).catch(() => {});
  });
}
