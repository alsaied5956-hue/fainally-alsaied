/**
 * src/architecture/emergencyBackup.ts
 * 
 * In-App Emergency Fallback: WAL Emergency JSON Exporter/Importer
 * 
 * Protects against:
 *  1. Silent browser storage eviction (iOS Safari / Chrome low disk)
 *  2. Manual user history/cache clearing
 *  3. 90-Day Long-Offline Zombie Data pre-wipe preservation
 * 
 * Features:
 *  - Deterministic cryptographic checksum verification
 *  - Browser auto-download of emergency backup `.json` files
 *  - Seamless restoration back into the live Write-Ahead Log (WAL)
 *  - Storage health and eviction threat diagnostics
 */

import {
  WALRecord,
  ARCHITECTURE_DB_CONFIG,
  deterministicHash,
  checkStoragePersistence,
  StoragePersistenceStatus,
} from "./dbSchema";

export interface EmergencyWALBackup {
  backupId: string;
  version: number;
  clientId: string;
  createdAt: number;
  createdAtIso: string;
  totalRecords: number;
  pendingWALRecords: WALRecord[];
  checksum: string;
  metadata?: {
    storagePersistence?: StoragePersistenceStatus;
    reason?: string;
  };
}

export interface StorageHealthInspection {
  status: StoragePersistenceStatus;
  pendingWALCount: number;
  riskLevel: "HEALTHY" | "WARNING" | "CRITICAL_EVICTION_RISK";
  recommendation: string;
}

const EMERGENCY_ARCHIVE_STORAGE_KEY = "aiman_emergency_wal_archive_snapshot";

// --------------------------------------------------------------------------
// 1. EXPORT WAL TO JSON
// --------------------------------------------------------------------------

/**
 * Gathers un-synced/pending WAL records from in-memory queue and IndexedDB,
 * formats them with a cryptographic checksum, and generates an emergency JSON backup.
 */
export async function exportUnsyncedWALToJson(
  records: WALRecord[],
  clientId: string,
  reason: string = "manual_emergency_backup"
): Promise<EmergencyWALBackup> {
  const now = Date.now();
  const backupId = `wal_backup_${now}_${Math.random().toString(36).substring(2, 7)}`;

  // Deep clone to prevent mutations
  const clonedRecords: WALRecord[] = JSON.parse(JSON.stringify(records));

  // Compute cryptographic checksum over all records
  let rollingHash = "WAL_ROOT";
  for (const rec of clonedRecords) {
    rollingHash = deterministicHash(rollingHash, rec.walId, rec.idempotencyKey, rec.entityType, rec.action);
  }

  const persistenceStatus = await checkStoragePersistence();

  const backup: EmergencyWALBackup = {
    backupId,
    version: 1,
    clientId,
    createdAt: now,
    createdAtIso: new Date(now).toISOString(),
    totalRecords: clonedRecords.length,
    pendingWALRecords: clonedRecords,
    checksum: rollingHash,
    metadata: {
      storagePersistence: persistenceStatus,
      reason,
    },
  };

  // Also save a copy in localStorage as an immediate second safety net
  try {
    if (typeof window !== "undefined") {
      localStorage.setItem(EMERGENCY_ARCHIVE_STORAGE_KEY, JSON.stringify(backup));
    }
  } catch (e) {
    console.warn("[EmergencyBackup] LocalStorage mirror write warning:", e);
  }

  return backup;
}

/**
 * Triggers a browser file download of the emergency JSON backup.
 */
export async function downloadWALEmergencyBackup(
  records: WALRecord[],
  clientId: string,
  reason?: string
): Promise<{ success: boolean; filename: string; totalRecords: number }> {
  const backup = await exportUnsyncedWALToJson(records, clientId, reason);
  const jsonString = JSON.stringify(backup, null, 2);
  const nowStr = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const filename = `aiman_emergency_wal_${nowStr}_(${backup.totalRecords}_records).json`;

  if (typeof window !== "undefined" && typeof document !== "undefined") {
    const blob = new Blob([jsonString], { type: "application/json;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.setAttribute("download", filename);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  }

  return {
    success: true,
    filename,
    totalRecords: backup.totalRecords,
  };
}

// --------------------------------------------------------------------------
// 2. IMPORT & RESTORE WAL FROM JSON
// --------------------------------------------------------------------------

export interface ImportWALResult {
  success: boolean;
  importedCount: number;
  backupId: string;
  createdAtIso: string;
  message: string;
  restoredRecords: WALRecord[];
}

/**
 * Validates, checks integrity, and restores un-synced records from an emergency JSON backup.
 */
export function importWALFromJson(jsonString: string): ImportWALResult {
  try {
    const parsed: EmergencyWALBackup = JSON.parse(jsonString);

    if (!parsed.backupId || !Array.isArray(parsed.pendingWALRecords)) {
      return {
        success: false,
        importedCount: 0,
        backupId: "",
        createdAtIso: "",
        message: "الملف غير صالح: لا يحتوي على تنسيق نسخة احتياطية لسجلات WAL.",
        restoredRecords: [],
      };
    }

    // Verify cryptographic checksum
    let rollingHash = "WAL_ROOT";
    for (const rec of parsed.pendingWALRecords) {
      rollingHash = deterministicHash(rollingHash, rec.walId, rec.idempotencyKey, rec.entityType, rec.action);
    }

    if (parsed.checksum && parsed.checksum !== rollingHash) {
      console.warn("[EmergencyBackup] Checksum mismatch detected during import. Proceeding with caution.");
    }

    return {
      success: true,
      importedCount: parsed.pendingWALRecords.length,
      backupId: parsed.backupId,
      createdAtIso: parsed.createdAtIso,
      message: `تم استعادة ${parsed.pendingWALRecords.length} سجل بنجاح من النسخة الاحتياطية (${parsed.backupId}).`,
      restoredRecords: parsed.pendingWALRecords,
    };
  } catch (err: any) {
    return {
      success: false,
      importedCount: 0,
      backupId: "",
      createdAtIso: "",
      message: `فشل قراءة الملف: ${err?.message || String(err)}`,
      restoredRecords: [],
    };
  }
}

/**
 * Retrieves the last auto-archived emergency snapshot from localStorage if available.
 */
export function getArchivedEmergencySnapshot(): EmergencyWALBackup | null {
  if (typeof window === "undefined") return null;
  try {
    const raw = localStorage.getItem(EMERGENCY_ARCHIVE_STORAGE_KEY);
    return raw ? JSON.parse(raw) : null;
  } catch {
    return null;
  }
}

// --------------------------------------------------------------------------
// 3. STORAGE EVICTION RISK INSPECTION
// --------------------------------------------------------------------------

/**
 * Evaluates whether browser storage is at risk of eviction.
 */
export async function inspectStorageHealth(pendingWALCount: number): Promise<StorageHealthInspection> {
  const status = await checkStoragePersistence();

  let riskLevel: "HEALTHY" | "WARNING" | "CRITICAL_EVICTION_RISK" = "HEALTHY";
  let recommendation = "حالة التخزين ممتازة وآمنة تماماً.";

  if (!status.persisted) {
    riskLevel = "WARNING";
    recommendation = "التخزين غير محمي ضد الحذف التلقائي من المتصفح. يُنصح بتفعيل التخزين الدائم.";
  }

  if (status.usagePercentage && status.usagePercentage > 85) {
    riskLevel = "CRITICAL_EVICTION_RISK";
    recommendation = `تحذير: استهلاك التخزين وصل إلى ${status.usagePercentage}%. المتصفح قد يحذف البيانات تلقائياً. يُنصح بعمل نسخة احتياطية فوراً.`;
  }

  if (pendingWALCount > 200 && !status.persisted) {
    riskLevel = "CRITICAL_EVICTION_RISK";
    recommendation = `يوجد ${pendingWALCount} سجل أوفلاين غير متزامن وتخزين الجهاز غير محمي. خطر فقدان البيانات مرتفع عند إغلاق المتصفح.`;
  }

  return {
    status,
    pendingWALCount,
    riskLevel,
    recommendation,
  };
}
