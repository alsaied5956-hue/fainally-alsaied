/**
 * src/architecture/index.ts
 * 
 * Central Export Hub for Production-Ready, Zero-Data-Loss, Offline-First Architecture
 * 
 * Includes:
 *  - dbSchema: WAL, Tombstones, Idempotency, Immutable Ledger, Storage Persistence
 *  - syncEngine: Master Sync, HLC, Clock Drift Guard (±5m), 90-Day Zombie Guard, Time-Based TTL Echo Suppression
 *  - parentSyncNotifier: Throttled Offline Flusher (500ms), 2-Hour TTL Queue, Battery & Data Saver
 *  - scannerHandler: Sub-20ms High-Throughput Card Scanner Pipeline
 *  - financialLedger: Immutable Append-Only Ledger, Reversals, Deterministic Balance
 *  - emergencyBackup: WAL JSON Exporter/Importer, Storage Eviction Safeguards
 */

export * from "./dbSchema";
export * from "./parentSyncNotifier";
export * from "./syncEngine";
export * from "./scannerHandler";
export * from "./financialLedger";
export * from "./emergencyBackup";
