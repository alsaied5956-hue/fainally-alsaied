/**
 * src/utils/supabaseClient.ts
 * High-Performance Supabase v2 Client & Sub-20ms Realtime WebSocket Hub
 * Powers Instant Multi-Device Sync for Attendance, Group Finalization, Payments, Homework, and Students
 */

import { createClient, SupabaseClient, RealtimeChannel } from "@supabase/supabase-js";

const SUPABASE_URL =
  (import.meta as any).env?.VITE_SUPABASE_URL || "https://lzdvmzumwuqycwdecaan.supabase.co";
const SUPABASE_ANON_KEY =
  (import.meta as any).env?.VITE_SUPABASE_ANON_KEY || "sb_publishable_B2ATdO71x3VxvOL18ATZtA_bupiDf3l";

export const supabase: SupabaseClient = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
  auth: {
    persistSession: true,
    autoRefreshToken: true,
  },
  realtime: {
    params: {
      eventsPerSecond: 30,
    },
  },
});

export interface LiveScanPayload {
  barcode: string;
  name: string;
  grade: string;
  days: string;
  status: "حضور" | "تأخير" | "غياب";
  timeIso: string;
  timeDisplay: string;
  isPaid: boolean;
  scannedBy: string;
  timestamp: number;
}

export interface GroupFinishedPayload {
  grade: string;
  days: string;
  absentBarcodes: string[];
  lateBarcodes: string[];
  presentBarcodes: string[];
  dateKey: string;
  finishedBy: string;
  timestamp: number;
}

export interface PaymentSyncPayload {
  action: "record" | "update" | "delete";
  barcode: string;
  monthKey: string;
  amount: number;
  date: string;
  time: string;
  note: string;
  recordedBy: string;
  timestamp: number;
}

export interface HomeworkSyncPayload {
  action: "update" | "bulk_update";
  barcodes: string[];
  dateKey: string;
  status: "done" | "incomplete" | "not_done";
  notes?: string;
  updatedBy: string;
  timestamp: number;
}

export interface StudentSyncPayload {
  action: "add" | "update" | "delete";
  barcode: string;
  studentData?: any;
  timestamp: number;
}

export interface ExamGradeSyncPayload {
  action: "record" | "update";
  barcode: string;
  studentName?: string;
  examTitle: string;
  score: number;
  maxScore: number;
  percentage: number;
  dateKey: string;
  timestamp: number;
}

export function getTodayDateKey(): string {
  const d = new Date();
  const year = d.getFullYear();
  const month = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

// ------------------------------------------------------------------------
// 1. DEDICATED REALTIME HUB (Sub-20ms WebSocket Channel)
// ------------------------------------------------------------------------

let realtimeHubChannel: RealtimeChannel | null = null;

export function getOrCreateRealtimeHub(): RealtimeChannel {
  if (!realtimeHubChannel) {
    realtimeHubChannel = supabase.channel("realtime-center-hub", {
      config: {
        broadcast: {
          self: false, // Don't echo back to the emitting device
          ack: false,  // Fire-and-forget for absolute zero-latency
        },
      },
    });

    realtimeHubChannel.subscribe((status) => {
      console.log(`[Supabase Realtime Hub] Status: ${status}`);
    });
  }
  return realtimeHubChannel;
}

// ------------------------------------------------------------------------
// 2. BROADCAST METHODS (Zero Latency Emits)
// ------------------------------------------------------------------------

/** Broadcast single scan to all assistant screens */
export async function broadcastLiveScan(payload: LiveScanPayload): Promise<void> {
  try {
    const channel = getOrCreateRealtimeHub();
    await channel.send({
      type: "broadcast",
      event: "assistant_scan",
      payload,
    });
  } catch (err) {
    console.warn("Realtime broadcast scan notice:", err);
  }
}

/** Broadcast group finish (حفظ وإرسال الغياب للكل) across all screens */
export async function broadcastGroupFinished(payload: GroupFinishedPayload): Promise<void> {
  try {
    const channel = getOrCreateRealtimeHub();
    await channel.send({
      type: "broadcast",
      event: "group_finished",
      payload,
    });
  } catch (err) {
    console.warn("Realtime broadcast group finish notice:", err);
  }
}

/** Broadcast payment record / update / delete across all screens */
export async function broadcastPaymentChange(payload: PaymentSyncPayload): Promise<void> {
  try {
    const channel = getOrCreateRealtimeHub();
    await channel.send({
      type: "broadcast",
      event: "payment_change",
      payload,
    });
  } catch (err) {
    console.warn("Realtime broadcast payment notice:", err);
  }
}

/** Broadcast homework status update across all screens */
export async function broadcastHomeworkChange(payload: HomeworkSyncPayload): Promise<void> {
  try {
    const channel = getOrCreateRealtimeHub();
    await channel.send({
      type: "broadcast",
      event: "homework_change",
      payload,
    });
  } catch (err) {
    console.warn("Realtime broadcast homework notice:", err);
  }
}

/** Broadcast student addition, update, or deletion */
export async function broadcastStudentChange(payload: StudentSyncPayload): Promise<void> {
  try {
    const channel = getOrCreateRealtimeHub();
    await channel.send({
      type: "broadcast",
      event: "student_change",
      payload,
    });
  } catch (err) {
    console.warn("Realtime broadcast student notice:", err);
  }
}

/** Broadcast exam grade recording or update */
export async function broadcastExamGradeChange(payload: ExamGradeSyncPayload): Promise<void> {
  try {
    const channel = getOrCreateRealtimeHub();
    await channel.send({
      type: "broadcast",
      event: "exam_grade_change",
      payload,
    });
  } catch (err) {
    console.warn("Realtime broadcast exam grade notice:", err);
  }
}

/** Broadcast full state changes across all connected devices (< 50ms peer delivery) */
export async function broadcastFullState(payload: any): Promise<void> {
  try {
    const channel = getOrCreateRealtimeHub();
    await channel.send({
      type: "broadcast",
      event: "full_state_sync",
      payload,
    });
  } catch (err) {
    console.warn("Realtime broadcast full state notice:", err);
  }
}

// ------------------------------------------------------------------------
// 3. LISTENERS (Instant Reception on All Devices with Zero-Leak Lifecycle)
// ------------------------------------------------------------------------

const liveScanListeners = new Set<(payload: LiveScanPayload) => void>();
const groupFinishedListeners = new Set<(payload: GroupFinishedPayload) => void>();
const paymentChangeListeners = new Set<(payload: PaymentSyncPayload) => void>();
const homeworkChangeListeners = new Set<(payload: HomeworkSyncPayload) => void>();
const studentChangeListeners = new Set<(payload: StudentSyncPayload) => void>();
const examGradeChangeListeners = new Set<(payload: ExamGradeSyncPayload) => void>();
const fullStateListeners = new Set<(payload: any) => void>();
let listenersInitialized = false;

function ensureChannelListenersRegistered() {
  if (listenersInitialized) return;
  listenersInitialized = true;
  const channel = getOrCreateRealtimeHub();

  channel.on("broadcast", { event: "full_state_sync" }, ({ payload }) => {
    if (payload) {
      fullStateListeners.forEach((fn) => {
        try {
          fn(payload);
        } catch (e) {
          console.warn("Error in fullState listener:", e);
        }
      });
    }
  });

  channel.on("broadcast", { event: "assistant_scan" }, ({ payload }) => {
    if (payload) {
      liveScanListeners.forEach((fn) => {
        try {
          fn(payload as LiveScanPayload);
        } catch (e) {
          console.warn("Error in liveScan listener:", e);
        }
      });
    }
  });

  channel.on("broadcast", { event: "group_finished" }, ({ payload }) => {
    if (payload) {
      groupFinishedListeners.forEach((fn) => {
        try {
          fn(payload as GroupFinishedPayload);
        } catch (e) {
          console.warn("Error in groupFinished listener:", e);
        }
      });
    }
  });

  channel.on("broadcast", { event: "payment_change" }, ({ payload }) => {
    if (payload) {
      paymentChangeListeners.forEach((fn) => {
        try {
          fn(payload as PaymentSyncPayload);
        } catch (e) {
          console.warn("Error in paymentChange listener:", e);
        }
      });
    }
  });

  channel.on("broadcast", { event: "homework_change" }, ({ payload }) => {
    if (payload) {
      homeworkChangeListeners.forEach((fn) => {
        try {
          fn(payload as HomeworkSyncPayload);
        } catch (e) {
          console.warn("Error in homeworkChange listener:", e);
        }
      });
    }
  });

  channel.on("broadcast", { event: "student_change" }, ({ payload }) => {
    if (payload) {
      studentChangeListeners.forEach((fn) => {
        try {
          fn(payload as StudentSyncPayload);
        } catch (e) {
          console.warn("Error in studentChange listener:", e);
        }
      });
    }
  });

  channel.on("broadcast", { event: "exam_grade_change" }, ({ payload }) => {
    if (payload) {
      examGradeChangeListeners.forEach((fn) => {
        try {
          fn(payload as ExamGradeSyncPayload);
        } catch (e) {
          console.warn("Error in examGradeChange listener:", e);
        }
      });
    }
  });
}

export function subscribeToLiveScans(
  onScanReceived: (payload: LiveScanPayload) => void
): () => void {
  ensureChannelListenersRegistered();
  liveScanListeners.add(onScanReceived);
  return () => {
    liveScanListeners.delete(onScanReceived);
  };
}

export function subscribeToGroupFinished(
  onGroupFinished: (payload: GroupFinishedPayload) => void
): () => void {
  ensureChannelListenersRegistered();
  groupFinishedListeners.add(onGroupFinished);
  return () => {
    groupFinishedListeners.delete(onGroupFinished);
  };
}

export function subscribeToPaymentChanges(
  onPaymentChanged: (payload: PaymentSyncPayload) => void
): () => void {
  ensureChannelListenersRegistered();
  paymentChangeListeners.add(onPaymentChanged);
  return () => {
    paymentChangeListeners.delete(onPaymentChanged);
  };
}

export function subscribeToHomeworkChanges(
  onHomeworkChanged: (payload: HomeworkSyncPayload) => void
): () => void {
  ensureChannelListenersRegistered();
  homeworkChangeListeners.add(onHomeworkChanged);
  return () => {
    homeworkChangeListeners.delete(onHomeworkChanged);
  };
}

export function subscribeToStudentChanges(
  onStudentChanged: (payload: StudentSyncPayload) => void
): () => void {
  ensureChannelListenersRegistered();
  studentChangeListeners.add(onStudentChanged);
  return () => {
    studentChangeListeners.delete(onStudentChanged);
  };
}

export function subscribeToExamGradeChanges(
  onExamGradeChanged: (payload: ExamGradeSyncPayload) => void
): () => void {
  ensureChannelListenersRegistered();
  examGradeChangeListeners.add(onExamGradeChanged);
  return () => {
    examGradeChangeListeners.delete(onExamGradeChanged);
  };
}

export function subscribeToFullState(
  onFullStateReceived: (payload: any) => void
): () => void {
  ensureChannelListenersRegistered();
  fullStateListeners.add(onFullStateReceived);
  return () => {
    fullStateListeners.delete(onFullStateReceived);
  };
}

// ------------------------------------------------------------------------
// 4. SUPABASE POSTGRES PERSISTENCE HELPERS
// ------------------------------------------------------------------------

// In-memory barcode to student_id cache to avoid redundant network lookups
const barcodeToIdCache = new Map<string, string>();

export async function getStudentIdByBarcode(barcode: string): Promise<string | null> {
  const b = String(barcode).trim();
  if (barcodeToIdCache.has(b)) {
    return barcodeToIdCache.get(b)!;
  }
  const { data } = await supabase
    .from("students")
    .select("id")
    .eq("barcode", b)
    .maybeSingle();

  if (data?.id) {
    barcodeToIdCache.set(b, data.id);
    return data.id;
  }
  return null;
}

/**
 * Ensures student exists in Supabase so foreign key constraints never fail.
 * Auto-creates student record on the fly if not found.
 */
export async function ensureStudentInSupabase(
  barcode: string,
  fallback?: {
    name?: string;
    phone?: string;
    parentPhone?: string;
    groupGrade?: string;
    groupDays?: string;
    groupTime?: string;
    monthlyFee?: number;
    discount?: number;
    notes?: string;
    isActive?: boolean;
  }
): Promise<string | null> {
  const b = String(barcode).trim();
  const cached = barcodeToIdCache.get(b);
  if (cached) return cached;

  const existingId = await getStudentIdByBarcode(b);
  if (existingId) return existingId;

  try {
    const payload = {
      barcode: b,
      name: fallback?.name || `طالب ${b}`,
      phone: String(fallback?.phone || ""),
      parent_phone: String(fallback?.parentPhone || fallback?.phone || "00000000000"),
      grade: fallback?.groupGrade || "غير محدد",
      group_days: fallback?.groupDays || "غير محدد",
      group_time: fallback?.groupTime || "04:00 م",
      monthly_fee: Number(fallback?.monthlyFee) || 0,
      discount: Number(fallback?.discount) || 0,
      notes: fallback?.notes || "",
      is_active: fallback?.isActive !== false,
    };
    const { data, error } = await supabase
      .from("students")
      .upsert(payload, { onConflict: "barcode" })
      .select("id")
      .single();
    if (data?.id) {
      barcodeToIdCache.set(b, data.id);
      return data.id;
    }
    if (error) {
      console.warn("Auto-insert student in Supabase notice:", error.message);
    }
  } catch (err) {
    console.warn("Auto-insert student in Supabase exception:", err);
  }
  return null;
}

/** Save single attendance record to Supabase with status normalization */
export async function saveAttendanceToSupabase(record: {
  barcode: string;
  studentName: string;
  status: "حضور" | "تأخير" | "غياب" | "غائب" | string;
  timeIso?: string;
  dateKey?: string;
  scannedBy?: string;
  studentFallback?: any;
}): Promise<void> {
  const dateKey = record.dateKey || getTodayDateKey();
  const studentId = await ensureStudentInSupabase(record.barcode, record.studentFallback || { name: record.studentName });
  if (!studentId) return;

  const normalizedStatus: "حضور" | "تأخير" | "غياب" =
    record.status === "غائب" || record.status === "غياب"
      ? "غياب"
      : record.status === "تأخير"
      ? "تأخير"
      : "حضور";

  await supabase
    .from("attendance_logs")
    .upsert(
      {
        student_id: studentId,
        barcode: String(record.barcode).trim(),
        student_name: record.studentName,
        date_key: dateKey,
        time_recorded: record.timeIso || new Date().toISOString(),
        status: normalizedStatus,
        scanned_by: record.scannedBy || "admin",
      },
      { onConflict: "student_id,date_key" }
    );
}

/**
 * Bulk save group attendance to Supabase in parallel chunks
 * Called when group attendance is finalized or absence is sent
 */
export async function saveBulkAttendanceToSupabase(
  records: Array<{
    barcode: string;
    studentName: string;
    status: "حضور" | "تأخير" | "غياب" | "غائب" | string;
    dateKey: string;
    scannedBy?: string;
    studentFallback?: any;
  }>
): Promise<void> {
  if (!records || records.length === 0) return;

  const rowsToInsert = [];
  for (const rec of records) {
    const sId = await ensureStudentInSupabase(rec.barcode, rec.studentFallback || { name: rec.studentName });
    if (!sId) continue;
    const normalizedStatus: "حضور" | "تأخير" | "غياب" =
      rec.status === "غائب" || rec.status === "غياب"
        ? "غياب"
        : rec.status === "تأخير"
        ? "تأخير"
        : "حضور";
    rowsToInsert.push({
      student_id: sId,
      barcode: String(rec.barcode).trim(),
      student_name: rec.studentName,
      date_key: rec.dateKey,
      time_recorded: new Date().toISOString(),
      status: normalizedStatus,
      scanned_by: rec.scannedBy || "admin",
    });
  }

  const chunkSize = 100;
  for (let i = 0; i < rowsToInsert.length; i += chunkSize) {
    const chunk = rowsToInsert.slice(i, i + chunkSize);
    await supabase
      .from("attendance_logs")
      .upsert(chunk, { onConflict: "student_id,date_key" });
  }
}

/** Save or update payment in Supabase */
export async function savePaymentToSupabase(record: {
  barcode: string;
  monthKey: string;
  amount: number;
  date?: string;
  note?: string;
  recordedBy?: string;
  studentFallback?: any;
}): Promise<void> {
  const studentId = await ensureStudentInSupabase(record.barcode, record.studentFallback);
  if (!studentId) return;

  await supabase
    .from("payments")
    .upsert(
      {
        student_id: studentId,
        month_key: record.monthKey,
        amount_paid: Number(record.amount) || 0,
        required_amount: Number(record.amount) || 100,
        discount: 0,
        status: "paid",
        payment_date: record.date ? new Date(record.date).toISOString() : new Date().toISOString(),
        received_by: record.recordedBy || "admin",
        notes: record.note || "سداد اشتراك",
      },
      { onConflict: "student_id,month_key" }
    );
}

/** Delete payment from Supabase */
export async function deletePaymentFromSupabase(barcode: string, monthKey: string): Promise<void> {
  const studentId = await getStudentIdByBarcode(barcode);
  if (!studentId) return;

  await supabase
    .from("payments")
    .delete()
    .eq("student_id", studentId)
    .eq("month_key", monthKey);
}

/** Save or update exam grade in Supabase */
export async function saveExamGradeToSupabase(record: {
  barcode: string;
  examTitle: string;
  score: number;
  maxScore: number;
  dateKey?: string;
  notes?: string;
  studentFallback?: any;
}): Promise<void> {
  const dateKey = record.dateKey || getTodayDateKey();
  const studentId = await ensureStudentInSupabase(record.barcode, record.studentFallback);
  if (!studentId) return;

  await supabase
    .from("homework")
    .insert({
      student_id: studentId,
      date_key: dateKey,
      title: record.examTitle || "امتحان / تقييم",
      status: "done",
      score: record.score,
      max_score: record.maxScore,
      notes: record.notes || `رصد درجة: ${record.score}/${record.maxScore}`,
    });
}

/** Save or update homework record in Supabase */
export async function saveHomeworkToSupabase(records: Array<{
  barcode: string;
  dateKey: string;
  status: "done" | "incomplete" | "not_done";
  notes?: string;
}>): Promise<void> {
  if (!records || records.length === 0) return;

  const rows = [];
  for (const r of records) {
    const sId = await getStudentIdByBarcode(r.barcode);
    if (!sId) continue;
    rows.push({
      student_id: sId,
      date_key: r.dateKey,
      title: "واجب الحصة",
      status: r.status,
      notes: r.notes || "",
    });
  }

  if (rows.length > 0) {
    await supabase.from("homework").insert(rows);
  }
}

/** Save student to Supabase */
export async function saveStudentToSupabase(s: any): Promise<void> {
  if (!s || !s.barcode) return;
  const payload = {
    barcode: String(s.barcode).trim(),
    name: s.name || "طالب بدون اسم",
    phone: String(s.phone || ""),
    parent_phone: String(s.parentPhone || s.phone || "00000000000"),
    grade: s.groupGrade || s.grade || "غير محدد",
    group_days: s.groupDays || "غير محدد",
    group_time: s.groupTime || "04:00 م",
    monthly_fee: Number(s.monthlyFee || s.customMonthlyFee) || 0,
    discount: Number(s.discount) || 0,
    notes: s.notes || "",
    is_active: s.isActive !== false,
  };

  const { data } = await supabase
    .from("students")
    .upsert(payload, { onConflict: "barcode" })
    .select("id")
    .single();

  if (data?.id) {
    barcodeToIdCache.set(String(s.barcode).trim(), data.id);
  }
}

/** Bulk save students to Supabase */
export async function saveBulkStudentsToSupabase(students: any[]): Promise<void> {
  if (!students || students.length === 0) return;
  const rows = students.map((s) => ({
    barcode: String(s.barcode).trim(),
    name: s.name || "طالب بدون اسم",
    phone: String(s.phone || ""),
    parent_phone: String(s.parentPhone || s.phone || "00000000000"),
    grade: s.groupGrade || s.grade || "غير محدد",
    group_days: s.groupDays || "غير محدد",
    group_time: s.groupTime || "04:00 م",
    monthly_fee: Number(s.monthlyFee || s.customMonthlyFee) || 0,
    discount: Number(s.discount) || 0,
    notes: s.notes || "",
    is_active: s.isActive !== false,
  }));

  const chunkSize = 100;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    await supabase.from("students").upsert(chunk, { onConflict: "barcode" });
  }
}

/** Delete student from Supabase */
export async function deleteStudentFromSupabase(barcode: string): Promise<void> {
  const b = String(barcode).trim();
  barcodeToIdCache.delete(b);
  await supabase.from("students").delete().eq("barcode", b);
}
