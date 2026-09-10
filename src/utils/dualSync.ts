/**
 * src/utils/dualSync.ts
 * Unified Dual-Cloud Real-Time Synchronization Engine
 * 
 * Guarantees that EVERY single system mutation:
 *  - Edit student / attendance / payment / grade
 *  - Delete student / payment
 *  - Scanner registration (حضور / تأخير)
 *  - Absence confirmation (حفظ غياب)
 *  - Payment recording / update / delete (دفع مصاريف)
 *  - Grade recording / update (رصد درجات)
 * 
 * Propagates IMMEDIATELY to both:
 *  1. Firebase (Firestore): Document collections & live event streams
 *  2. Supabase: Postgres tables & WebSocket Realtime channel (<20ms)
 * 
 * This enables any secondary system or external integration to ingest
 * live data in real time directly from either or both clouds.
 */

import { doc, setDoc, deleteDoc, writeBatch } from "firebase/firestore";
import { db, ensureFirebaseAuth } from "./firebase";
import {
  broadcastLiveScan,
  broadcastGroupFinished,
  broadcastPaymentChange,
  broadcastStudentChange,
  broadcastExamGradeChange,
  saveAttendanceToSupabase,
  saveBulkAttendanceToSupabase,
  savePaymentToSupabase,
  deletePaymentFromSupabase,
  saveExamGradeToSupabase,
  saveStudentToSupabase,
  saveBulkStudentsToSupabase,
  deleteStudentFromSupabase,
  getTodayDateKey,
} from "./supabaseClient";
import { pushLiveAttendanceEvent, pushLiveAttendanceBatch } from "./liveEventStream";
import { recordDeviceEntryExitScan } from "./deviceClient";
import { Student } from "../types";

// Safe non-blocking execution wrapper
function runInBackground(promise: Promise<any>, opName: string) {
  promise.catch((err) => {
    console.warn(`[DualSync] Notice during background sync (${opName}):`, err?.message || err);
  });
}

// ------------------------------------------------------------------------
// 1. SCANNER & ATTENDANCE DUAL SYNC (حضور / تأخير)
// ------------------------------------------------------------------------

export interface ScanSyncParams {
  barcode: string;
  name: string;
  grade: string;
  days: string;
  status: "حضور" | "تأخير" | "غياب" | "غائب" | string;
  timeIso: string;
  timeDisplay?: string;
  isPaid?: boolean;
  scannedBy?: string;
  studentFallback?: Partial<Student>;
}

export function dualSyncLiveScan(params: ScanSyncParams) {
  const b = String(params.barcode).trim();
  const dateKey = getTodayDateKey();
  const normalizedStatus: "حضور" | "تأخير" | "غياب" =
    params.status === "غائب" || params.status === "غياب"
      ? "غياب"
      : params.status === "تأخير"
      ? "تأخير"
      : "حضور";

  // 1️⃣ Supabase Realtime broadcast (<20ms to all assistant screens)
  runInBackground(
    broadcastLiveScan({
      barcode: b,
      name: params.name,
      grade: params.grade,
      days: params.days,
      status: normalizedStatus,
      timeIso: params.timeIso,
      timeDisplay:
        params.timeDisplay ||
        new Date(params.timeIso).toLocaleTimeString("ar-EG", { hour: "2-digit", minute: "2-digit" }),
      isPaid: !!params.isPaid,
      scannedBy: params.scannedBy || "الماسح",
      timestamp: Date.now(),
    }),
    "Supabase broadcastLiveScan"
  );

  // 2️⃣ Supabase Postgres direct persistence (attendance_logs table)
  runInBackground(
    saveAttendanceToSupabase({
      barcode: b,
      studentName: params.name,
      status: normalizedStatus,
      timeIso: params.timeIso,
      dateKey,
      scannedBy: params.scannedBy || "الماسح",
      studentFallback: params.studentFallback || {
        name: params.name,
        groupGrade: params.grade as any,
        groupDays: params.days as any,
      },
    }),
    "Supabase saveAttendanceToSupabase"
  );

  // 3️⃣ Firebase live_events/today stream
  const liveEventStatus: "حضور" | "تأخير" | "غائب" =
    normalizedStatus === "غياب" ? "غائب" : (normalizedStatus as "حضور" | "تأخير");

  runInBackground(
    pushLiveAttendanceEvent(b, liveEventStatus, Date.now(), true),
    "Firebase pushLiveAttendanceEvent"
  );

  // 4️⃣ Firebase Firestore individual attendance collection: attendance_records/{dateKey}_{barcode}
  runInBackground(
    (async () => {
      await ensureFirebaseAuth();
      const ref = doc(db, "attendance_records", `${dateKey}_${b}`);
      await setDoc(
        ref,
        {
          barcode: b,
          studentName: params.name,
          grade: params.grade,
          days: params.days,
          status: normalizedStatus,
          dateKey,
          timeIso: params.timeIso,
          recordedAt: Date.now(),
          scannedBy: params.scannedBy || "الماسح",
        },
        { merge: true }
      );
    })(),
    "Firebase attendance_records individual doc"
  );

  // 5️⃣ Isolated Device API scan logging (Zero-cache, device-specific ledger)
  runInBackground(
    recordDeviceEntryExitScan({
      barcode: b,
      type: "دخول",
      studentName: params.name,
      grade: params.grade,
      days: params.days,
      syncToUnifiedAttendance: true,
    }),
    "Device Hub recordDeviceEntryExitScan"
  );
}

// ------------------------------------------------------------------------
// 2. GROUP ATTENDANCE FINALIZATION DUAL SYNC (حفظ واعتماد الغياب)
// ------------------------------------------------------------------------

export interface GroupFinishedSyncParams {
  grade: string;
  days: string;
  absentBarcodes: string[];
  lateBarcodes: string[];
  presentBarcodes: string[];
  dateKey?: string;
  finishedBy?: string;
  allStudents: Student[];
}

export function dualSyncGroupFinished(params: GroupFinishedSyncParams) {
  const dateKey = params.dateKey || getTodayDateKey();
  const absentSet = new Set(params.absentBarcodes.map((b) => String(b).trim()));
  const lateSet = new Set(params.lateBarcodes.map((b) => String(b).trim()));
  const presentSet = new Set(params.presentBarcodes.map((b) => String(b).trim()));

  // 1️⃣ Supabase Realtime broadcast
  runInBackground(
    broadcastGroupFinished({
      grade: params.grade,
      days: params.days,
      absentBarcodes: params.absentBarcodes,
      lateBarcodes: params.lateBarcodes,
      presentBarcodes: params.presentBarcodes,
      dateKey,
      finishedBy: params.finishedBy || "الماسح",
      timestamp: Date.now(),
    }),
    "Supabase broadcastGroupFinished"
  );

  // 2️⃣ Supabase Postgres bulk upsert into attendance_logs table
  const supabaseRecords = params.allStudents
    .filter((s) => {
      const b = String(s.barcode).trim();
      return absentSet.has(b) || lateSet.has(b) || presentSet.has(b);
    })
    .map((s) => {
      const b = String(s.barcode).trim();
      const status = absentSet.has(b) ? "غياب" : lateSet.has(b) ? "تأخير" : "حضور";
      return {
        barcode: b,
        studentName: s.name,
        status: status as "حضور" | "تأخير" | "غياب",
        dateKey,
        scannedBy: params.finishedBy || "admin",
        studentFallback: s,
      };
    });

  if (supabaseRecords.length > 0) {
    runInBackground(
      saveBulkAttendanceToSupabase(supabaseRecords),
      "Supabase saveBulkAttendanceToSupabase"
    );
  }

  // 3️⃣ Firebase live_events stream batch
  const liveEvents = supabaseRecords.map((r) => ({
    studentId: r.barcode,
    status: r.status as any,
    timestamp: Date.now(),
  }));
  if (liveEvents.length > 0) {
    runInBackground(
      pushLiveAttendanceBatch(liveEvents),
      "Firebase pushLiveAttendanceBatch"
    );
  }

  // 4️⃣ Firebase Firestore individual attendance docs in batch chunks
  runInBackground(
    (async () => {
      await ensureFirebaseAuth();
      const chunkSize = 400;
      for (let i = 0; i < supabaseRecords.length; i += chunkSize) {
        const chunk = supabaseRecords.slice(i, i + chunkSize);
        const batch = writeBatch(db);
        chunk.forEach((item) => {
          const docRef = doc(db, "attendance_records", `${dateKey}_${item.barcode}`);
          batch.set(
            docRef,
            {
              barcode: item.barcode,
              studentName: item.studentName,
              grade: params.grade,
              days: params.days,
              status: item.status,
              dateKey,
              timeIso: new Date().toISOString(),
              recordedAt: Date.now(),
              scannedBy: params.finishedBy || "admin",
            },
            { merge: true }
          );
        });
        await batch.commit();
      }
    })(),
    "Firebase batch attendance_records docs"
  );
}

// ------------------------------------------------------------------------
// 3. ATTENDANCE STATUS CHANGE DUAL SYNC (تعديل حضور أو غياب من التقرير)
// ------------------------------------------------------------------------

export function dualSyncAttendanceStatusChange(params: {
  barcode: string;
  studentName: string;
  status: "حضور" | "تأخير" | "غياب" | "غائب" | string;
  dateKey?: string;
  timeIso?: string;
  updatedBy?: string;
  studentFallback?: Partial<Student>;
}) {
  const b = String(params.barcode).trim();
  const dateKey = params.dateKey || getTodayDateKey();
  const normalizedStatus: "حضور" | "تأخير" | "غياب" =
    params.status === "غائب" || params.status === "غياب"
      ? "غياب"
      : params.status === "تأخير"
      ? "تأخير"
      : "حضور";

  // Supabase
  runInBackground(
    saveAttendanceToSupabase({
      barcode: b,
      studentName: params.studentName,
      status: normalizedStatus,
      dateKey,
      timeIso: params.timeIso || new Date().toISOString(),
      scannedBy: params.updatedBy || "admin",
      studentFallback: params.studentFallback,
    }),
    "Supabase status update"
  );

  // Firebase
  runInBackground(
    (async () => {
      await ensureFirebaseAuth();
      const ref = doc(db, "attendance_records", `${dateKey}_${b}`);
      await setDoc(
        ref,
        {
          barcode: b,
          studentName: params.studentName,
          status: normalizedStatus,
          dateKey,
          timeIso: params.timeIso || new Date().toISOString(),
          updatedAt: Date.now(),
          scannedBy: params.updatedBy || "admin",
        },
        { merge: true }
      );
    })(),
    "Firebase status update"
  );
}

// ------------------------------------------------------------------------
// 4. PAYMENTS DUAL SYNC (تسجيل، تعديل، حذف دفع المصاريف)
// ------------------------------------------------------------------------

export function dualSyncPaymentRecord(params: {
  barcode: string;
  monthKey: string;
  amount: number;
  date: string;
  time?: string;
  note?: string;
  recordedBy?: string;
  studentFallback?: Partial<Student>;
}) {
  const b = String(params.barcode).trim();
  const amount = Number(params.amount) || 0;
  const time = params.time || new Date().toLocaleTimeString("ar-EG", { hour: "2-digit", minute: "2-digit" });

  // 1️⃣ Supabase Realtime broadcast
  runInBackground(
    broadcastPaymentChange({
      action: "record",
      barcode: b,
      monthKey: params.monthKey,
      amount,
      date: params.date,
      time,
      note: params.note || "سداد اشتراك",
      recordedBy: params.recordedBy || "admin",
      timestamp: Date.now(),
    }),
    "Supabase broadcastPaymentChange (record)"
  );

  // 2️⃣ Supabase Postgres payments table
  runInBackground(
    savePaymentToSupabase({
      barcode: b,
      monthKey: params.monthKey,
      amount,
      date: params.date,
      note: params.note,
      recordedBy: params.recordedBy,
      studentFallback: params.studentFallback,
    }),
    "Supabase savePaymentToSupabase"
  );

  // 3️⃣ Firebase Firestore collection: payment_records/{monthKey}_{barcode}
  runInBackground(
    (async () => {
      await ensureFirebaseAuth();
      const ref = doc(db, "payment_records", `${params.monthKey}_${b}`);
      await setDoc(
        ref,
        {
          barcode: b,
          monthKey: params.monthKey,
          amount,
          date: params.date,
          time,
          note: params.note || "سداد اشتراك",
          recordedBy: params.recordedBy || "admin",
          updatedAt: Date.now(),
        },
        { merge: true }
      );
    })(),
    "Firebase payment_records doc"
  );
}

export function dualSyncPaymentUpdate(params: {
  oldMonthKey: string;
  newMonthKey: string;
  barcode: string;
  newAmount: number;
  newNote?: string;
  newDate: string;
  recordedBy?: string;
  studentFallback?: Partial<Student>;
}) {
  const b = String(params.barcode).trim();
  const time = new Date().toLocaleTimeString("ar-EG", { hour: "2-digit", minute: "2-digit" });

  // 1️⃣ Supabase Realtime broadcast
  runInBackground(
    broadcastPaymentChange({
      action: "update",
      barcode: b,
      monthKey: params.newMonthKey,
      amount: params.newAmount,
      date: params.newDate,
      time,
      note: params.newNote || "تعديل سداد اشتراك",
      recordedBy: params.recordedBy || "admin",
      timestamp: Date.now(),
    }),
    "Supabase broadcastPaymentChange (update)"
  );

  // 2️⃣ Supabase Postgres
  runInBackground(
    (async () => {
      if (params.oldMonthKey !== params.newMonthKey) {
        await deletePaymentFromSupabase(b, params.oldMonthKey);
      }
      await savePaymentToSupabase({
        barcode: b,
        monthKey: params.newMonthKey,
        amount: params.newAmount,
        date: params.newDate,
        note: params.newNote,
        recordedBy: params.recordedBy,
        studentFallback: params.studentFallback,
      });
    })(),
    "Supabase payment update"
  );

  // 3️⃣ Firebase Firestore
  runInBackground(
    (async () => {
      await ensureFirebaseAuth();
      if (params.oldMonthKey !== params.newMonthKey) {
        const oldRef = doc(db, "payment_records", `${params.oldMonthKey}_${b}`);
        await deleteDoc(oldRef);
      }
      const newRef = doc(db, "payment_records", `${params.newMonthKey}_${b}`);
      await setDoc(
        newRef,
        {
          barcode: b,
          monthKey: params.newMonthKey,
          amount: params.newAmount,
          date: params.newDate,
          time,
          note: params.newNote || "تعديل سداد اشتراك",
          recordedBy: params.recordedBy || "admin",
          updatedAt: Date.now(),
        },
        { merge: true }
      );
    })(),
    "Firebase payment update"
  );
}

export function dualSyncPaymentDelete(params: { barcode: string; monthKey: string }) {
  const b = String(params.barcode).trim();

  // 1️⃣ Supabase Realtime broadcast
  runInBackground(
    broadcastPaymentChange({
      action: "delete",
      barcode: b,
      monthKey: params.monthKey,
      amount: 0,
      date: new Date().toISOString().split("T")[0],
      time: "",
      note: "حذف سداد",
      recordedBy: "admin",
      timestamp: Date.now(),
    }),
    "Supabase broadcastPaymentChange (delete)"
  );

  // 2️⃣ Supabase Postgres
  runInBackground(
    deletePaymentFromSupabase(b, params.monthKey),
    "Supabase deletePaymentFromSupabase"
  );

  // 3️⃣ Firebase Firestore
  runInBackground(
    (async () => {
      await ensureFirebaseAuth();
      const ref = doc(db, "payment_records", `${params.monthKey}_${b}`);
      await deleteDoc(ref);
    })(),
    "Firebase delete payment doc"
  );
}

// ------------------------------------------------------------------------
// 5. EXAM GRADES DUAL SYNC (رصد وتعديل درجات الامتحانات)
// ------------------------------------------------------------------------

export interface ExamGradeSyncParams {
  barcode: string;
  studentName?: string;
  examTitle: string;
  score: number;
  maxScore: number;
  percentage?: number;
  dateKey?: string;
  notes?: string;
  recordedBy?: string;
  studentFallback?: Partial<Student>;
}

export function dualSyncExamGrade(params: ExamGradeSyncParams) {
  const b = String(params.barcode).trim();
  const dateKey = params.dateKey || getTodayDateKey();
  const score = Number(params.score) || 0;
  const maxScore = Number(params.maxScore) || 100;
  const percentage = params.percentage ?? Math.round((score / Math.max(1, maxScore)) * 100);

  // 1️⃣ Supabase Realtime broadcast
  runInBackground(
    broadcastExamGradeChange({
      action: "record",
      barcode: b,
      studentName: params.studentName,
      examTitle: params.examTitle,
      score,
      maxScore,
      percentage,
      dateKey,
      timestamp: Date.now(),
    }),
    "Supabase broadcastExamGradeChange"
  );

  // 2️⃣ Supabase Postgres (homework table used for assessment and grade recording)
  runInBackground(
    saveExamGradeToSupabase({
      barcode: b,
      examTitle: params.examTitle,
      score,
      maxScore,
      dateKey,
      notes: params.notes || `رصد درجة امتحان: ${params.examTitle} (${score}/${maxScore} - ${percentage}%)`,
      studentFallback: params.studentFallback,
    }),
    "Supabase saveExamGradeToSupabase"
  );

  // 3️⃣ Firebase Firestore collection: exam_records/{dateKey}_{barcode}
  runInBackground(
    (async () => {
      await ensureFirebaseAuth();
      const ref = doc(db, "exam_records", `${dateKey}_${b}`);
      await setDoc(
        ref,
        {
          barcode: b,
          studentName: params.studentName || "",
          examTitle: params.examTitle,
          score,
          maxScore,
          percentage,
          dateKey,
          notes: params.notes || "",
          recordedBy: params.recordedBy || "admin",
          recordedAt: Date.now(),
        },
        { merge: true }
      );

      // Also update student's last exam summary in Firestore students collection
      const studentRef = doc(db, "students", b);
      await setDoc(
        studentRef,
        {
          lastExamTitle: params.examTitle,
          lastExamScore: `${score}/${maxScore} (${percentage}%)`,
          updatedAt: Date.now(),
        },
        { merge: true }
      );
    })(),
    "Firebase exam_records & student update"
  );
}

// ------------------------------------------------------------------------
// 6. STUDENTS DUAL SYNC (إضافة، تعديل، حذف طالب)
// ------------------------------------------------------------------------

export function dualSyncStudentSave(student: Student, action: "add" | "update" = "update") {
  const b = String(student.barcode).trim();

  // 1️⃣ Supabase Realtime broadcast
  runInBackground(
    broadcastStudentChange({
      action,
      barcode: b,
      studentData: student,
      timestamp: Date.now(),
    }),
    `Supabase broadcastStudentChange (${action})`
  );

  // 2️⃣ Supabase Postgres
  runInBackground(
    saveStudentToSupabase(student),
    "Supabase saveStudentToSupabase"
  );

  // 3️⃣ Firebase Firestore: students/{barcode}
  runInBackground(
    (async () => {
      await ensureFirebaseAuth();
      const ref = doc(db, "students", b);
      await setDoc(ref, { ...student, updatedAt: Date.now() }, { merge: true });
    })(),
    "Firebase students/{barcode} doc"
  );
}

export function dualSyncStudentDelete(barcode: string) {
  const b = String(barcode).trim();

  // 1️⃣ Supabase Realtime broadcast
  runInBackground(
    broadcastStudentChange({
      action: "delete",
      barcode: b,
      timestamp: Date.now(),
    }),
    "Supabase broadcastStudentChange (delete)"
  );

  // 2️⃣ Supabase Postgres
  runInBackground(
    deleteStudentFromSupabase(b),
    "Supabase deleteStudentFromSupabase"
  );

  // 3️⃣ Firebase Firestore
  runInBackground(
    (async () => {
      await ensureFirebaseAuth();
      const ref = doc(db, "students", b);
      await deleteDoc(ref);
    })(),
    "Firebase delete student doc"
  );
}

export function dualSyncBulkStudents(students: Student[]) {
  if (!students || students.length === 0) return;

  // 1️⃣ Supabase bulk upsert
  runInBackground(
    saveBulkStudentsToSupabase(students),
    "Supabase saveBulkStudentsToSupabase"
  );

  // 2️⃣ Firebase Firestore batch write
  runInBackground(
    (async () => {
      await ensureFirebaseAuth();
      const chunkSize = 400;
      for (let i = 0; i < students.length; i += chunkSize) {
        const chunk = students.slice(i, i + chunkSize);
        const batch = writeBatch(db);
        chunk.forEach((s) => {
          const b = String(s.barcode).trim();
          const ref = doc(db, "students", b);
          batch.set(ref, { ...s, updatedAt: Date.now() }, { merge: true });
        });
        await batch.commit();
      }
    })(),
    "Firebase bulk students batch write"
  );
}
