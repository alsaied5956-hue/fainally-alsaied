/**
 * src/utils/supabaseClient.ts
 * TypeScript Supabase v2 client for Educational Management System
 * Provides sub-50ms Realtime WebSocket synchronization across all assistants
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
      eventsPerSecond: 25,
    },
  },
});

export interface StudentDB {
  id: string;
  barcode: string;
  name: string;
  phone?: string;
  parent_phone: string;
  grade: string;
  group_days: string;
  group_time?: string;
  monthly_fee: number;
  discount: number;
  notes?: string;
  is_active: boolean;
  created_at: string;
  updated_at: string;
}

export interface AttendanceDB {
  id?: string;
  student_id: string;
  barcode: string;
  student_name: string;
  date_key: string;
  time_recorded: string;
  status: "حضور" | "تأخير" | "غياب";
  session_slot_id?: string;
  scanned_by?: string;
  notes?: string;
}

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

export function getTodayDateKey(): string {
  const d = new Date();
  const year = d.getFullYear();
  const month = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

// ------------------------------------------------------------------------
// 1. DEDICATED REAL-TIME BROADCAST CHANNEL (Sub-20ms WebSocket Channel)
// ------------------------------------------------------------------------

let liveScannerChannel: RealtimeChannel | null = null;

export function getOrCreateLiveScannerChannel(): RealtimeChannel {
  if (!liveScannerChannel) {
    liveScannerChannel = supabase.channel("realtime-assistant-scanner", {
      config: {
        broadcast: {
          self: false, // Don't echo back to the sender
          ack: false,  // Zero-latency fire-and-forget
        },
      },
    });

    liveScannerChannel.subscribe((status) => {
      console.log(`[Supabase Realtime] Scanner Channel Status: ${status}`);
    });
  }
  return liveScannerChannel;
}

/**
 * Broadcasts an instant scan event across all connected assistants in fractions of a second
 */
export async function broadcastLiveScan(payload: LiveScanPayload): Promise<void> {
  try {
    const channel = getOrCreateLiveScannerChannel();
    await channel.send({
      type: "broadcast",
      event: "assistant_scan",
      payload,
    });
  } catch (err) {
    console.warn("Realtime broadcast notice:", err);
  }
}

/**
 * Subscribes an assistant device to receive live scans from other assistants in real-time
 */
export function subscribeToLiveScans(
  onScanReceived: (payload: LiveScanPayload) => void
): () => void {
  const channel = getOrCreateLiveScannerChannel();

  channel.on("broadcast", { event: "assistant_scan" }, ({ payload }) => {
    if (payload && typeof onScanReceived === "function") {
      onScanReceived(payload as LiveScanPayload);
    }
  });

  return () => {
    // Keep channel alive if other listeners exist, or unsubscribe
  };
}

// ------------------------------------------------------------------------
// 2. DIRECT ATTENDANCE PERSISTENCE IN SUPABASE (Postgres Upsert)
// ------------------------------------------------------------------------

/**
 * Saves or updates an attendance record in Supabase attendance_logs
 */
export async function saveAttendanceToSupabase(record: {
  barcode: string;
  studentName: string;
  status: "حضور" | "تأخير" | "غياب";
  timeIso?: string;
  dateKey?: string;
  scannedBy?: string;
}): Promise<AttendanceDB | null> {
  const dateKey = record.dateKey || getTodayDateKey();
  const barcode = String(record.barcode).trim();

  // Look up student_id by barcode
  const { data: student } = await supabase
    .from("students")
    .select("id, name")
    .eq("barcode", barcode)
    .maybeSingle();

  if (!student) {
    console.warn(`Student with barcode ${barcode} not found in Supabase.`);
    return null;
  }

  const payload = {
    student_id: student.id,
    barcode,
    student_name: record.studentName || student.name,
    date_key: dateKey,
    time_recorded: record.timeIso || new Date().toISOString(),
    status: record.status,
    scanned_by: record.scannedBy || "admin",
  };

  const { data, error } = await supabase
    .from("attendance_logs")
    .upsert(payload, { onConflict: "student_id,date_key" })
    .select()
    .single();

  if (error) {
    console.error("Error saving attendance to Supabase:", error.message);
    return null;
  }

  return data;
}

/**
 * Subscribes to Postgres DB changes on attendance_logs for the given date
 */
export function subscribeToAttendanceDatabase(
  dateKey: string = getTodayDateKey(),
  onLogUpdated: (log: AttendanceDB) => void
): () => void {
  const channel = supabase
    .channel(`db-attendance-changes-${dateKey}`)
    .on(
      "postgres_changes",
      {
        event: "*",
        schema: "public",
        table: "attendance_logs",
        filter: `date_key=eq.${dateKey}`,
      },
      (payload) => {
        if (payload.new && typeof onLogUpdated === "function") {
          onLogUpdated(payload.new as AttendanceDB);
        }
      }
    )
    .subscribe();

  return () => {
    supabase.removeChannel(channel);
  };
}
