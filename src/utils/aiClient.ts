import { Student } from "../types";

export interface StudentDiagnosticResult {
  studentName: string;
  overallStatus: "ممتاز" | "جيد جداً" | "جيد" | "يحتاج متابعة" | "حرج";
  riskScore: number;
  academicStrengths: string[];
  areasOfConcern: string[];
  tailoredActionSteps: string[];
  parentSummaryArabic: string;
  teacherPedagogicalTip: string;
  isFallback: boolean;
  generatedAt: string;
  processingTimeMs?: number;
}

export interface SmartNotificationResult {
  studentName: string;
  title: string;
  tone: "مشجع وإيجابي" | "تنبيه ودي" | "إنذار جاد" | "رسمي";
  formattedMessage: string;
  keyHighlight: string;
  isFallback: boolean;
  generatedAt: string;
}

export interface AiHealthStatus {
  status: string;
  model: string;
  hasApiKey: boolean;
  activeRequestsInFlight: number;
  queuedRequests: number;
  remainingRpmQuota: number;
  recentCallsPastMinute: number;
}

/**
 * Ensures session and device isolation across multiple devices / browser tabs
 */
export function getDeviceId(): string {
  if (typeof window === "undefined" || typeof localStorage === "undefined") {
    return "server_device";
  }
  let deviceId = localStorage.getItem("aiman_device_id");
  if (!deviceId) {
    deviceId = `dev_${Math.random().toString(36).substring(2, 9)}_${Date.now()}`;
    localStorage.setItem("aiman_device_id", deviceId);
  }
  return deviceId;
}

export function getSessionId(): string {
  if (typeof window === "undefined" || typeof sessionStorage === "undefined") {
    return "server_session";
  }
  let sessionId = sessionStorage.getItem("aiman_session_id");
  if (!sessionId) {
    sessionId = `sess_${Math.random().toString(36).substring(2, 9)}_${Date.now()}`;
    sessionStorage.setItem("aiman_session_id", sessionId);
  }
  return sessionId;
}

/**
 * Client-Side Diagnostic Fallback Generator
 */
function generateClientDiagnosticFallback(
  student: Student,
  attendanceRate: number,
  examAvg: number,
  isUnpaid: boolean
): StudentDiagnosticResult {
  let overallStatus: StudentDiagnosticResult["overallStatus"] = "جيد";
  let riskScore = 20;
  const academicStrengths: string[] = [];
  const areasOfConcern: string[] = [];
  const tailoredActionSteps: string[] = [];

  if (examAvg >= 85) {
    overallStatus = "ممتاز";
    academicStrengths.push(`تحصيل دراسي متميز في الرياضيات بمعدل ${examAvg}%`);
    riskScore = 10;
  } else if (examAvg >= 70) {
    overallStatus = "جيد جداً";
    academicStrengths.push(`أداء متوازن بمعدل ${examAvg}% مع قابلية للتطوير`);
    riskScore = 25;
  } else if (examAvg < 50) {
    overallStatus = "حرج";
    areasOfConcern.push(`تراجع ملحوظ في الدرجات (${examAvg}%) يحتاج معالجة فورية`);
    riskScore = 80;
  } else {
    overallStatus = "يحتاج متابعة";
    areasOfConcern.push(`مستوى درجات متوسط (${examAvg}%) يتطلب تكثيف التدريب`);
    riskScore = 50;
  }

  if (attendanceRate >= 90) {
    academicStrengths.push(`حضور منتظم ومثالي بنسبة ${attendanceRate}%`);
  } else if (attendanceRate < 75) {
    areasOfConcern.push(`نسبة غياب (${100 - attendanceRate}%) قد تعيق مواكبة الدروس`);
    riskScore = Math.min(100, riskScore + 20);
  }

  if (isUnpaid) {
    areasOfConcern.push("رسوم الاشتراك الشهري مستحقة السداد");
  }

  tailoredActionSteps.push("حل التمارين الإضافية ومراجعة مسائل الواجب بتركيز");
  tailoredActionSteps.push("تخصيص وقت يومي منتظم لمراجعة العمليات والقوانين الرياضية");

  const parentSummaryArabic = `تحية طيبة لولي أمر الطالب/ة (${student.name})، نود إحاطتكم بأن تقييم الأداء الحالي (${overallStatus}) بمعدل اختبارات ${examAvg}% وحضور ${attendanceRate}%. نوصي بمساندة الطالب ومتابعته المنزلية المستمرة لضمان تحقيق أعلى المراتب. مع تحيات ميس إيمان الدمشيتي.`;

  return {
    studentName: student.name,
    overallStatus,
    riskScore,
    academicStrengths: academicStrengths.length > 0 ? academicStrengths : ["حرص الطالب على الحضور والمشاركة"],
    areasOfConcern: areasOfConcern.length > 0 ? areasOfConcern : ["المحافظة على وتيرة التدريب اليومي"],
    tailoredActionSteps,
    parentSummaryArabic,
    teacherPedagogicalTip: "تعزيز ثقة الطالب بنفسه من خلال توجيه أسئلة تدريجية في بداية كل حصة.",
    isFallback: true,
    generatedAt: new Date().toISOString(),
  };
}

/**
 * Asynchronous, concurrent-safe API call to evaluate a student via Gemini API
 */
export async function requestStudentDiagnosis(params: {
  student: Student;
  attendanceRate: number;
  examAvg: number;
  isUnpaid: boolean;
  notes?: string;
  timeoutMs?: number;
}): Promise<StudentDiagnosticResult> {
  const { student, attendanceRate, examAvg, isUnpaid, notes, timeoutMs = 20000 } = params;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch("/api/ai/analyze-student", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Device-ID": getDeviceId(),
        "X-Session-ID": getSessionId(),
      },
      body: JSON.stringify({
        student,
        attendanceRate,
        examAvg,
        isUnpaid,
        notes,
      }),
      signal: controller.signal,
    });

    clearTimeout(timer);

    if (!res.ok) {
      console.warn(`[AI Client] Server responded with status ${res.status}. Using fallback.`);
      return generateClientDiagnosticFallback(student, attendanceRate, examAvg, isUnpaid);
    }

    const payload = await res.json();
    if (payload.success && payload.data) {
      return payload.data as StudentDiagnosticResult;
    }

    return generateClientDiagnosticFallback(student, attendanceRate, examAvg, isUnpaid);
  } catch (err: any) {
    clearTimeout(timer);
    console.warn("[AI Client Error] Connection or timeout error during diagnosis. Using structured fallback.", err?.message);
    return generateClientDiagnosticFallback(student, attendanceRate, examAvg, isUnpaid);
  }
}

/**
 * Asynchronous, concurrent-safe call to generate a tailored smart notification
 */
export async function requestSmartNotification(params: {
  student: Student;
  messageType: string;
  contextData?: string;
  timeoutMs?: number;
}): Promise<SmartNotificationResult> {
  const { student, messageType, contextData, timeoutMs = 15000 } = params;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch("/api/ai/smart-notification", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Device-ID": getDeviceId(),
        "X-Session-ID": getSessionId(),
      },
      body: JSON.stringify({
        student,
        messageType,
        contextData,
      }),
      signal: controller.signal,
    });

    clearTimeout(timer);

    if (!res.ok) {
      throw new Error(`Server returned HTTP ${res.status}`);
    }

    const payload = await res.json();
    if (payload.success && payload.data) {
      return payload.data as SmartNotificationResult;
    }

    throw new Error("Invalid payload format");
  } catch (err: any) {
    clearTimeout(timer);
    console.warn("[AI Client Error] Fallback generated for notification:", err?.message);

    return {
      studentName: student.name,
      title: "رسالة متابعة دراسية",
      tone: "مشجع وإيجابي",
      formattedMessage: `تحية طيبة لولي أمر الطالب/ة: (${student.name}) 🌸\nنود متابعتكم بأداء الطالب في حصص الرياضيات.\nشاكرين حسن تعاونكم واهتمامكم ✨\nميس إيمان الدمشيتي 📐`,
      keyHighlight: "متابعة دراسية",
      isFallback: true,
      generatedAt: new Date().toISOString(),
    };
  }
}

/**
 * Fetches real-time Gemini AI service status and rate-limit headroom
 */
export async function getAiHealthStatus(): Promise<AiHealthStatus | null> {
  try {
    const res = await fetch("/api/ai/health", {
      headers: {
        "X-Device-ID": getDeviceId(),
        "X-Session-ID": getSessionId(),
      },
    });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}
