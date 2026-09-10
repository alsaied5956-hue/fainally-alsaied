import { GoogleGenAI, Type } from "@google/genai";
import { Student } from "../types";

/**
 * Gemini Service Configuration & Limits
 */
const GEMINI_MODEL = "gemini-3.8-flash";
const MAX_CONCURRENT_CALLS = 3;
const MAX_REQUESTS_PER_MINUTE = 20;
const MAX_RETRIES = 4;
const INITIAL_RETRY_DELAY_MS = 1200;
const MAX_RETRY_DELAY_MS = 15000;
const BACKOFF_FACTOR = 2;

// In-memory response cache to prevent duplicate calls from multiple devices
interface CachedResult<T> {
  data: T;
  cachedAt: number;
  ttlMs: number;
}
const responseCache = new Map<string, CachedResult<any>>();
const CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes

/**
 * Concurrency & Rate Limit Semaphore
 */
class RequestQueue {
  private inFlight = 0;
  private queue: Array<() => void> = [];
  private timestamps: number[] = [];

  async acquire(): Promise<void> {
    return new Promise((resolve) => {
      const tryExecute = () => {
        const now = Date.now();
        // Remove timestamps older than 60 seconds
        this.timestamps = this.timestamps.filter((t) => now - t < 60000);

        if (this.inFlight < MAX_CONCURRENT_CALLS && this.timestamps.length < MAX_REQUESTS_PER_MINUTE) {
          this.inFlight++;
          this.timestamps.push(now);
          resolve();
        } else {
          this.queue.push(tryExecute);
          // Re-check when window opens
          const oldest = this.timestamps[0] || now;
          const waitTime = Math.max(100, 60000 - (now - oldest) + 50);
          setTimeout(() => this.drainQueue(), Math.min(waitTime, 1000));
        }
      };

      tryExecute();
    });
  }

  release(): void {
    this.inFlight = Math.max(0, this.inFlight - 1);
    this.drainQueue();
  }

  private drainQueue(): void {
    if (this.queue.length > 0 && this.inFlight < MAX_CONCURRENT_CALLS) {
      const next = this.queue.shift();
      if (next) next();
    }
  }

  getStats() {
    const now = Date.now();
    const recent = this.timestamps.filter((t) => now - t < 60000).length;
    return {
      inFlight: this.inFlight,
      queued: this.queue.length,
      requestsPastMinute: recent,
      remainingRpm: Math.max(0, MAX_REQUESTS_PER_MINUTE - recent),
    };
  }
}

const aiQueue = new RequestQueue();

/**
 * Lazy-initialized Gemini Client
 */
let genAIInstance: GoogleGenAI | null = null;

function getGenAI(): GoogleGenAI {
  if (!genAIInstance) {
    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) {
      console.warn("[Gemini Service] GEMINI_API_KEY is not set. Service will use safe fallback responses.");
    }
    genAIInstance = new GoogleGenAI({ apiKey: apiKey || "UNSET_DEV_KEY" });
  }
  return genAIInstance;
}

/**
 * Checks if an error is retryable (Rate Limit 429, Resource Exhausted, 5xx server errors, network reset)
 */
function isRetryableError(error: any): boolean {
  if (!error) return false;
  const str = String(error.message || error.status || error);
  if (
    str.includes("429") ||
    str.includes("RESOURCE_EXHAUSTED") ||
    str.includes("Quota") ||
    str.includes("rate limit") ||
    str.includes("503") ||
    str.includes("500") ||
    str.includes("502") ||
    str.includes("504") ||
    str.includes("UNAVAILABLE") ||
    str.includes("DEADLINE_EXCEEDED") ||
    str.includes("ECONNRESET") ||
    str.includes("ETIMEDOUT") ||
    str.includes("fetch failed")
  ) {
    return true;
  }
  return false;
}

/**
 * Exponential Backoff with Full Jitter for handling Rate Limits (429) & Server Glitches (5xx)
 */
async function callGeminiWithRetry<T>(
  operation: (ai: GoogleGenAI) => Promise<T>,
  contextDesc: string
): Promise<T> {
  const ai = getGenAI();

  if (!process.env.GEMINI_API_KEY) {
    throw new Error("GEMINI_API_KEY is not configured on server.");
  }

  let attempt = 0;

  while (true) {
    try {
      await aiQueue.acquire();
      try {
        return await operation(ai);
      } finally {
        aiQueue.release();
      }
    } catch (err: any) {
      attempt++;

      if (attempt > MAX_RETRIES || !isRetryableError(err)) {
        console.error(`[Gemini Error] ${contextDesc} failed permanently after attempt ${attempt}:`, err?.message || err);
        throw err;
      }

      // Calculate exponential backoff with full jitter
      const exponentialDelay = INITIAL_RETRY_DELAY_MS * Math.pow(BACKOFF_FACTOR, attempt - 1);
      const cappedDelay = Math.min(MAX_RETRY_DELAY_MS, exponentialDelay);
      const jitter = Math.floor(Math.random() * 600);
      const totalDelay = cappedDelay + jitter;

      console.warn(
        `[Gemini Retry] ${contextDesc} encountered ${err?.message?.substring(0, 100) || "error"}. Retrying attempt ${attempt}/${MAX_RETRIES} in ${totalDelay}ms...`
      );

      await new Promise((resolve) => setTimeout(resolve, totalDelay));
    }
  }
}

/**
 * Structured Output Interfaces
 */
export interface StudentDiagnosticResult {
  studentName: string;
  overallStatus: "ممتاز" | "جيد جداً" | "جيد" | "يحتاج متابعة" | "حرج";
  riskScore: number; // 0 to 100
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

/**
 * Fallback Diagnostic Generator (Ensures Zero Frontend Crashes Even If API Is Down)
 */
function createFallbackDiagnostic(
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
    academicStrengths.push(`تحصيل دراسي متميز في اختبارات الرياضيات بمعدل ${examAvg}%`);
    riskScore = Math.min(riskScore, 10);
  } else if (examAvg >= 70) {
    overallStatus = "جيد جداً";
    academicStrengths.push(`أداء جيد في مادة الرياضيات بمعدل ${examAvg}% مع قابلية للتطوير`);
    riskScore = 25;
  } else if (examAvg < 50) {
    overallStatus = "حرج";
    areasOfConcern.push(`تراجع حاد في درجات الرياضيات (${examAvg}%) يحتاج لمعالجة فورية`);
    riskScore = 85;
  } else {
    overallStatus = "يحتاج متابعة";
    areasOfConcern.push(`معدل درجات متوسط (${examAvg}%) يتطلب تكثيف التدريب`);
    riskScore = 55;
  }

  if (attendanceRate >= 90) {
    academicStrengths.push(`حضور منتظم ومشرف بنسبة ${attendanceRate}%`);
  } else if (attendanceRate < 75) {
    areasOfConcern.push(`نسبة غياب ملحوظة (${100 - attendanceRate}%) تؤثر على فهم الدروس المتراكمة`);
    riskScore = Math.min(100, riskScore + 25);
  }

  if (isUnpaid) {
    areasOfConcern.push("رسوم الاشتراك الشهري مستحقة السداد");
  }

  if (academicStrengths.length === 0) {
    academicStrengths.push("الرغبة في التعلم والقدرة على الاستجابة للتوجيه الإيجابي");
  }

  tailoredActionSteps.push("حل التمارين الإضافية بانتظام ومراجعة الواجبات غير المكتملة");
  tailoredActionSteps.push("تخصيص 20 دقيقة يومياً لمراجعة القوانين والمسائل النموذجية");
  if (areasOfConcern.length > 0) {
    tailoredActionSteps.push("حضور حصص الدعم والمراجعة المقررة قبل الاختبار القادم");
  }

  const parentSummaryArabic = `تحية طيبة لولي أمر الطالب/ة (${student.name})، نود إحاطتكم بأن تقييم الأداء الحالي (${overallStatus}) بمعدل اختبارات ${examAvg}% وحضور ${attendanceRate}%. نوصي بمساندة الطالب ومتابعته المنزلية المستمرة لضمان تحقيق أعلى المراتب. مع تحيات ميس إيمان الدمشيتي.`;

  return {
    studentName: student.name,
    overallStatus,
    riskScore,
    academicStrengths,
    areasOfConcern: areasOfConcern.length > 0 ? areasOfConcern : ["لا توجد ملاحظات سلبية حالياً"],
    tailoredActionSteps,
    parentSummaryArabic,
    teacherPedagogicalTip: "التركيز على تعزيز الثقة بالنفس واستخدام أسئلة متدرجة الصعوبة لرفع مستوى التركيز.",
    isFallback: true,
    generatedAt: new Date().toISOString(),
  };
}

/**
 * Fallback Smart Notification Generator
 */
function createFallbackNotification(
  student: Student,
  messageType: string,
  extraContext?: string
): SmartNotificationResult {
  let title = "رسالة متابعة دراسية";
  let tone: SmartNotificationResult["tone"] = "مشجع وإيجابي";
  let formattedMessage = "";
  let keyHighlight = "متابعة مستمرة";

  switch (messageType) {
    case "إنذار":
    case "غياب":
      title = "تنبيه متابعة حضور وغياب";
      tone = "تنبيه ودي";
      keyHighlight = "الالتزام بالحضور";
      formattedMessage = `السلام عليكم ورحمة الله وبركاته 🌸\nعناية ولي أمر الطالب/ة: (${student.name})\nالصف: ${student.groupGrade}\n\nنود التنويه بضرورة الالتزام بمواعيد الحصص وعدم الغياب حتى لا تتراكم الدروس والمفاهيم الرياضية.\nشاكرين لكم حرصكم الدائم ✨\nمنظومة أ. إيمان الدمشيتي 📐`;
      break;
    case "درجات":
      title = "تقرير نتيجة اختبار الرياضيات";
      tone = "مشجع وإيجابي";
      keyHighlight = "نتائج الامتحانات";
      formattedMessage = `نتيجة اختبار الرياضيات 📐\nاسم الطالب: (${student.name})\nالصف: ${student.groupGrade}\n${extraContext || "الدرجة مرصودة في سجل المتابعة"}\nنرجو مراجعة الأخطاء مع الطالب للاستفادة الكاملة 🌟\nمع تحيات ميس إيمان الدمشيتي ✨`;
      break;
    case "مصاريف":
      title = "تذكير بسداد الاشتراك الشهري";
      tone = "رسمي";
      keyHighlight = "الاشتراك الشهري";
      formattedMessage = `السلام عليكم ورحمة الله وبركاته 🌸\nعناية ولي أمر الطالب/ة: (${student.name})\nنذكركم بلطف بسداد الاشتراك الشهري لمادة الرياضيات شاكرين لكم حسن تعاونكم الدائم معنا ✨`;
      break;
    default:
      title = "رسالة تفوق وتشجيع";
      tone = "مشجع وإيجابي";
      keyHighlight = "تشجيع وتحفيز";
      formattedMessage = `رسالة تحفيزية لطالبنا المتميز (${student.name}) 🌟\nنقدر اجتهادك المستمر في حصص الرياضيات ونتطلع دائماً لأعلى المراتب والتفوق المستحق!\nميس إيمان الدمشيتي 📐✨`;
      break;
  }

  return {
    studentName: student.name,
    title,
    tone,
    formattedMessage,
    keyHighlight,
    isFallback: true,
    generatedAt: new Date().toISOString(),
  };
}

/**
 * Service Method: Analyze Student Performance with Gemini AI
 */
export async function analyzeStudentPerformance(params: {
  student: Student;
  attendanceRate: number;
  examAvg: number;
  isUnpaid: boolean;
  notes?: string;
  deviceId?: string;
  sessionId?: string;
}): Promise<StudentDiagnosticResult> {
  const { student, attendanceRate, examAvg, isUnpaid, notes } = params;
  const startTime = Date.now();

  // Cache Key for State Isolation and Deduplication
  const cacheKey = `diag_${student.barcode}_${attendanceRate}_${examAvg}_${isUnpaid}_${(student.totalExamScores || []).join("-")}`;
  const cached = responseCache.get(cacheKey);
  if (cached && Date.now() - cached.cachedAt < cached.ttlMs) {
    return {
      ...cached.data,
      processingTimeMs: Date.now() - startTime,
    };
  }

  const prompt = `
أنت مستشار تعليمي وتربوي خبير في تدريس مادة الرياضيات للمراحل الابتدائية والإعدادية.
قم بتحليل أداء هذا الطالب بدقة وموضوعية، وتقديم تقرير تشخيصي متكامل باللغة العربية الفصحى الودية والمحفزة.

بيانات الطالب:
- الاسم: ${student.name}
- الكود (الباركود): ${student.barcode}
- المرحلة / الصف الدراسي: ${student.groupGrade}
- نسبة الحضور الإجمالية: ${attendanceRate}% (أيام الغياب: ${student.totalAbsentDays || 0})
- متوسط درجات الاختبارات: ${examAvg}% (سجل الدرجات: ${(student.totalExamScores || []).join(", ") || "لا توجد درجات مرصودة"})
- نقاط التميز والتفوق: ${student.points || 0} نقطة
- حالة الاشتراك المالي: ${isUnpaid ? "غير مسدد حتى الآن" : "مسدد بالكامل"}
- ملاحظات المعلمة المسجلة: ${student.notes || notes || "لا توجد"}

المطلوب:
أخرج تحليلاً بتنسيق JSON مطابق تماماً للحقول التالية:
- overallStatus: إحدى القيم التالية فقط: ["ممتاز", "جيد جداً", "جيد", "يحتاج متابعة", "حرج"]
- riskScore: رقم من 0 إلى 100 يعبر عن حجم الخطر الأكاديمي أو التراجع (0 ممتاز و100 خطر شديد)
- academicStrengths: قائمة من 2 إلى 4 نقاط قوة واضحة ومحددة لدى الطالب
- areasOfConcern: قائمة من 1 إلى 3 جوانب تحتاج تركيز وعناية
- tailoredActionSteps: قائمة من 2 إلى 4 خطوات عملية موجهة للطالب لرفع مستواه
- parentSummaryArabic: فقرة دافئة ومحترمة ومكتوبة بعناية موجهة لولي الأمر تشرح الوضع باحترافية وتبث الطمأنينة دون تهويل
- teacherPedagogicalTip: نصيحة تربوية للمعلمة لكيفية التعامل مع هذا الطالب داخل الحصة
`.trim();

  try {
    const rawResult = await callGeminiWithRetry(async (ai) => {
      const response = await ai.models.generateContent({
        model: GEMINI_MODEL,
        contents: prompt,
        config: {
          responseMimeType: "application/json",
          responseSchema: {
            type: Type.OBJECT,
            properties: {
              overallStatus: {
                type: Type.STRING,
                description: "الحالة العامة للأداء",
              },
              riskScore: {
                type: Type.NUMBER,
                description: "مؤشر الخطر من 0 إلى 100",
              },
              academicStrengths: {
                type: Type.ARRAY,
                items: { type: Type.STRING },
                description: "نقاط القوة",
              },
              areasOfConcern: {
                type: Type.ARRAY,
                items: { type: Type.STRING },
                description: "نقاط التحسين والاهتمام",
              },
              tailoredActionSteps: {
                type: Type.ARRAY,
                items: { type: Type.STRING },
                description: "خطة العمل والخطوات الموصى بها",
              },
              parentSummaryArabic: {
                type: Type.STRING,
                description: "رسالة موجهة لولي الأمر",
              },
              teacherPedagogicalTip: {
                type: Type.STRING,
                description: "إرشاد تربوي للمعلمة",
              },
            },
            required: [
              "overallStatus",
              "riskScore",
              "academicStrengths",
              "areasOfConcern",
              "tailoredActionSteps",
              "parentSummaryArabic",
              "teacherPedagogicalTip",
            ],
          },
        },
      });

      return response.text;
    }, `Student Diagnostics for ${student.name} (${student.barcode})`);

    let parsed: any = null;
    if (rawResult) {
      try {
        parsed = JSON.parse(rawResult.trim());
      } catch (e) {
        // Strip possible markdown fences if any
        const cleaned = rawResult.replace(/```json\s*|```/g, "").trim();
        parsed = JSON.parse(cleaned);
      }
    }

    if (!parsed) {
      throw new Error("Empty or malformed JSON returned from Gemini");
    }

    const validatedResult: StudentDiagnosticResult = {
      studentName: student.name,
      overallStatus: ["ممتاز", "جيد جداً", "جيد", "يحتاج متابعة", "حرج"].includes(parsed.overallStatus)
        ? parsed.overallStatus
        : "جيد",
      riskScore: typeof parsed.riskScore === "number" ? Math.min(100, Math.max(0, parsed.riskScore)) : 25,
      academicStrengths: Array.isArray(parsed.academicStrengths) && parsed.academicStrengths.length > 0
        ? parsed.academicStrengths
        : ["الالتزام بالحضور وحسن الاستماع"],
      areasOfConcern: Array.isArray(parsed.areasOfConcern) && parsed.areasOfConcern.length > 0
        ? parsed.areasOfConcern
        : ["الاستمرار في المراجعة المنتظمة"],
      tailoredActionSteps: Array.isArray(parsed.tailoredActionSteps) && parsed.tailoredActionSteps.length > 0
        ? parsed.tailoredActionSteps
        : ["حل تمارين الكتاب المدرسي يومياً"],
      parentSummaryArabic: String(parsed.parentSummaryArabic || "").trim(),
      teacherPedagogicalTip: String(parsed.teacherPedagogicalTip || "").trim(),
      isFallback: false,
      generatedAt: new Date().toISOString(),
      processingTimeMs: Date.now() - startTime,
    };

    // Cache valid result
    responseCache.set(cacheKey, {
      data: validatedResult,
      cachedAt: Date.now(),
      ttlMs: CACHE_TTL_MS,
    });

    return validatedResult;
  } catch (error) {
    console.warn(`[Gemini Fallback Activated] Returning safe diagnostic model for student ${student.name}:`, error);
    const fallback = createFallbackDiagnostic(student, attendanceRate, examAvg, isUnpaid);
    fallback.processingTimeMs = Date.now() - startTime;
    return fallback;
  }
}

/**
 * Service Method: Generate Tailored Smart Notification with Gemini AI
 */
export async function generateSmartNotification(params: {
  student: Student;
  messageType: string;
  contextData?: string;
  deviceId?: string;
  sessionId?: string;
}): Promise<SmartNotificationResult> {
  const { student, messageType, contextData } = params;

  const prompt = `
أنت المساعد الذكي للأستاذة إيمان الدمشيتي، معلمة الرياضيات المتميزة.
اكتب رسالة واتساب تربوية شخصية ومتقنة باللغة العربية الفصحى الودية والمحترمة تناسب ولي أمر الطالب/ة.

بيانات الطالب:
- الاسم: ${student.name}
- الصف: ${student.groupGrade}
- نوع الرسالة المطلوب: ${messageType}
- تفاصيل إضافية: ${contextData || "لا توجد تفاصيل إضافية"}

الشروط:
- استخدام الرموز التعبيرية الهادفة (مثل 📐 ✨ 🌸 🌟).
- البدء بالتحية اللائقة والختام بـ "مع تحيات ميس إيمان الدمشيتي 📐✨".
- أن تكون الرسالة واضحة، مختصرة، ومحددة.
- إخراج النتيجة بتنسيق JSON مطابق للحقول:
  - title: عنوان مختصر للرسالة
  - tone: نبرة الرسالة ("مشجع وإيجابي" أو "تنبيه ودي" أو "إنذار جاد" أو "رسمي")
  - formattedMessage: نص الرسالة الكامل الجاهز للإرسال
  - keyHighlight: خلاصة الرسالة في جملة واحدة قصيرة
`.trim();

  try {
    const rawResult = await callGeminiWithRetry(async (ai) => {
      const response = await ai.models.generateContent({
        model: GEMINI_MODEL,
        contents: prompt,
        config: {
          responseMimeType: "application/json",
          responseSchema: {
            type: Type.OBJECT,
            properties: {
              title: { type: Type.STRING },
              tone: { type: Type.STRING },
              formattedMessage: { type: Type.STRING },
              keyHighlight: { type: Type.STRING },
            },
            required: ["title", "tone", "formattedMessage", "keyHighlight"],
          },
        },
      });

      return response.text;
    }, `Smart Notification for ${student.name} (${messageType})`);

    let parsed: any = null;
    if (rawResult) {
      const cleaned = rawResult.replace(/```json\s*|```/g, "").trim();
      parsed = JSON.parse(cleaned);
    }

    if (!parsed || !parsed.formattedMessage) {
      throw new Error("Invalid notification output from Gemini");
    }

    return {
      studentName: student.name,
      title: parsed.title || "تنبيه دراسي",
      tone: parsed.tone || "مشجع وإيجابي",
      formattedMessage: parsed.formattedMessage,
      keyHighlight: parsed.keyHighlight || "متابعة دراسية",
      isFallback: false,
      generatedAt: new Date().toISOString(),
    };
  } catch (error) {
    console.warn(`[Gemini Fallback Activated] Returning safe notification template for ${student.name}:`, error);
    return createFallbackNotification(student, messageType, contextData);
  }
}

/**
 * Health & Rate-Limit Diagnostics
 */
export function getAiServiceHealth() {
  const queueStats = aiQueue.getStats();
  return {
    status: process.env.GEMINI_API_KEY ? "healthy" : "fallback_mode_no_api_key",
    model: GEMINI_MODEL,
    hasApiKey: Boolean(process.env.GEMINI_API_KEY),
    maxConcurrentCalls: MAX_CONCURRENT_CALLS,
    maxRequestsPerMinute: MAX_REQUESTS_PER_MINUTE,
    activeRequestsInFlight: queueStats.inFlight,
    queuedRequests: queueStats.queued,
    recentCallsPastMinute: queueStats.requestsPastMinute,
    remainingRpmQuota: queueStats.remainingRpm,
    cacheEntries: responseCache.size,
    timestamp: new Date().toISOString(),
  };
}
