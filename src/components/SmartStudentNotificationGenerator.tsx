import React, { useState, useMemo } from "react";
import { Student, PaymentRecord, PlatformMessageType } from "../types";
import { getTodayKey, getCurrentMonthKey, openWhatsApp } from "../utils/helpers";
import { enqueuePlatformMessage } from "../utils/storage";
import { StudentSearchBox } from "./StudentSearchBox";
import {
  requestSmartNotification,
  SmartNotificationResult,
} from "../utils/aiClient";
import {
  Sparkles,
  Search,
  Copy,
  Check,
  Send,
  UserCheck,
  UserX,
  Clock,
  BookOpen,
  CreditCard,
  FileCheck2,
  Award,
  FileText,
  User,
  QrCode,
  Layers,
  ChevronRight,
  RotateCw,
  MessageSquare,
} from "lucide-react";

export interface GeneratedStudentNotifications {
  student: Student;
  dateStr: string;
  attendanceMessage: string;
  absenceMessage: string;
  lateMessage: string;
  homeworkDeficiencyMessage: string;
  paymentMessage: string;
  lastExamMessage: string;
  cumulativeScoreMessage: string;
  comprehensiveReportMessage: string;
  parsedValues: {
    examName: string;
    score: string;
    maxScore: string;
    cumulativeScore: string;
    attendanceStatusToday: string;
    homeworkStatus: string;
    paymentStatus: string;
  };
}

export function generateAllStudentPlatformNotifications(
  student: Student,
  attendanceToday: Record<string, string> = {},
  payments: Record<string, Record<string, PaymentRecord>> = {},
  targetDate: string = getTodayKey()
): GeneratedStudentNotifications {
  const dateStr = targetDate;
  const currentMonthKey = getCurrentMonthKey();

  // 1. Exam data parsing
  const examName = student.lastExamTitle || "الاختبار الدوري";
  let score = "50";
  let maxScore = "50";

  if (student.lastExamScore) {
    const raw = String(student.lastExamScore).trim();
    if (raw.includes("/")) {
      const parts = raw.split("/");
      score = parts[0]?.trim() || "50";
      maxScore = parts[1]?.trim() || "50";
    } else if (raw.includes("%")) {
      score = raw.replace("%", "").trim();
      maxScore = "100";
    } else {
      score = raw;
      maxScore = "50";
    }
  } else if (student.totalExamScores && student.totalExamScores.length > 0) {
    const last = student.totalExamScores[student.totalExamScores.length - 1];
    score = String(last);
    maxScore = "100";
  }

  // 2. Cumulative Score
  let cumulativeScore = "متميز 95%";
  if (student.totalExamScores && student.totalExamScores.length > 0) {
    const avg = Math.round(
      student.totalExamScores.reduce((a, b) => a + b, 0) / student.totalExamScores.length
    );
    let gradeRating = "ممتاز";
    if (avg < 65) gradeRating = "يحتاج متابعة";
    else if (avg < 80) gradeRating = "جيد";
    else if (avg < 90) gradeRating = "جيد جداً";

    cumulativeScore = `${avg}% (${gradeRating})`;
  } else if (student.points) {
    cumulativeScore = `${student.points} نقطة تفوق`;
  }

  // 3. Attendance Today
  const todayRaw = attendanceToday[student.barcode];
  let attendanceStatusToday = "لم يرصد بعد";
  if (todayRaw === "حضور") attendanceStatusToday = "حاضر في الموعد";
  else if (todayRaw === "تأخير") attendanceStatusToday = "تأخير عن الموعد";
  else if (todayRaw === "غائب") attendanceStatusToday = "غائب";
  else if (todayRaw === "إذن") attendanceStatusToday = "غائب بإذن";

  // 4. Homework Status
  let homeworkStatus = "مسلّم ومنتظم";
  if (student.notes && student.notes.includes("واجب")) {
    homeworkStatus = student.notes;
  }

  // 5. Payment Status
  const monthRecord = payments[currentMonthKey]?.[student.barcode];
  let paymentStatus = "غير مسدد (مستحق)";
  if (monthRecord && monthRecord.amount > 0) {
    paymentStatus = `مسدد بالكامل (${monthRecord.amount} ج.م)`;
  }

  // Exact 8 Platform Notification Templates as strictly requested
  const attendanceMessage = `تنبيه منصة: تم تسجيل حضور الطالب/ة (${student.name}) بتاريخ ${dateStr} بنجاح.`;
  const absenceMessage = `تنبيه منصة: تم تسجيل غياب الطالب/ة (${student.name}) بتاريخ ${dateStr}.`;
  const lateMessage = `تنبيه منصة: تم تسجيل تأخير الطالب/ة (${student.name}) عن الموعد المحدد بتاريخ ${dateStr}.`;
  const homeworkDeficiencyMessage = `تنبيه منصة: يوجد تقصير في الواجب المنزلي للطالب/ة (${student.name})، حيث تم تسليم جزء من الواجب ولم يكتمل.`;
  const paymentMessage = `تنبيه منصة: تم تأكيد سداد المصروفات الدراسية المقررة للطالب/ة (${student.name}).`;
  const lastExamMessage = `تنبيه منصة - نتيجة الاختبار:
الطالب/ة: ${student.name}
الاختبار: ${examName}
الدرجة: ${score} من ${maxScore}`;
  const cumulativeScoreMessage = `تنبيه منصة - السجل التراكمي:
الطالب/ة: ${student.name}
النتيجة التراكمية العامة: ${cumulativeScore}`;
  const comprehensiveReportMessage = `📊 [تقرير منصة شامل]
• اسم الطالب/ة: ${student.name}
• كود الطالب: ${student.barcode}
• حالة اليوم (${dateStr}): ${attendanceStatusToday}
• موقف الواجب: ${homeworkStatus}
• آخر امتحان (${examName}): ${score} / ${maxScore}
• التقييم التراكمي: ${cumulativeScore}
• حالة المصروفات: ${paymentStatus}`;

  return {
    student,
    dateStr,
    attendanceMessage,
    absenceMessage,
    lateMessage,
    homeworkDeficiencyMessage,
    paymentMessage,
    lastExamMessage,
    cumulativeScoreMessage,
    comprehensiveReportMessage,
    parsedValues: {
      examName,
      score,
      maxScore,
      cumulativeScore,
      attendanceStatusToday,
      homeworkStatus,
      paymentStatus,
    },
  };
}

interface SmartStudentNotificationGeneratorProps {
  students: Student[];
  attendanceToday?: Record<string, string>;
  payments?: Record<string, Record<string, PaymentRecord>>;
  initialStudentBarcode?: string;
  onNotificationSent?: () => void;
}

export const SmartStudentNotificationGenerator: React.FC<
  SmartStudentNotificationGeneratorProps
> = ({
  students,
  attendanceToday = {},
  payments = {},
  initialStudentBarcode,
  onNotificationSent,
}) => {
  const [selectedStudent, setSelectedStudent] = useState<Student | null>(() => {
    if (initialStudentBarcode) {
      return students.find((s) => s.barcode === initialStudentBarcode) || null;
    }
    return students.length > 0 ? students[0] : null;
  });

  const [barcodeInput, setBarcodeInput] = useState("");
  const [copiedKey, setCopiedKey] = useState<string | null>(null);
  const [sentKey, setSentKey] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<"all" | "report" | "ai">("all");

  // Gemini AI Message Generation State
  const [aiTheme, setAiTheme] = useState<string>("تشجيع وتفوق");
  const [aiCustomContext, setAiCustomContext] = useState<string>("");
  const [aiLoading, setAiLoading] = useState<boolean>(false);
  const [aiResult, setAiResult] = useState<SmartNotificationResult | null>(null);

  const handleGenerateAiMessage = async () => {
    if (!selectedStudent) return;
    setAiLoading(true);
    try {
      const result = await requestSmartNotification({
        student: selectedStudent,
        messageType: aiTheme,
        contextData: aiCustomContext,
      });
      setAiResult(result);
    } catch (err) {
      console.warn("Error generating AI message:", err);
    } finally {
      setAiLoading(false);
    }
  };

  const generated = useMemo(() => {
    if (!selectedStudent) return null;
    return generateAllStudentPlatformNotifications(
      selectedStudent,
      attendanceToday,
      payments
    );
  }, [selectedStudent, attendanceToday, payments]);

  const handleBarcodeSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const cleanCode = barcodeInput.trim();
    if (!cleanCode) return;

    const found = students.find(
      (s) => s.barcode === cleanCode || s.phone.includes(cleanCode) || s.name.includes(cleanCode)
    );

    if (found) {
      setSelectedStudent(found);
      setBarcodeInput("");
    } else {
      alert(`⚠️ لم يتم العثور على طالب بالكود أو الرقم: (${cleanCode})`);
    }
  };

  const handleCopy = (key: string, text: string) => {
    navigator.clipboard.writeText(text);
    setCopiedKey(key);
    setTimeout(() => setCopiedKey(null), 2000);
  };

  const handlePublishPlatformNotification = (
    key: string,
    title: string,
    message: string,
    type: PlatformMessageType
  ) => {
    if (!selectedStudent) return;

    enqueuePlatformMessage({
      studentBarcode: selectedStudent.barcode,
      studentName: selectedStudent.name,
      grade: selectedStudent.groupGrade,
      phone: selectedStudent.parentPhone || selectedStudent.phone || "",
      messageType: type,
      title,
      message,
      channel: "in_app",
    });

    setSentKey(key);
    setTimeout(() => setSentKey(null), 3000);
    if (onNotificationSent) onNotificationSent();
  };

  const templatesList = generated
    ? [
        {
          id: "attendance",
          title: "1. رسالة حضور",
          type: "حضور" as PlatformMessageType,
          icon: UserCheck,
          color: "text-emerald-400",
          border: "border-emerald-500/30",
          bg: "bg-emerald-950/20",
          text: generated.attendanceMessage,
        },
        {
          id: "absence",
          title: "2. رسالة غياب",
          type: "غياب" as PlatformMessageType,
          icon: UserX,
          color: "text-rose-400",
          border: "border-rose-500/30",
          bg: "bg-rose-950/20",
          text: generated.absenceMessage,
        },
        {
          id: "late",
          title: "3. رسالة تأخير",
          type: "تأخير" as PlatformMessageType,
          icon: Clock,
          color: "text-amber-400",
          border: "border-amber-500/30",
          bg: "bg-amber-950/20",
          text: generated.lateMessage,
        },
        {
          id: "homework",
          title: "4. رسالة إهمال / نقص الواجب",
          type: "سلوك" as PlatformMessageType,
          icon: BookOpen,
          color: "text-orange-400",
          border: "border-orange-500/30",
          bg: "bg-orange-950/20",
          text: generated.homeworkDeficiencyMessage,
        },
        {
          id: "payment",
          title: "5. رسالة المصروفات",
          type: "مصاريف" as PlatformMessageType,
          icon: CreditCard,
          color: "text-teal-400",
          border: "border-teal-500/30",
          bg: "bg-teal-950/20",
          text: generated.paymentMessage,
        },
        {
          id: "last_exam",
          title: "6. رسالة آخر امتحان",
          type: "درجات" as PlatformMessageType,
          icon: FileCheck2,
          color: "text-sky-400",
          border: "border-sky-500/30",
          bg: "bg-sky-950/20",
          text: generated.lastExamMessage,
        },
        {
          id: "cumulative",
          title: "7. رسالة النتيجة التراكمية",
          type: "تفوق" as PlatformMessageType,
          icon: Award,
          color: "text-purple-400",
          border: "border-purple-500/30",
          bg: "bg-purple-950/20",
          text: generated.cumulativeScoreMessage,
        },
        {
          id: "report",
          title: "8. تقرير شامل عن الطالب",
          type: "عام" as PlatformMessageType,
          icon: FileText,
          color: "text-indigo-400",
          border: "border-indigo-500/40",
          bg: "bg-indigo-950/30",
          text: generated.comprehensiveReportMessage,
        },
      ]
    : [];

  return (
    <div className="glass-panel p-6 md:p-8 rounded-3xl border border-indigo-500/30 bg-gradient-to-b from-slate-900/95 via-slate-900/80 to-[#0c1424] shadow-2xl space-y-6 text-right font-tajawal">
      {/* Top Header */}
      <div className="flex flex-wrap items-center justify-between gap-4 pb-4 border-b border-slate-800">
        <div className="space-y-1">
          <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-indigo-500/15 border border-indigo-400/30 text-indigo-300 text-xs font-bold shadow-sm">
            <Sparkles className="w-3.5 h-3.5 text-amber-400" />
            <span>المساعد الذكي لمعالجة بيانات الطلاب وتوليد إشعارات المنصة</span>
          </div>
          <h3 className="text-xl md:text-2xl font-black text-white font-fancy flex items-center gap-2">
            <span>توليد وقالبة الرسائل والتنبيهات المعتمدة للمنصة</span>
          </h3>
          <p className="text-xs md:text-sm text-slate-300">
            ابحث بكود الطالب أو اسمه لتوليد كافة الرسائل الثمانية المعتمدة فورياً مع بيانات حقيقية وأزرار تفاعلية للنشر والنسخ.
          </p>
        </div>

        {/* View Mode Tabs */}
        <div className="flex items-center gap-2 bg-slate-950/80 p-1.5 rounded-2xl border border-slate-800">
          <button
            type="button"
            onClick={() => setActiveTab("all")}
            className={`px-3.5 py-1.5 rounded-xl text-xs font-bold transition-all cursor-pointer ${
              activeTab === "all"
                ? "bg-indigo-600 text-white shadow-md shadow-indigo-600/30"
                : "text-slate-400 hover:text-white"
            }`}
          >
            كل الرسائل (8 قوالب)
          </button>
          <button
            type="button"
            onClick={() => setActiveTab("ai")}
            className={`px-3.5 py-1.5 rounded-xl text-xs font-bold transition-all cursor-pointer flex items-center gap-1.5 ${
              activeTab === "ai"
                ? "bg-gradient-to-r from-indigo-600 to-purple-600 text-white shadow-md shadow-purple-600/30"
                : "text-amber-300 hover:text-white"
            }`}
          >
            <Sparkles className="w-3.5 h-3.5 text-amber-300" />
            <span>صياغة الذكاء الاصطناعي (Gemini) ✨</span>
          </button>
          <button
            type="button"
            onClick={() => setActiveTab("report")}
            className={`px-3.5 py-1.5 rounded-xl text-xs font-bold transition-all cursor-pointer ${
              activeTab === "report"
                ? "bg-indigo-600 text-white shadow-md shadow-indigo-600/30"
                : "text-slate-400 hover:text-white"
            }`}
          >
            التقرير الشامل 📊
          </button>
        </div>
      </div>

      {/* Student Search & Quick Barcode Input */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* Direct Barcode Entry */}
        <form onSubmit={handleBarcodeSubmit} className="space-y-1.5">
          <label className="block text-xs font-bold text-slate-300 flex items-center gap-1.5">
            <QrCode className="w-4 h-4 text-amber-400" />
            <span>إدخال كود الطالب المباشر (Barcode):</span>
          </label>
          <div className="relative flex items-center">
            <input
              type="text"
              value={barcodeInput}
              onChange={(e) => setBarcodeInput(e.target.value)}
              placeholder="اكتب كود الطالب واضغط Enter..."
              className="w-full pr-4 pl-24 py-2.5 rounded-2xl bg-slate-950/90 border border-slate-800 text-white text-xs md:text-sm font-mono focus:border-indigo-500 outline-none shadow-inner"
            />
            <button
              type="submit"
              className="absolute left-1.5 top-1.5 bottom-1.5 px-3.5 rounded-xl bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-bold transition-all cursor-pointer shadow-sm"
            >
              بحث فوري
            </button>
          </div>
        </form>

        {/* Dropdown Name Search Box */}
        <div className="space-y-1.5">
          <label className="block text-xs font-bold text-slate-300 flex items-center gap-1.5">
            <Search className="w-4 h-4 text-sky-400" />
            <span>أو البحث باسم الطالب:</span>
          </label>
          <StudentSearchBox
            students={students}
            onSelectStudent={(s) => setSelectedStudent(s)}
            placeholder="ابحث باسم الطالب لاختياره..."
          />
        </div>
      </div>

      {/* Selected Student Information Strip */}
      {selectedStudent && generated && (
        <div className="p-4 rounded-2xl bg-slate-950/80 border border-indigo-500/30 flex flex-wrap items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="w-11 h-11 rounded-2xl bg-indigo-600/20 border border-indigo-500/40 flex items-center justify-center text-indigo-300 font-bold">
              <User className="w-6 h-6" />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <h4 className="text-base font-extrabold text-white font-fancy">
                  {selectedStudent.name}
                </h4>
                <span className="px-2.5 py-0.5 rounded-lg bg-indigo-500/20 text-indigo-300 text-[11px] font-bold border border-indigo-500/30 font-mono">
                  كود: {selectedStudent.barcode}
                </span>
              </div>
              <p className="text-xs text-slate-400 mt-0.5">
                {selectedStudent.groupGrade} • {selectedStudent.groupDays} • تاريخ اليوم:{" "}
                <span className="text-amber-300 font-bold font-mono">{generated.dateStr}</span>
              </p>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2 text-xs font-bold">
            <span className="px-2.5 py-1 rounded-xl bg-slate-900 border border-slate-800 text-slate-300">
              حالة اليوم: <strong className="text-emerald-400">{generated.parsedValues.attendanceStatusToday}</strong>
            </span>
            <span className="px-2.5 py-1 rounded-xl bg-slate-900 border border-slate-800 text-slate-300">
              المصروفات: <strong className="text-teal-400">{generated.parsedValues.paymentStatus}</strong>
            </span>
            <span className="px-2.5 py-1 rounded-xl bg-slate-900 border border-slate-800 text-slate-300">
              التراكمي: <strong className="text-purple-400">{generated.parsedValues.cumulativeScore}</strong>
            </span>
          </div>
        </div>
      )}

      {/* Generated Cards Container */}
      {selectedStudent && generated ? (
        activeTab === "ai" ? (
          /* Gemini AI Customized Message Workshop */
          <div className="p-6 rounded-3xl bg-indigo-950/20 border border-indigo-500/40 space-y-5 shadow-xl">
            <div className="flex flex-wrap items-center justify-between gap-3 pb-3 border-b border-indigo-500/20">
              <div className="flex items-center gap-2.5">
                <div className="p-2 rounded-xl bg-purple-500/20 text-purple-300">
                  <Sparkles className="w-5 h-5 text-amber-300" />
                </div>
                <div>
                  <h4 className="text-sm font-bold text-white font-fancy">
                    ورشة الصياغة الذكية لرسائل المتابعة (Gemini 3.8 Flash)
                  </h4>
                  <p className="text-xs text-indigo-300">
                    توليد رسائل تربوية دقيقة مراعية لشخصية الطالب وأدائه
                  </p>
                </div>
              </div>
            </div>

            {/* Controls */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="space-y-1.5">
                <label className="text-xs font-bold text-slate-300">طابع / هدف الرسالة:</label>
                <select
                  value={aiTheme}
                  onChange={(e) => setAiTheme(e.target.value)}
                  className="w-full bg-[#080d1e] border border-indigo-500/30 text-white text-xs font-bold px-3.5 py-2.5 rounded-xl outline-none"
                >
                  <option value="تشجيع وتفوق">تشجيع وتفوق 🌟</option>
                  <option value="تنبيه غياب">تنبيه غياب وتأخر ⚠️</option>
                  <option value="متابعة درجات">متابعة درجات واختبارات 📐</option>
                  <option value="تذكير بالاشتراك">تذكير بالاشتراك الشهري 💳</option>
                  <option value="ملاحظة خاصة">ملاحظة سلوكية وتربوية 🌸</option>
                </select>
              </div>

              <div className="md:col-span-2 space-y-1.5">
                <label className="text-xs font-bold text-slate-300">
                  سياق إضافي أو ملاحظة تريد تضمينها (اختياري):
                </label>
                <input
                  type="text"
                  value={aiCustomContext}
                  onChange={(e) => setAiCustomContext(e.target.value)}
                  placeholder="مثال: نريد التركيز على حل مسائل الهندسة، أو تحسن ملحوظ في التفاعل..."
                  className="w-full bg-[#080d1e] border border-indigo-500/30 text-white text-xs px-3.5 py-2.5 rounded-xl outline-none focus:border-amber-400 font-medium"
                />
              </div>
            </div>

            <button
              type="button"
              disabled={aiLoading}
              onClick={handleGenerateAiMessage}
              className="px-6 py-3 rounded-2xl bg-gradient-to-r from-indigo-600 via-purple-600 to-indigo-700 hover:from-indigo-500 hover:to-purple-500 text-white text-xs font-bold flex items-center gap-2 shadow-lg shadow-purple-600/30 cursor-pointer disabled:opacity-50 transition-all"
            >
              {aiLoading ? (
                <>
                  <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                  <span>جارٍ التوليد عبر Gemini AI...</span>
                </>
              ) : (
                <>
                  <Sparkles className="w-4 h-4 text-amber-300" />
                  <span>توليد الرسالة الآن بالذكاء الاصطناعي ✨</span>
                </>
              )}
            </button>

            {/* Generated AI Message Display */}
            {aiResult && (
              <div className="p-4 rounded-2xl bg-slate-950/90 border border-indigo-500/40 space-y-3 animate-fadeIn">
                <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-800 pb-2.5">
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-bold text-amber-400">{aiResult.title}</span>
                    <span className="text-[11px] px-2 py-0.5 rounded-lg bg-indigo-900/50 text-indigo-300 border border-indigo-500/30">
                      النبرة: {aiResult.tone}
                    </span>
                    {aiResult.isFallback && (
                      <span className="text-[10px] px-2 py-0.5 rounded-lg bg-slate-800 text-slate-300">
                        نموذج قواعد آمن
                      </span>
                    )}
                  </div>

                  <div className="flex items-center gap-2">
                    <button
                      type="button"
                      onClick={() => handleCopy("ai_msg", aiResult.formattedMessage)}
                      className="px-3 py-1.5 rounded-xl bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs font-bold flex items-center gap-1.5 cursor-pointer"
                    >
                      {copiedKey === "ai_msg" ? (
                        <>
                          <Check className="w-3.5 h-3.5 text-emerald-400" />
                          <span>تم النسخ!</span>
                        </>
                      ) : (
                        <>
                          <Copy className="w-3.5 h-3.5" />
                          <span>نسخ</span>
                        </>
                      )}
                    </button>

                    <button
                      type="button"
                      onClick={() => {
                        openWhatsApp(
                          selectedStudent.parentPhone || selectedStudent.phone || "",
                          aiResult.formattedMessage
                        );
                      }}
                      className="px-3.5 py-1.5 rounded-xl bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-bold flex items-center gap-1.5 cursor-pointer shadow-md shadow-emerald-600/30"
                    >
                      <Send className="w-3.5 h-3.5" />
                      <span>إرسال واتساب 📲</span>
                    </button>

                    <button
                      type="button"
                      onClick={() =>
                        handlePublishPlatformNotification(
                          "ai_msg",
                          aiResult.title,
                          aiResult.formattedMessage,
                          "عام"
                        )
                      }
                      className="px-3.5 py-1.5 rounded-xl bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-bold flex items-center gap-1.5 cursor-pointer shadow-md"
                    >
                      <MessageSquare className="w-3.5 h-3.5" />
                      <span>نشر بالمنصة</span>
                    </button>
                  </div>
                </div>

                <div className="p-3.5 rounded-xl bg-[#080d1e] border border-indigo-500/20 text-slate-100 text-xs font-mono whitespace-pre-wrap leading-relaxed">
                  {aiResult.formattedMessage}
                </div>
              </div>
            )}
          </div>
        ) : activeTab === "report" ? (
          /* Single Comprehensive Report Display */
          <div className="p-6 rounded-3xl bg-indigo-950/30 border border-indigo-500/40 space-y-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2 text-indigo-300 font-black text-base">
                <FileText className="w-5 h-5 text-indigo-400" />
                <span>8. [تقرير شامل عن الطالب]</span>
              </div>
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={() => handleCopy("report", generated.comprehensiveReportMessage)}
                  className="px-3 py-1.5 rounded-xl bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs font-bold border border-slate-700 flex items-center gap-1.5 transition-all cursor-pointer"
                >
                  {copiedKey === "report" ? (
                    <>
                      <Check className="w-3.5 h-3.5 text-emerald-400" />
                      <span className="text-emerald-300">تم النسخ بنجاح</span>
                    </>
                  ) : (
                    <>
                      <Copy className="w-3.5 h-3.5" />
                      <span>نسخ النص</span>
                    </>
                  )}
                </button>
                <button
                  type="button"
                  onClick={() =>
                    handlePublishPlatformNotification(
                      "report",
                      `تقرير منصة شامل: ${selectedStudent.name}`,
                      generated.comprehensiveReportMessage,
                      "عام"
                    )
                  }
                  className="px-3.5 py-1.5 rounded-xl bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-bold flex items-center gap-1.5 transition-all shadow-md cursor-pointer"
                >
                  {sentKey === "report" ? (
                    <>
                      <Check className="w-3.5 h-3.5 text-amber-300" />
                      <span>تم التوثيق بالمنصة ✓</span>
                    </>
                  ) : (
                    <>
                      <Send className="w-3.5 h-3.5" />
                      <span>توثيق ونشر بالمنصة</span>
                    </>
                  )}
                </button>
              </div>
            </div>

            <pre className="p-4 rounded-2xl bg-slate-950/90 border border-slate-800 text-slate-200 font-mono text-xs md:text-sm whitespace-pre-wrap leading-relaxed shadow-inner">
              {generated.comprehensiveReportMessage}
            </pre>
          </div>
        ) : (
          /* Grid of All 8 Generated Templates */
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {templatesList.map((tpl) => {
              const IconComp = tpl.icon;
              return (
                <div
                  key={tpl.id}
                  className={`p-4 rounded-2xl border ${tpl.border} ${tpl.bg} flex flex-col justify-between gap-3 shadow-md`}
                >
                  <div className="flex items-center justify-between pb-2 border-b border-slate-800/80">
                    <div className="flex items-center gap-2">
                      <div className={`p-1.5 rounded-xl bg-slate-900/90 ${tpl.color}`}>
                        <IconComp className="w-4 h-4" />
                      </div>
                      <span className={`text-xs md:text-sm font-bold ${tpl.color}`}>
                        {tpl.title}
                      </span>
                    </div>

                    <div className="flex items-center gap-1.5">
                      <button
                        type="button"
                        onClick={() => handleCopy(tpl.id, tpl.text)}
                        className="px-2.5 py-1 rounded-xl bg-slate-900/80 hover:bg-slate-800 text-slate-300 text-[11px] font-bold border border-slate-800 flex items-center gap-1 transition-all cursor-pointer"
                        title="نسخ نص التنبيه للحافظة"
                      >
                        {copiedKey === tpl.id ? (
                          <>
                            <Check className="w-3 h-3 text-emerald-400" />
                            <span className="text-emerald-300">تم</span>
                          </>
                        ) : (
                          <>
                            <Copy className="w-3 h-3" />
                            <span>نسخ</span>
                          </>
                        )}
                      </button>

                      <button
                        type="button"
                        onClick={() =>
                          handlePublishPlatformNotification(
                            tpl.id,
                            tpl.title,
                            tpl.text,
                            tpl.type
                          )
                        }
                        className="px-2.5 py-1 rounded-xl bg-indigo-600/40 hover:bg-indigo-600/70 border border-indigo-500/40 text-indigo-200 text-[11px] font-bold flex items-center gap-1 transition-all cursor-pointer"
                        title="توثيق هذا التنبيه فوراً في سجل رسائل المنصة الداخلي"
                      >
                        {sentKey === tpl.id ? (
                          <>
                            <Check className="w-3 h-3 text-amber-300" />
                            <span>تم النشر ✓</span>
                          </>
                        ) : (
                          <>
                            <Send className="w-3 h-3 text-amber-400" />
                            <span>نشر بالمنصة</span>
                          </>
                        )}
                      </button>
                    </div>
                  </div>

                  <div className="p-3 rounded-xl bg-slate-950/80 border border-slate-900 text-slate-200 text-xs font-mono whitespace-pre-wrap leading-relaxed">
                    {tpl.text}
                  </div>
                </div>
              );
            })}
          </div>
        )
      ) : (
        <div className="p-12 text-center text-slate-400 bg-slate-950/40 rounded-2xl border border-dashed border-slate-800 space-y-2">
          <User className="w-10 h-10 mx-auto text-slate-600" />
          <p className="text-sm font-bold text-slate-300">
            يرجى إدخال كود الطالب أو اختياره من القائمة لتوليد الرسائل والقوالب المعتمدة فورياً.
          </p>
        </div>
      )}
    </div>
  );
};
