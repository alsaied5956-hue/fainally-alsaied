import React, { useState, useMemo } from "react";
import { Student, PaymentRecord, GRADE_ORDER } from "../types";
import { getAbsenceRate, getExamAverage, openWhatsApp, getCurrentMonthKey, isStudentPaid } from "../utils/helpers";
import { matchStudentSearch } from "../utils/search";
import {
  requestStudentDiagnosis,
  getAiHealthStatus,
  StudentDiagnosticResult,
  AiHealthStatus,
} from "../utils/aiClient";
import {
  AlertTriangle,
  AlertOctagon,
  ShieldAlert,
  Send,
  Search,
  X,
  Sparkles,
  CheckCircle,
  Activity,
  Lightbulb,
  CheckCheck,
  RotateCw,
  Copy,
  Info,
} from "lucide-react";

interface EarlyWarningTabProps {
  students?: Student[];
  payments?: Record<string, Record<string, PaymentRecord>>;
}

export const EarlyWarningTab: React.FC<EarlyWarningTabProps> = ({
  students = [],
  payments = {},
}) => {
  const [filterType, setFilterType] = useState<"ALL" | "ABSENCE" | "GRADES" | "PAYMENT">("ALL");
  const [filterGrade, setFilterGrade] = useState<string>("ALL");
  const [searchQuery, setSearchQuery] = useState("");

  // AI Diagnostic Modal State
  const [selectedStudentForAi, setSelectedStudentForAi] = useState<{
    student: Student;
    absRate: number;
    examAvg: number;
    isUnpaid: boolean;
  } | null>(null);
  const [aiLoading, setAiLoading] = useState(false);
  const [aiResult, setAiResult] = useState<StudentDiagnosticResult | null>(null);
  const [copiedMessage, setCopiedMessage] = useState(false);

  // AI Health Check Modal/Status
  const [healthModalOpen, setHealthModalOpen] = useState(false);
  const [healthStatus, setHealthStatus] = useState<AiHealthStatus | null>(null);
  const [healthLoading, setHealthLoading] = useState(false);

  const currentMonthKey = getCurrentMonthKey();

  // Evaluate students at risk
  const warningList = useMemo(() => {
    return students
      .map((student) => {
        const absRate = getAbsenceRate(student);
        const examAvg = getExamAverage(student);
        const isUnpaid = !isStudentPaid(payments?.[currentMonthKey], student.barcode);

        const reasons: string[] = [];
        let severity: "high" | "medium" | "low" = "low";

        // Absence Risk
        if (absRate >= 30 || student.totalAbsentDays >= 3) {
          reasons.push(`نسبة غياب مرتفعة جداً (${absRate}%) - غاب ${student.totalAbsentDays} حصص`);
          severity = "high";
        } else if (absRate >= 20 || student.totalAbsentDays >= 2) {
          reasons.push(`غياب متكرر (${absRate}%)`);
          severity = "medium";
        }

        // Exam Risk
        if (student.totalExamScores && student.totalExamScores.length > 0) {
          if (examAvg < 50) {
            reasons.push(`تراجع حاد في درجات الرياضيات (${examAvg}%)`);
            severity = "high";
          } else if (examAvg < 65) {
            reasons.push(`مستوى أكاديمي ضعيف (${examAvg}%)`);
            if (severity === "low") {
              severity = "medium";
            }
          }
        }

        // Unpaid
        if (isUnpaid) {
          reasons.push(`اشتراك شهر ${currentMonthKey} غير مدفوع حتى الآن`);
        }

        return {
          student,
          reasons,
          absRate,
          examAvg,
          isUnpaid,
          severity,
          hasRisk: reasons.length > 0,
        };
      })
      .filter((item) => {
        if (!item.hasRisk) return false;
        if (filterGrade !== "ALL" && item.student.groupGrade !== filterGrade) return false;
        if (filterType === "ABSENCE" && item.absRate < 20) return false;
        if (filterType === "GRADES" && item.examAvg >= 65) return false;
        if (filterType === "PAYMENT" && !item.isUnpaid) return false;
        if (searchQuery.trim()) {
          const { match } = matchStudentSearch(item.student, searchQuery);
          return match;
        }
        return true;
      });
  }, [students, payments, currentMonthKey, filterGrade, filterType, searchQuery]);

  const handleSendWarning = (student: Student, reasons: string[]) => {
    const reasonsText = reasons.map((r) => `• ${r}`).join("\n");
    const msg = `🚨 إنذار متابعة عاجل من منظومة الأستاذة إيمان الدمشيتي 📐\n\nنلفت عناية ولي أمر الطالب/ة: (${student.name})\nالمقيد في: ${student.groupGrade}\n\nنود إحاطتكم علماً بالملاحظات التالية:\n${reasonsText}\n\nنرجو التواصل الفوري والاهتمام لمصلحة الطالب ومستقبله التعليمي ✨`;
    openWhatsApp(student.parentPhone || student.phone || "", msg);
  };

  const handleOpenAiDiagnosis = async (item: {
    student: Student;
    absRate: number;
    examAvg: number;
    isUnpaid: boolean;
  }) => {
    setSelectedStudentForAi(item);
    setAiLoading(true);
    setAiResult(null);
    setCopiedMessage(false);

    try {
      const result = await requestStudentDiagnosis({
        student: item.student,
        attendanceRate: Math.max(0, 100 - item.absRate),
        examAvg: item.examAvg,
        isUnpaid: item.isUnpaid,
      });
      setAiResult(result);
    } catch (e) {
      console.warn("Error requesting diagnosis:", e);
    } finally {
      setAiLoading(false);
    }
  };

  const handleRefreshAiHealth = async () => {
    setHealthLoading(true);
    try {
      const stats = await getAiHealthStatus();
      setHealthStatus(stats);
    } finally {
      setHealthLoading(false);
    }
  };

  const highCount = warningList.filter((w) => w.severity === "high").length;
  const mediumCount = warningList.filter((w) => w.severity === "medium").length;

  return (
    <div className="space-y-6">
      {/* Alert Header Cards & Gemini AI Status Bar */}
      <div className="grid grid-cols-1 sm:grid-cols-4 gap-4">
        <div className="glass-panel border-rose-500/40 p-5 rounded-3xl flex items-center gap-3.5 shadow-xl">
          <div className="w-12 h-12 rounded-2xl bg-rose-500/10 border border-rose-500/30 text-rose-400 flex items-center justify-center shadow-md">
            <AlertOctagon className="w-6 h-6" />
          </div>
          <div>
            <p className="text-xs text-rose-300 font-bold font-tajawal">إنذارات عالية الخطورة 🔴</p>
            <p className="text-2xl font-black text-rose-400 font-mono">
              {highCount} <span className="text-xs font-tajawal font-normal text-slate-400">طالب</span>
            </p>
          </div>
        </div>

        <div className="glass-panel border-amber-500/40 p-5 rounded-3xl flex items-center gap-3.5 shadow-xl">
          <div className="w-12 h-12 rounded-2xl bg-amber-500/10 border border-amber-500/30 text-amber-400 flex items-center justify-center shadow-md">
            <AlertTriangle className="w-6 h-6" />
          </div>
          <div>
            <p className="text-xs text-amber-300 font-bold font-tajawal">إنذارات متوسطة 🟡</p>
            <p className="text-2xl font-black text-amber-300 font-mono">
              {mediumCount} <span className="text-xs font-tajawal font-normal text-slate-400">طالب</span>
            </p>
          </div>
        </div>

        <div className="glass-panel border-sky-500/40 p-5 rounded-3xl flex items-center gap-3.5 shadow-xl">
          <div className="w-12 h-12 rounded-2xl bg-sky-500/10 border border-sky-500/30 text-sky-400 flex items-center justify-center shadow-md">
            <ShieldAlert className="w-6 h-6" />
          </div>
          <div>
            <p className="text-xs text-sky-300 font-bold font-tajawal">إجمالي الحالات المتابعة</p>
            <p className="text-2xl font-black text-sky-400 font-mono">
              {warningList.length} <span className="text-xs font-tajawal font-normal text-slate-400">طالب</span>
            </p>
          </div>
        </div>

        <div className="glass-panel border-indigo-500/40 p-5 rounded-3xl flex items-center justify-between shadow-xl">
          <div className="flex items-center gap-3">
            <div className="w-12 h-12 rounded-2xl bg-indigo-500/10 border border-indigo-500/30 text-indigo-400 flex items-center justify-center shadow-md">
              <Sparkles className="w-6 h-6 animate-pulse" />
            </div>
            <div>
              <p className="text-xs text-indigo-300 font-bold font-tajawal">محرك الذكاء الاصطناعي</p>
              <p className="text-sm font-black text-white font-tajawal">Gemini 3.8 Flash</p>
            </div>
          </div>
          <button
            type="button"
            onClick={() => {
              setHealthModalOpen(true);
              handleRefreshAiHealth();
            }}
            className="p-2.5 rounded-xl bg-indigo-500/20 hover:bg-indigo-500/30 text-indigo-300 transition-all cursor-pointer"
            title="فحص معدل الطلبات والتزامن"
          >
            <Activity className="w-4 h-4" />
          </button>
        </div>
      </div>

      {/* Filter Bar */}
      <div className="glass-panel p-5 rounded-3xl flex flex-wrap items-center justify-between gap-3 shadow-2xl font-tajawal">
        <div className="flex flex-wrap items-center gap-3 flex-1 min-w-[300px]">
          <select
            value={filterType}
            onChange={(e) => setFilterType(e.target.value as "ALL" | "ABSENCE" | "GRADES" | "PAYMENT")}
            className="bg-[#080d1e] border border-indigo-500/30 text-slate-100 text-xs font-bold px-4 py-3 rounded-2xl outline-none"
          >
            <option value="ALL">جميع أنواع الإنذارات</option>
            <option value="ABSENCE">إنذارات الغياب المتكرر فقط 🔴</option>
            <option value="GRADES">إنذارات تراجع الدرجات فقط 📉</option>
            <option value="PAYMENT">المتأخرات المالية فقط 💳</option>
          </select>

          <select
            value={filterGrade}
            onChange={(e) => setFilterGrade(e.target.value)}
            className="bg-[#080d1e] border border-indigo-500/30 text-slate-100 text-xs font-bold px-4 py-3 rounded-2xl outline-none"
          >
            <option value="ALL">كل الصفوف الدراسية</option>
            {GRADE_ORDER.map((g) => (
              <option key={g} value={g} className="bg-slate-900 text-white">
                {g}
              </option>
            ))}
          </select>

          {/* Search box */}
          <div className="relative flex-1 min-w-[200px]">
            <Search className="w-4 h-4 text-amber-400/60 absolute right-3.5 top-3.5 pointer-events-none" />
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="بحث بالاسم أو الباركود..."
              className="w-full bg-[#080d1e] border border-indigo-500/30 text-slate-100 text-xs pr-10 pl-8 py-3 rounded-2xl outline-none focus:border-amber-400 transition-all font-medium"
            />
            {searchQuery && (
              <button
                type="button"
                onClick={() => setSearchQuery("")}
                className="absolute left-3 top-3 text-slate-400 hover:text-white cursor-pointer"
              >
                <X className="w-3.5 h-3.5" />
              </button>
            )}
          </div>
        </div>

        <p className="text-xs text-slate-400">
          تحليل فوري لمعدل الحضور والدرجات مع دعم التشخيص الذكي بالذكاء الاصطناعي
        </p>
      </div>

      {/* Warning List Cards */}
      <div className="space-y-3 font-tajawal">
        {warningList.length === 0 ? (
          <div className="glass-panel border-emerald-500/30 p-10 rounded-3xl text-center space-y-3">
            <CheckCircle className="w-14 h-14 text-emerald-400 mx-auto drop-shadow-md" />
            <h3 className="text-lg font-bold font-fancy text-emerald-300">
              {searchQuery
                ? `لا توجد إنذارات مطابقة لبحث "${searchQuery}"`
                : "رائع! لا يوجد طلاب في دائرة الخطر أو الإنذار حالياً"}
            </h3>
            <p className="text-xs text-slate-400 max-w-md mx-auto">
              جميع الطلاب يظهرون التزاماً ممتازاً بالحضور ومستوى درجات مستقر وحالة سداد منتظمة.
            </p>
          </div>
        ) : (
          warningList.map((item) => {
            const { student, reasons, severity, absRate, examAvg } = item;
            return (
              <div
                key={student.barcode}
                className={`p-5 rounded-3xl glass-card transition-all flex flex-wrap items-center justify-between gap-4 shadow-xl ${
                  severity === "high"
                    ? "border-rose-500/50 border-r-8 border-r-rose-500"
                    : "border-amber-500/40 border-r-8 border-r-amber-500"
                }`}
              >
                <div className="space-y-2 flex-1 min-w-[280px]">
                  <div className="flex items-center gap-2.5">
                    <h4 className="text-base font-bold font-fancy text-white">{student.name}</h4>
                    <span className="text-[11px] font-mono text-amber-300 bg-slate-900/80 px-2.5 py-1 rounded-xl border border-indigo-500/20">
                      #{student.barcode}
                    </span>
                    <span className="text-xs font-bold text-amber-300/90">
                      {student.groupGrade} ({student.groupDays})
                    </span>
                  </div>

                  <div className="space-y-1.5 pt-1">
                    {reasons.map((reason, i) => (
                      <div key={i} className="text-xs text-rose-300 font-medium flex items-center gap-2">
                        <span className="w-2 h-2 rounded-full bg-rose-400 shrink-0" />
                        <span>{reason}</span>
                      </div>
                    ))}
                  </div>

                  <div className="flex items-center gap-4 text-[11px] text-slate-400 pt-1">
                    <span>
                      نسبة الغياب: <strong className="text-rose-400 font-mono">{absRate}%</strong>
                    </span>
                    <span>
                      متوسط الدرجات: <strong className="text-amber-300 font-mono">{examAvg}%</strong>
                    </span>
                    <span>
                      ولي الأمر: <strong className="text-slate-200 font-mono">{student.parentPhone}</strong>
                    </span>
                  </div>
                </div>

                <div className="flex items-center gap-2.5 shrink-0 flex-wrap">
                  {/* Gemini AI Diagnostic Button */}
                  <button
                    type="button"
                    onClick={() => handleOpenAiDiagnosis(item)}
                    className="px-4 py-3 bg-gradient-to-r from-indigo-600 via-purple-600 to-indigo-700 hover:from-indigo-500 hover:to-purple-500 text-white font-bold text-xs rounded-2xl shadow-lg shadow-indigo-600/30 flex items-center gap-2 transition-all transform hover:scale-[1.02] cursor-pointer"
                  >
                    <Sparkles className="w-4 h-4 text-amber-300" />
                    <span>تشخيص وخطة Gemini AI ✦</span>
                  </button>

                  {/* Standard WhatsApp Warning */}
                  <button
                    type="button"
                    onClick={() => handleSendWarning(student, reasons)}
                    className="px-4 py-3 bg-gradient-to-r from-rose-600 to-rose-500 hover:from-rose-500 text-white font-black text-xs rounded-2xl shadow-lg shadow-rose-600/30 flex items-center gap-2 transition-all transform hover:scale-[1.02] cursor-pointer"
                  >
                    <Send className="w-4 h-4" />
                    <span>إرسال إنذار فوري 📲</span>
                  </button>
                </div>
              </div>
            );
          })
        )}
      </div>

      {/* -------------------------------------------------------- */}
      {/* GEMINI AI DIAGNOSTIC & INTERVENTION MODAL                */}
      {/* -------------------------------------------------------- */}
      {selectedStudentForAi && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/80 backdrop-blur-md animate-fadeIn font-tajawal">
          <div className="bg-[#090e24] border border-indigo-500/40 rounded-3xl max-w-2xl w-full p-6 space-y-5 shadow-2xl overflow-y-auto max-h-[90vh]">
            {/* Modal Header */}
            <div className="flex items-center justify-between border-b border-indigo-500/20 pb-4">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-2xl bg-indigo-500/20 border border-indigo-500/30 flex items-center justify-center text-indigo-400">
                  <Sparkles className="w-5 h-5 text-amber-300" />
                </div>
                <div>
                  <h3 className="text-lg font-bold text-white font-fancy">
                    التشخيص التربوي الذكي بالذكاء الاصطناعي
                  </h3>
                  <p className="text-xs text-indigo-300">
                    الطالب: {selectedStudentForAi.student.name} (#{selectedStudentForAi.student.barcode})
                  </p>
                </div>
              </div>
              <button
                type="button"
                onClick={() => setSelectedStudentForAi(null)}
                className="p-2 rounded-xl bg-slate-800 text-slate-400 hover:text-white cursor-pointer"
              >
                <X className="w-5 h-5" />
              </button>
            </div>

            {/* Modal Body */}
            {aiLoading ? (
              <div className="py-12 text-center space-y-4">
                <div className="w-12 h-12 border-4 border-indigo-500/30 border-t-indigo-500 rounded-full animate-spin mx-auto" />
                <p className="text-sm font-bold text-indigo-200">
                  جارٍ استدعاء نموذج Gemini 3.8 Flash لتحليل سجل الطالب وتوليد التوصيات...
                </p>
                <p className="text-xs text-slate-400">
                  يتم تطبيق سياسة التراجع الأسي (Exponential Backoff) وتأمين حماية معدل الطلبات آلياً.
                </p>
              </div>
            ) : aiResult ? (
              <div className="space-y-4 animate-fadeIn">
                {/* Status & Risk Banner */}
                <div className="flex flex-wrap items-center justify-between p-4 rounded-2xl bg-indigo-950/40 border border-indigo-500/30 gap-3">
                  <div className="flex items-center gap-3">
                    <span className="text-xs text-slate-300 font-bold">الحالة العامة:</span>
                    <span
                      className={`px-3 py-1 rounded-xl text-xs font-black ${
                        aiResult.overallStatus === "ممتاز" || aiResult.overallStatus === "جيد جداً"
                          ? "bg-emerald-500/20 text-emerald-300 border border-emerald-500/40"
                          : aiResult.overallStatus === "حرج"
                          ? "bg-rose-500/20 text-rose-300 border border-rose-500/40"
                          : "bg-amber-500/20 text-amber-300 border border-amber-500/40"
                      }`}
                    >
                      {aiResult.overallStatus}
                    </span>
                  </div>

                  <div className="flex items-center gap-2 text-xs">
                    <span className="text-slate-400">مؤشر الخطر:</span>
                    <span className="font-mono font-bold text-amber-400">{aiResult.riskScore}/100</span>
                    {aiResult.isFallback && (
                      <span className="px-2 py-0.5 rounded-lg bg-slate-800 text-[10px] text-slate-300 border border-slate-700">
                        نموذج قواعد آمن
                      </span>
                    )}
                  </div>
                </div>

                {/* Strengths & Concerns Grid */}
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                  <div className="p-4 rounded-2xl bg-emerald-950/20 border border-emerald-500/20 space-y-2">
                    <h5 className="text-xs font-bold text-emerald-300 flex items-center gap-2">
                      <CheckCheck className="w-4 h-4" />
                      <span>نقاط القوة الملحوظة</span>
                    </h5>
                    <ul className="text-xs text-slate-200 space-y-1 list-disc list-inside">
                      {aiResult.academicStrengths.map((s, idx) => (
                        <li key={idx}>{s}</li>
                      ))}
                    </ul>
                  </div>

                  <div className="p-4 rounded-2xl bg-rose-950/20 border border-rose-500/20 space-y-2">
                    <h5 className="text-xs font-bold text-rose-300 flex items-center gap-2">
                      <AlertTriangle className="w-4 h-4" />
                      <span>جوانب تحتاج اهتماماً فورياً</span>
                    </h5>
                    <ul className="text-xs text-slate-200 space-y-1 list-disc list-inside">
                      {aiResult.areasOfConcern.map((c, idx) => (
                        <li key={idx}>{c}</li>
                      ))}
                    </ul>
                  </div>
                </div>

                {/* Action Steps */}
                <div className="p-4 rounded-2xl bg-sky-950/20 border border-sky-500/20 space-y-2">
                  <h5 className="text-xs font-bold text-sky-300 flex items-center gap-2">
                    <Lightbulb className="w-4 h-4 text-amber-300" />
                    <span>خطة العمل المقترحة للطالب</span>
                  </h5>
                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 text-xs text-slate-200">
                    {aiResult.tailoredActionSteps.map((step, idx) => (
                      <div key={idx} className="flex items-start gap-2 bg-slate-900/60 p-2.5 rounded-xl border border-sky-500/10">
                        <span className="w-5 h-5 rounded-full bg-sky-500/20 text-sky-300 flex items-center justify-center font-mono text-[10px] shrink-0">
                          {idx + 1}
                        </span>
                        <span>{step}</span>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Parent WhatsApp Message */}
                <div className="p-4 rounded-2xl bg-[#080d1e] border border-indigo-500/30 space-y-3">
                  <div className="flex items-center justify-between">
                    <h5 className="text-xs font-bold text-amber-300 flex items-center gap-2">
                      <Send className="w-4 h-4" />
                      <span>صيغة الرسالة المهنية لولي الأمر</span>
                    </h5>
                    <button
                      type="button"
                      onClick={() => {
                        navigator.clipboard.writeText(aiResult.parentSummaryArabic);
                        setCopiedMessage(true);
                        setTimeout(() => setCopiedMessage(false), 2000);
                      }}
                      className="text-xs text-indigo-300 hover:text-white flex items-center gap-1.5 cursor-pointer"
                    >
                      {copiedMessage ? <CheckCircle className="w-3.5 h-3.5 text-emerald-400" /> : <Copy className="w-3.5 h-3.5" />}
                      <span>{copiedMessage ? "تم النسخ!" : "نسخ النص"}</span>
                    </button>
                  </div>
                  <p className="text-xs text-slate-200 leading-relaxed whitespace-pre-wrap bg-slate-950/60 p-3.5 rounded-xl border border-indigo-500/20">
                    {aiResult.parentSummaryArabic}
                  </p>
                </div>

                {/* Pedagogical Tip */}
                {aiResult.teacherPedagogicalTip && (
                  <div className="p-3.5 rounded-2xl bg-purple-950/20 border border-purple-500/20 flex items-start gap-2.5 text-xs text-purple-200">
                    <Info className="w-4 h-4 text-purple-400 shrink-0 mt-0.5" />
                    <span>
                      <strong>توجيه تربوي للمعلمة: </strong>
                      {aiResult.teacherPedagogicalTip}
                    </span>
                  </div>
                )}

                {/* Action Buttons */}
                <div className="flex items-center justify-between pt-2">
                  <button
                    type="button"
                    onClick={() => handleOpenAiDiagnosis(selectedStudentForAi)}
                    className="px-4 py-2.5 rounded-xl bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs font-bold flex items-center gap-2 cursor-pointer transition-all"
                  >
                    <RotateCw className="w-3.5 h-3.5" />
                    <span>إعادة التشخيص</span>
                  </button>

                  <button
                    type="button"
                    onClick={() => {
                      openWhatsApp(
                        selectedStudentForAi.student.parentPhone || selectedStudentForAi.student.phone || "",
                        aiResult.parentSummaryArabic
                      );
                    }}
                    className="px-5 py-3 rounded-2xl bg-gradient-to-r from-emerald-600 to-emerald-500 hover:from-emerald-500 text-white text-xs font-bold flex items-center gap-2 shadow-lg shadow-emerald-600/30 cursor-pointer transition-all"
                  >
                    <Send className="w-4 h-4" />
                    <span>إرسال التقرير لولي الأمر عبر واتساب 📲</span>
                  </button>
                </div>
              </div>
            ) : null}
          </div>
        </div>
      )}

      {/* -------------------------------------------------------- */}
      {/* GEMINI AI HEALTH & RATE-LIMIT METRICS MODAL              */}
      {/* -------------------------------------------------------- */}
      {healthModalOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/80 backdrop-blur-md animate-fadeIn font-tajawal">
          <div className="bg-[#090e24] border border-indigo-500/40 rounded-3xl max-w-md w-full p-6 space-y-5 shadow-2xl">
            <div className="flex items-center justify-between border-b border-indigo-500/20 pb-4">
              <div className="flex items-center gap-3">
                <Activity className="w-5 h-5 text-indigo-400" />
                <h3 className="text-base font-bold text-white font-fancy">
                  حالة اتصال ومعالجة Gemini API
                </h3>
              </div>
              <button
                type="button"
                onClick={() => setHealthModalOpen(false)}
                className="p-1.5 rounded-xl bg-slate-800 text-slate-400 hover:text-white cursor-pointer"
              >
                <X className="w-4 h-4" />
              </button>
            </div>

            {healthLoading ? (
              <div className="py-8 text-center space-y-3">
                <div className="w-8 h-8 border-3 border-indigo-500/30 border-t-indigo-500 rounded-full animate-spin mx-auto" />
                <p className="text-xs text-slate-300">جارٍ جلب إحصائيات معدل الاستهلاك والتزامن...</p>
              </div>
            ) : healthStatus ? (
              <div className="space-y-3 text-xs">
                <div className="p-3.5 rounded-2xl bg-slate-900 border border-indigo-500/20 space-y-2">
                  <div className="flex justify-between">
                    <span className="text-slate-400">النموذج النشط:</span>
                    <span className="text-white font-mono font-bold">{healthStatus.model}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-slate-400">حالة الخدمة:</span>
                    <span className="text-emerald-400 font-bold">{healthStatus.status}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-slate-400">طلبات متزامنة قيد التنفيذ:</span>
                    <span className="text-amber-400 font-mono font-bold">
                      {healthStatus.activeRequestsInFlight} / 3
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-slate-400">طلبات في قائمة الانتظار:</span>
                    <span className="text-sky-400 font-mono font-bold">{healthStatus.queuedRequests}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-slate-400">المتبقي من سقف الدقيقة (RPM):</span>
                    <span className="text-emerald-400 font-mono font-bold">
                      {healthStatus.remainingRpmQuota} طلب
                    </span>
                  </div>
                </div>

                <p className="text-[11px] text-slate-400 leading-relaxed">
                  يتم تنظيم وتوزيع كافة الطلبات عبر طابور المعالجة الآمن (Concurrency Semaphore) مع تطبيق التراجع الأسي (Exponential Backoff) لحماية النظام من تجاوز معدل الاستخدام (Rate Limit 429).
                </p>

                <button
                  type="button"
                  onClick={handleRefreshAiHealth}
                  className="w-full py-2.5 rounded-xl bg-indigo-600/30 hover:bg-indigo-600/50 text-indigo-200 font-bold flex items-center justify-center gap-2 cursor-pointer transition-all"
                >
                  <RotateCw className="w-3.5 h-3.5" />
                  <span>تحديث البيانات الحالية</span>
                </button>
              </div>
            ) : (
              <div className="text-center py-6 space-y-2 text-xs text-slate-400">
                <p>تعذر جلب إحصائيات الخدمة حالياً.</p>
                <button
                  type="button"
                  onClick={handleRefreshAiHealth}
                  className="px-4 py-2 rounded-xl bg-slate-800 text-slate-200 font-bold cursor-pointer"
                >
                  إعادة المحاولة
                </button>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};
