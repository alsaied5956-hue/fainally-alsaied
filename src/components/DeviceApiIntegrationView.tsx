import React, { useState, useEffect } from "react";
import {
  Server,
  Zap,
  Radio,
  Copy,
  Check,
  RefreshCw,
  ArrowDownRight,
  ArrowUpRight,
  ShieldCheck,
  Smartphone,
  Cpu,
  Layers,
  Terminal,
  Trash2,
  Filter,
  CheckCircle2,
  AlertCircle,
  Clock,
  Send,
} from "lucide-react";
import {
  getPersistentDeviceId,
  setPersistentDeviceId,
  getPersistentDeviceName,
  getPersistentDeviceLocation,
  setDeviceIdentity,
  recordDeviceEntryExitScan,
  fetchDeviceLiveLogs,
  fetchAllEntryExitLogs,
  subscribeToDeviceLiveStream,
  updateDeviceConfigOnServer,
  clearDeviceLogsOnServer,
} from "../utils/deviceClient";
import { DeviceEntryExitEvent, EntryExitType, Student } from "../types";

interface DeviceApiIntegrationViewProps {
  students: Student[];
}

export const DeviceApiIntegrationView: React.FC<DeviceApiIntegrationViewProps> = ({ students }) => {
  const [deviceId, setDeviceId] = useState<string>(() => getPersistentDeviceId());
  const [deviceName, setDeviceName] = useState<string>(() => getPersistentDeviceName());
  const [deviceLocation, setDeviceLocation] = useState<string>(() => getPersistentDeviceLocation());
  const [isEditingIdentity, setIsEditingIdentity] = useState(false);
  const [copiedKey, setCopiedKey] = useState<string | null>(null);

  // View mode: device specific logs vs center-wide logs
  const [viewScope, setViewScope] = useState<"this_device" | "all_center">("this_device");
  const [typeFilter, setTypeFilter] = useState<"ALL" | "دخول" | "خروج">("ALL");
  const [searchQuery, setSearchQuery] = useState("");

  // Live Logs state
  const [logs, setLogs] = useState<DeviceEntryExitEvent[]>([]);
  const [isLoadingLogs, setIsLoadingLogs] = useState(false);
  const [isSseConnected, setIsSseConnected] = useState(true);
  const [lastRefreshedAt, setLastRefreshedAt] = useState<number>(Date.now());

  // Test scanner simulator
  const [testBarcode, setTestBarcode] = useState("");
  const [testType, setTestType] = useState<EntryExitType>("دخول");
  const [testNote, setTestNote] = useState("");
  const [testStatusMsg, setTestStatusMsg] = useState<{ text: string; ok: boolean } | null>(null);
  const [isScanning, setIsScanning] = useState(false);

  // Copy helper
  const handleCopy = (text: string, keyName: string) => {
    navigator.clipboard?.writeText(text);
    setCopiedKey(keyName);
    setTimeout(() => setCopiedKey(null), 2000);
  };

  // Load logs directly from server (Zero Cache)
  const refreshLogs = async () => {
    setIsLoadingLogs(true);
    try {
      if (viewScope === "this_device") {
        const res = await fetchDeviceLiveLogs(deviceId);
        if (res.ok) {
          setLogs(res.logs);
        }
      } else {
        const list = await fetchAllEntryExitLogs();
        setLogs(list);
      }
      setLastRefreshedAt(Date.now());
    } finally {
      setIsLoadingLogs(false);
    }
  };

  useEffect(() => {
    refreshLogs();
  }, [deviceId, viewScope]);

  // Subscribe to live SSE events for this device
  useEffect(() => {
    const unsubscribe = subscribeToDeviceLiveStream(
      deviceId,
      (newEvent) => {
        setLogs((prev) => {
          // Prevent duplicates
          if (prev.some((e) => e.id === newEvent.id)) return prev;
          return [newEvent, ...prev];
        });
      },
      () => {
        setIsSseConnected(false);
      }
    );

    return () => {
      unsubscribe();
    };
  }, [deviceId]);

  // Save device identity
  const handleSaveIdentity = async () => {
    setDeviceIdentity(deviceName, deviceLocation);
    if (deviceId) {
      setPersistentDeviceId(deviceId);
      await updateDeviceConfigOnServer(deviceId, { deviceName, deviceLocation });
    }
    setIsEditingIdentity(false);
    refreshLogs();
  };

  // Perform test scan
  const handleExecuteTestScan = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!testBarcode.trim()) return;

    setIsScanning(true);
    setTestStatusMsg(null);

    // Lookup matching student
    const matchedStudent = students.find((s) => String(s.barcode).trim() === testBarcode.trim());

    try {
      const res = await recordDeviceEntryExitScan({
        barcode: testBarcode.trim(),
        type: testType,
        studentName: matchedStudent?.name,
        grade: matchedStudent?.groupGrade,
        days: matchedStudent?.groupDays,
        notes: testNote.trim() || undefined,
        deviceId,
        deviceName,
        deviceLocation,
        syncToUnifiedAttendance: testType === "دخول",
      });

      if (res.ok && res.event) {
        setTestStatusMsg({
          text: `تم تسجيل حركة (${testType}) بنجاح للطالب: ${res.event.studentName} في تمام ${res.event.timeDisplay}`,
          ok: true,
        });
        setTestBarcode("");
        setTestNote("");
        // Prepend event locally if not already received via SSE
        setLogs((prev) => {
          if (prev.some((ev) => ev.id === res.event!.id)) return prev;
          return [res.event!, ...prev];
        });
      } else {
        setTestStatusMsg({
          text: res.error || "فشل تسجيل الحركة",
          ok: false,
        });
      }
    } catch (err: any) {
      setTestStatusMsg({
        text: err?.message || "خطأ غير متوقع",
        ok: false,
      });
    } finally {
      setIsScanning(false);
    }
  };

  // Clear device logs
  const handleClearLogs = async () => {
    if (!window.confirm("هل أنت متأكد من مسح سجل الحركات المحفوظ لهذا الجهاز؟")) return;
    await clearDeviceLogsOnServer(deviceId);
    setLogs([]);
  };

  // Filtered logs
  const filteredLogs = logs.filter((log) => {
    if (typeFilter !== "ALL" && log.type !== typeFilter) return false;
    if (searchQuery.trim()) {
      const q = searchQuery.toLowerCase().trim();
      const matchName = (log.studentName || "").toLowerCase().includes(q);
      const matchBarcode = (log.barcode || "").toLowerCase().includes(q);
      const matchDev = (log.deviceId || "").toLowerCase().includes(q);
      return matchName || matchBarcode || matchDev;
    }
    return true;
  });

  const entryCount = logs.filter((l) => l.type === "دخول").length;
  const exitCount = logs.filter((l) => l.type === "خروج").length;

  return (
    <div className="space-y-6 animate-fadeIn text-slate-100">
      {/* Top Banner: Device Identity & Status */}
      <div className="bg-slate-800/80 border border-slate-700/80 rounded-2xl p-5 shadow-lg relative overflow-hidden">
        <div className="flex flex-col lg:flex-row items-start lg:items-center justify-between gap-4">
          <div className="flex items-start gap-3.5">
            <div className="w-12 h-12 rounded-2xl bg-gradient-to-tr from-cyan-600 to-blue-600 flex items-center justify-center text-white shadow-md shadow-cyan-500/20 shrink-0">
              <Smartphone className="w-6 h-6" />
            </div>
            <div>
              <div className="flex items-center gap-2.5 flex-wrap">
                <h3 className="text-base font-black text-amber-300">
                  {deviceName} ({deviceLocation})
                </h3>
                <span className="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-[11px] font-bold bg-emerald-500/20 text-emerald-300 border border-emerald-500/30">
                  <Radio className="w-3 h-3 animate-pulse text-emerald-400" />
                  {isSseConnected ? "بث حي مباشر (Zero-Cache) 🟢" : "إعادة الاتصال بالبث 🟡"}
                </span>
                <span className="px-2 py-0.5 rounded-md text-[10px] font-mono bg-slate-900/90 text-cyan-300 border border-cyan-500/30">
                  الموقع المستقل
                </span>
              </div>
              <div className="flex items-center gap-2 mt-1 text-xs text-slate-300 flex-wrap">
                <span className="text-slate-400">معرف الجهاز الفريد (Device ID):</span>
                <code className="px-2 py-0.5 rounded bg-slate-950 text-amber-400 font-mono text-xs border border-slate-700">
                  {deviceId}
                </code>
                <button
                  type="button"
                  onClick={() => handleCopy(deviceId, "devId")}
                  className="p-1 hover:text-white text-slate-400 rounded hover:bg-slate-700/50 transition-colors"
                  title="نسخ معرف الجهاز"
                >
                  {copiedKey === "devId" ? (
                    <Check className="w-3.5 h-3.5 text-emerald-400" />
                  ) : (
                    <Copy className="w-3.5 h-3.5" />
                  )}
                </button>
              </div>
            </div>
          </div>

          {/* Identity edit button */}
          <div className="flex items-center gap-2 w-full lg:w-auto justify-end">
            {isEditingIdentity ? (
              <div className="flex items-center gap-2 flex-wrap">
                <button
                  onClick={handleSaveIdentity}
                  className="px-3.5 py-1.5 rounded-xl bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-bold transition-colors cursor-pointer"
                >
                  حفظ الهوية
                </button>
                <button
                  onClick={() => setIsEditingIdentity(false)}
                  className="px-3.5 py-1.5 rounded-xl bg-slate-700 hover:bg-slate-600 text-slate-300 text-xs font-bold transition-colors cursor-pointer"
                >
                  إلغاء
                </button>
              </div>
            ) : (
              <button
                onClick={() => setIsEditingIdentity(true)}
                className="px-3.5 py-1.5 rounded-xl bg-slate-700/80 hover:bg-slate-700 text-cyan-300 border border-cyan-500/30 text-xs font-bold transition-colors cursor-pointer flex items-center gap-1.5"
              >
                <Cpu className="w-3.5 h-3.5" />
                <span>تعديل اسم ومكان الجهاز</span>
              </button>
            )}
          </div>
        </div>

        {/* Identity Edit Form */}
        {isEditingIdentity && (
          <div className="mt-4 pt-4 border-t border-slate-700/70 grid grid-cols-1 md:grid-cols-3 gap-3 animate-fadeIn">
            <div>
              <label className="block text-[11px] font-bold text-slate-400 mb-1">
                معرف الجهاز (Device ID)
              </label>
              <input
                type="text"
                value={deviceId}
                onChange={(e) => setDeviceId(e.target.value)}
                className="w-full px-3 py-1.5 rounded-lg bg-slate-900 border border-slate-700 text-xs font-mono text-amber-300 focus:outline-none focus:border-cyan-500"
                placeholder="مثال: GATE_01_RECEPTION"
              />
            </div>
            <div>
              <label className="block text-[11px] font-bold text-slate-400 mb-1">
                اسم الجهاز (Device Name)
              </label>
              <input
                type="text"
                value={deviceName}
                onChange={(e) => setDeviceName(e.target.value)}
                className="w-full px-3 py-1.5 rounded-lg bg-slate-900 border border-slate-700 text-xs text-white focus:outline-none focus:border-cyan-500"
                placeholder="مثال: ماسح بوابة الاستقبال"
              />
            </div>
            <div>
              <label className="block text-[11px] font-bold text-slate-400 mb-1">
                الموقع / البوابة (Location)
              </label>
              <input
                type="text"
                value={deviceLocation}
                onChange={(e) => setDeviceLocation(e.target.value)}
                className="w-full px-3 py-1.5 rounded-lg bg-slate-900 border border-slate-700 text-xs text-white focus:outline-none focus:border-cyan-500"
                placeholder="مثال: البوابة الشرقية"
              />
            </div>
          </div>
        )}
      </div>

      {/* Dual Architecture Guide Cards (الموقع الموحد vs الموقع المستقل) */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* System 1: Unified Hub */}
        <div className="p-4 rounded-2xl bg-gradient-to-br from-indigo-950/40 via-slate-900 to-slate-900 border border-indigo-500/30 shadow-md">
          <div className="flex items-center gap-2 mb-2">
            <Layers className="w-4 h-4 text-indigo-400" />
            <h4 className="text-xs font-black text-indigo-300">
              1. النظام الأول (الموقع الموحد - Unified Center)
            </h4>
          </div>
          <p className="text-[11px] text-slate-300 leading-relaxed mb-3">
            يتصل بالـ API الموحد للمركز. تُعرض فيه كافة البيانات بالتساوي لجميع الشاشات والأجهزة، مع
            تحديث فوري وبث مباشر بدون تضارب.
          </p>
          <div className="space-y-1.5 text-[10px] font-mono">
            <div className="flex items-center justify-between p-1.5 rounded bg-slate-950/70 border border-slate-800">
              <span className="text-emerald-400">GET /api/sync/state</span>
              <button
                onClick={() => handleCopy("/api/sync/state", "c1")}
                className="text-slate-400 hover:text-white"
              >
                {copiedKey === "c1" ? <Check className="w-3 h-3 text-emerald-400" /> : <Copy className="w-3 h-3" />}
              </button>
            </div>
            <div className="flex items-center justify-between p-1.5 rounded bg-slate-950/70 border border-slate-800">
              <span className="text-cyan-400">GET /api/sync/events (SSE)</span>
              <button
                onClick={() => handleCopy("/api/sync/events", "c2")}
                className="text-slate-400 hover:text-white"
              >
                {copiedKey === "c2" ? <Check className="w-3 h-3 text-emerald-400" /> : <Copy className="w-3 h-3" />}
              </button>
            </div>
          </div>
        </div>

        {/* System 2: Independent Device Hub */}
        <div className="p-4 rounded-2xl bg-gradient-to-br from-cyan-950/40 via-slate-900 to-slate-900 border border-cyan-500/30 shadow-md">
          <div className="flex items-center gap-2 mb-2">
            <Terminal className="w-4 h-4 text-cyan-400" />
            <h4 className="text-xs font-black text-cyan-300">
              2. النظام الثاني (الموقع المستقل - Independent Device)
            </h4>
          </div>
          <p className="text-[11px] text-slate-300 leading-relaxed mb-3">
            يجلب بيانات أصلية وخاصة بهذا الجهاز فقط بناءً على معرف الجهاز{" "}
            <span className="text-amber-400 font-mono">Device ID</span> مع عزل كامل، وترويسة مانعة للـ
            Cash / Stale Data لضمان بيانات حية بنسبة 100%.
          </p>
          <div className="space-y-1.5 text-[10px] font-mono">
            <div className="flex items-center justify-between p-1.5 rounded bg-slate-950/70 border border-slate-800">
              <span className="text-amber-400">POST /api/devices/{deviceId}/scan</span>
              <button
                onClick={() => handleCopy(`/api/devices/${deviceId}/scan`, "c3")}
                className="text-slate-400 hover:text-white"
              >
                {copiedKey === "c3" ? <Check className="w-3 h-3 text-emerald-400" /> : <Copy className="w-3 h-3" />}
              </button>
            </div>
            <div className="flex items-center justify-between p-1.5 rounded bg-slate-950/70 border border-slate-800">
              <span className="text-emerald-400">GET /api/devices/{deviceId}/logs</span>
              <button
                onClick={() => handleCopy(`/api/devices/${deviceId}/logs`, "c4")}
                className="text-slate-400 hover:text-white"
              >
                {copiedKey === "c4" ? <Check className="w-3 h-3 text-emerald-400" /> : <Copy className="w-3 h-3" />}
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Test Scan Simulator (فحص حركة الدخول والخروج الحية) */}
      <div className="p-4 rounded-2xl bg-slate-800/90 border border-slate-700/80 shadow-md">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            <Zap className="w-4 h-4 text-amber-400" />
            <h4 className="text-xs font-black text-white">
              اختبار تسجيل حركة مباشرة (دخول / خروج) للجهاز الحالي
            </h4>
          </div>
          <span className="text-[11px] text-slate-400 font-mono">
            Direct Zero-Latency API Test
          </span>
        </div>

        <form onSubmit={handleExecuteTestScan} className="space-y-3">
          <div className="grid grid-cols-1 sm:grid-cols-12 gap-2.5">
            {/* Movement Type Toggle */}
            <div className="sm:col-span-3 flex items-center p-1 rounded-xl bg-slate-900 border border-slate-700">
              <button
                type="button"
                onClick={() => setTestType("دخول")}
                className={`flex-1 py-1.5 rounded-lg text-xs font-black transition-all flex items-center justify-center gap-1 cursor-pointer ${
                  testType === "دخول"
                    ? "bg-emerald-600 text-white shadow-md"
                    : "text-slate-400 hover:text-white"
                }`}
              >
                <ArrowDownRight className="w-3.5 h-3.5" />
                <span>دخول 🟢</span>
              </button>
              <button
                type="button"
                onClick={() => setTestType("خروج")}
                className={`flex-1 py-1.5 rounded-lg text-xs font-black transition-all flex items-center justify-center gap-1 cursor-pointer ${
                  testType === "خروج"
                    ? "bg-rose-600 text-white shadow-md"
                    : "text-slate-400 hover:text-white"
                }`}
              >
                <ArrowUpRight className="w-3.5 h-3.5" />
                <span>خروج 🔴</span>
              </button>
            </div>

            {/* Barcode input */}
            <div className="sm:col-span-4">
              <input
                type="text"
                value={testBarcode}
                onChange={(e) => setTestBarcode(e.target.value)}
                placeholder="أدخل باركود الطالب (مثال: 1001)..."
                className="w-full px-3 py-2 rounded-xl bg-slate-900 border border-slate-700 text-xs text-white placeholder-slate-500 focus:outline-none focus:border-cyan-500 font-mono"
              />
            </div>

            {/* Note input */}
            <div className="sm:col-span-3">
              <input
                type="text"
                value={testNote}
                onChange={(e) => setTestNote(e.target.value)}
                placeholder="ملاحظة اختيارية (مثل: إذن خروج)..."
                className="w-full px-3 py-2 rounded-xl bg-slate-900 border border-slate-700 text-xs text-white placeholder-slate-500 focus:outline-none focus:border-cyan-500"
              />
            </div>

            {/* Submit button */}
            <div className="sm:col-span-2">
              <button
                type="submit"
                disabled={isScanning || !testBarcode.trim()}
                className="w-full h-full min-h-[38px] px-3 py-2 rounded-xl bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500 text-white text-xs font-bold transition-all disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-1.5 cursor-pointer shadow-md"
              >
                {isScanning ? (
                  <RefreshCw className="w-3.5 h-3.5 animate-spin" />
                ) : (
                  <Send className="w-3.5 h-3.5" />
                )}
                <span>تسجيل الحركة</span>
              </button>
            </div>
          </div>

          {testStatusMsg && (
            <div
              className={`p-2.5 rounded-xl text-xs font-bold flex items-center gap-2 ${
                testStatusMsg.ok
                  ? "bg-emerald-500/20 text-emerald-300 border border-emerald-500/30"
                  : "bg-rose-500/20 text-rose-300 border border-rose-500/30"
              }`}
            >
              {testStatusMsg.ok ? (
                <CheckCircle2 className="w-4 h-4 text-emerald-400 shrink-0" />
              ) : (
                <AlertCircle className="w-4 h-4 text-rose-400 shrink-0" />
              )}
              <span>{testStatusMsg.text}</span>
            </div>
          )}
        </form>
      </div>

      {/* Live Logs Table: Authentic Live Events Directly from Source */}
      <div className="bg-slate-900/90 border border-slate-800 rounded-2xl p-4 shadow-lg">
        <div className="flex flex-col md:flex-row items-start md:items-center justify-between gap-3 mb-4 flex-wrap">
          {/* View Scope Tabs */}
          <div className="flex items-center gap-2 p-1 rounded-xl bg-slate-800 border border-slate-700">
            <button
              onClick={() => setViewScope("this_device")}
              className={`px-3 py-1.5 rounded-lg text-xs font-bold transition-all cursor-pointer ${
                viewScope === "this_device"
                  ? "bg-cyan-600 text-white shadow"
                  : "text-slate-400 hover:text-white"
              }`}
            >
              حركات هذا الجهاز فقط ({entryCount + exitCount})
            </button>
            <button
              onClick={() => setViewScope("all_center")}
              className={`px-3 py-1.5 rounded-lg text-xs font-bold transition-all cursor-pointer ${
                viewScope === "all_center"
                  ? "bg-indigo-600 text-white shadow"
                  : "text-slate-400 hover:text-white"
              }`}
            >
              جميع حركات المركز الموحد
            </button>
          </div>

          {/* Quick Filters & Controls */}
          <div className="flex items-center gap-2 flex-wrap w-full md:w-auto justify-end">
            {/* Filter buttons */}
            <div className="flex items-center gap-1 p-0.5 rounded-lg bg-slate-800 border border-slate-700">
              <button
                onClick={() => setTypeFilter("ALL")}
                className={`px-2 py-1 rounded text-[11px] font-bold cursor-pointer ${
                  typeFilter === "ALL" ? "bg-slate-700 text-white" : "text-slate-400"
                }`}
              >
                الكل
              </button>
              <button
                onClick={() => setTypeFilter("دخول")}
                className={`px-2 py-1 rounded text-[11px] font-bold cursor-pointer ${
                  typeFilter === "دخول" ? "bg-emerald-600 text-white" : "text-slate-400"
                }`}
              >
                دخول 🟢 ({entryCount})
              </button>
              <button
                onClick={() => setTypeFilter("خروج")}
                className={`px-2 py-1 rounded text-[11px] font-bold cursor-pointer ${
                  typeFilter === "خروج" ? "bg-rose-600 text-white" : "text-slate-400"
                }`}
              >
                خروج 🔴 ({exitCount})
              </button>
            </div>

            {/* Refresh button */}
            <button
              onClick={refreshLogs}
              disabled={isLoadingLogs}
              className="p-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-cyan-400 border border-slate-700 text-xs transition-colors cursor-pointer"
              title="تحديث فوري من المصدر (Anti-Cache)"
            >
              <RefreshCw className={`w-3.5 h-3.5 ${isLoadingLogs ? "animate-spin" : ""}`} />
            </button>

            {/* Clear logs button */}
            {viewScope === "this_device" && logs.length > 0 && (
              <button
                onClick={handleClearLogs}
                className="p-1.5 rounded-lg bg-rose-950/40 hover:bg-rose-900/60 text-rose-300 border border-rose-800/40 text-xs transition-colors cursor-pointer"
                title="مسح سجل هذا الجهاز"
              >
                <Trash2 className="w-3.5 h-3.5" />
              </button>
            )}
          </div>
        </div>

        {/* Search input */}
        <div className="mb-3">
          <input
            type="text"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            placeholder="بحث باسم الطالب أو الباركود أو اسم الجهاز..."
            className="w-full px-3 py-1.5 rounded-xl bg-slate-950 border border-slate-800 text-xs text-slate-200 placeholder-slate-500 focus:outline-none focus:border-cyan-500"
          />
        </div>

        {/* Table of Events */}
        {filteredLogs.length === 0 ? (
          <div className="text-center py-10 text-slate-400 text-xs border border-dashed border-slate-800 rounded-xl">
            <Clock className="w-8 h-8 text-slate-600 mx-auto mb-2" />
            <p>لا توجد حركات مسجلة حالياً وفقاً للفلاتر المحددة</p>
            <p className="text-[10px] text-slate-500 mt-1">
              قم بتمرير باركود تجريبي أو استخدام قارئ الباركود لاختبار التحديث الحي
            </p>
          </div>
        ) : (
          <div className="overflow-x-auto max-h-96 overflow-y-auto">
            <table className="w-full text-right border-collapse text-xs">
              <thead className="sticky top-0 bg-slate-800/95 text-slate-300 border-b border-slate-700">
                <tr>
                  <th className="py-2.5 px-3">نوع الحركة</th>
                  <th className="py-2.5 px-3">اسم الطالب</th>
                  <th className="py-2.5 px-3">الباركود</th>
                  <th className="py-2.5 px-3">الصف والمجموعة</th>
                  <th className="py-2.5 px-3">التوقيت الأصلي</th>
                  <th className="py-2.5 px-3">الجهاز والمكان</th>
                  <th className="py-2.5 px-3">ملاحظات</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800">
                {filteredLogs.map((log) => {
                  const isEntry = log.type === "دخول";
                  return (
                    <tr key={log.id} className="hover:bg-slate-800/40 transition-colors">
                      <td className="py-2 px-3 whitespace-nowrap">
                        <span
                          className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-[11px] font-bold ${
                            isEntry
                              ? "bg-emerald-500/20 text-emerald-300 border border-emerald-500/30"
                              : "bg-rose-500/20 text-rose-300 border border-rose-500/30"
                          }`}
                        >
                          {isEntry ? (
                            <ArrowDownRight className="w-3 h-3" />
                          ) : (
                            <ArrowUpRight className="w-3 h-3" />
                          )}
                          {log.type}
                        </span>
                      </td>
                      <td className="py-2 px-3 font-bold text-white whitespace-nowrap">
                        {log.studentName || "طالب غير مسجل"}
                      </td>
                      <td className="py-2 px-3 font-mono text-cyan-300 whitespace-nowrap">
                        {log.barcode}
                      </td>
                      <td className="py-2 px-3 text-slate-300 whitespace-nowrap text-[11px]">
                        {log.grade ? `${log.grade} (${log.days || "-"})` : "-"}
                      </td>
                      <td className="py-2 px-3 text-amber-300 font-mono text-[11px] whitespace-nowrap">
                        {log.timeDisplay}
                      </td>
                      <td className="py-2 px-3 text-slate-400 text-[11px] whitespace-nowrap">
                        <span className="text-slate-300">{log.deviceName || log.deviceId}</span>
                        {log.deviceLocation ? ` - ${log.deviceLocation}` : ""}
                      </td>
                      <td className="py-2 px-3 text-slate-400 text-[11px]">
                        {log.notes || "-"}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}

        <div className="mt-3 pt-3 border-t border-slate-800 flex items-center justify-between text-[11px] text-slate-400 flex-wrap">
          <span>إجمالي الحركات المعروضة: {filteredLogs.length}</span>
          <span className="font-mono">
            آخر تحديث حي: {new Date(lastRefreshedAt).toLocaleTimeString("ar-EG")}
          </span>
        </div>
      </div>
    </div>
  );
};
