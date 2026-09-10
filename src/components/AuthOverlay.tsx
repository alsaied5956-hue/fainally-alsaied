import React, { useState } from "react";
import { UserAccount } from "../types";
import { TEACHER_NAME } from "../utils/helpers";
import { ALL_PERMISSIONS } from "../utils/storage";
import { Lock, Shield, KeyRound, ArrowLeft, Sparkles, CheckCircle2, Laptop } from "lucide-react";

interface AuthOverlayProps {
  usersList: UserAccount[];
  onLoginSuccess: (user: UserAccount) => void;
}

export const AuthOverlay: React.FC<AuthOverlayProps> = ({
  usersList,
  onLoginSuccess,
}) => {
  const [selectedUsername, setSelectedUsername] = useState("admin");
  const [passwordInput, setPasswordInput] = useState("");
  const [errorMsg, setErrorMsg] = useState("");
  const [rememberDevice, setRememberDevice] = useState(true);

  // Guarantee single supervisor account with full administrative powers and pass 2468
  const defaultSupervisorAccount: UserAccount = {
    username: "admin",
    pass: "2468",
    role: "admin",
    permissions: [...ALL_PERMISSIONS],
  };

  const executeLogin = (user: UserAccount) => {
    if (typeof window !== "undefined") {
      try {
        localStorage.removeItem("aiman_user_logged_out");
        localStorage.setItem("aiman_current_user", JSON.stringify(user));
        sessionStorage.setItem("aiman_current_user", JSON.stringify(user));
      } catch (err) {
        console.warn("Could not persist session:", err);
      }
    }
    onLoginSuccess(user);
  };

  const handleDirectSupervisorLogin = () => {
    executeLogin(defaultSupervisorAccount);
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setErrorMsg("");

    const trimmedPass = passwordInput.trim();

    // 1. Universal Master Supervisor Password Check: 2468 ALWAYS grants immediate access
    if (trimmedPass === "2468") {
      executeLogin(defaultSupervisorAccount);
      return;
    }

    // 2. Lookup in usersList if another account was selected
    const targetUser = usersList.find((u) => u.username === selectedUsername);
    if (!targetUser) {
      if (selectedUsername === "admin") {
        if (trimmedPass === "2468") {
          executeLogin(defaultSupervisorAccount);
          return;
        }
      }
      setErrorMsg("❌ كلمة المرور غير صحيحة! كلمة مرور المشرف هي: 2468");
      return;
    }

    if (targetUser.pass !== trimmedPass && trimmedPass !== "2468") {
      setErrorMsg("❌ كلمة المرور غير صحيحة! كلمة المرور الثابتة هي: 2468");
      return;
    }

    executeLogin(targetUser);
  };

  return (
    <div className="fixed inset-0 z-50 bg-[#060a14] flex items-center justify-center p-4 overflow-hidden" dir="rtl">
      {/* Background Decorative Gradients */}
      <div className="absolute top-1/4 -right-24 w-96 h-96 bg-amber-500/10 rounded-full blur-3xl pointer-events-none animate-pulse" />
      <div className="absolute bottom-1/4 -left-24 w-96 h-96 bg-emerald-500/10 rounded-full blur-3xl pointer-events-none" />
      <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[600px] bg-sky-500/5 rounded-full blur-[120px] pointer-events-none" />

      <div className="max-w-md w-full glass-panel border-2 border-amber-500/30 p-8 md:p-10 rounded-3xl shadow-2xl relative z-10 space-y-6 animate-in fade-in zoom-in-95 duration-300">
        {/* Brand Showcase */}
        <div className="text-center space-y-3">
          <div className="relative inline-block">
            <div className="w-20 h-20 mx-auto rounded-3xl bg-gradient-to-tr from-amber-600 via-amber-400 to-yellow-200 flex items-center justify-center text-slate-950 font-black text-4xl shadow-2xl shadow-amber-500/30 border-2 border-amber-300/60 transform hover:scale-105 transition-transform">
              إ
            </div>
            <div className="absolute -bottom-1 -left-1 w-6 h-6 rounded-full bg-emerald-500 border-2 border-[#090e1a] flex items-center justify-center shadow-md">
              <Sparkles className="w-3.5 h-3.5 text-slate-950" />
            </div>
          </div>

          <div>
            <h1 className="text-2xl font-black text-white tracking-tight flex items-center justify-center gap-2">
              <span>منظومة</span>
              <span className="gold-gradient-text">{TEACHER_NAME}</span>
            </h1>
            <p className="text-xs text-slate-400 font-semibold mt-1">
              المنصة السحابية لإدارة طلاب الرياضيات والحضور الذكي
            </p>
          </div>
        </div>

        {/* Supervisor Direct Access Card */}
        <div className="bg-amber-500/10 border border-amber-400/30 rounded-2xl p-4 text-center space-y-2">
          <div className="flex items-center justify-center gap-2 text-amber-300 font-bold text-xs">
            <Shield className="w-4 h-4 text-amber-400" />
            <span>حساب المشرف العام المعتمد للمنصة</span>
          </div>
          <div className="text-[11px] text-slate-300">
            كلمة المرور الثابتة: <span className="font-mono font-black text-amber-400 text-sm tracking-wider bg-slate-900/80 px-2.5 py-0.5 rounded-lg border border-amber-400/40">2468</span>
          </div>
          <button
            type="button"
            onClick={handleDirectSupervisorLogin}
            className="w-full py-2.5 bg-gradient-to-r from-amber-500 to-yellow-400 hover:from-amber-400 hover:to-yellow-300 text-slate-950 font-black text-xs rounded-xl shadow-md transition-all flex items-center justify-center gap-2 cursor-pointer active:scale-95"
          >
            <CheckCircle2 className="w-4 h-4" />
            <span>دخول سريع وفوري كـمشرف (2468)</span>
          </button>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4 text-xs font-bold">
          {errorMsg && (
            <div className="p-3.5 bg-rose-500/15 border border-rose-500/40 text-rose-300 rounded-2xl text-center font-bold shadow-lg animate-in fade-in">
              {errorMsg}
            </div>
          )}

          <div className="space-y-1.5">
            <label className="text-slate-300 font-black flex items-center gap-1.5">
              <Shield className="w-3.5 h-3.5 text-amber-400" />
              <span>الحساب:</span>
            </label>
            <div className="relative">
              <select
                value={selectedUsername}
                onChange={(e) => setSelectedUsername(e.target.value)}
                className="w-full bg-[#070c17] border-2 border-amber-500/30 text-white px-4 py-3 rounded-2xl outline-none font-black text-xs cursor-pointer focus:border-amber-400 focus:ring-2 focus:ring-amber-400/20 transition-all shadow-inner"
              >
                <option value="admin" className="bg-slate-900 text-white">
                  admin (👑 المشرف العام - كامل الصلاحيات)
                </option>
                {usersList
                  .filter((u) => u.username !== "admin")
                  .map((u) => (
                    <option key={u.username} value={u.username} className="bg-slate-900 text-white">
                      {u.username} ({u.role === "admin" ? "👑 مشرف" : "👤 سكرتارية"})
                    </option>
                  ))}
              </select>
            </div>
          </div>

          <div className="space-y-1.5">
            <label className="text-slate-300 font-black flex items-center gap-1.5">
              <KeyRound className="w-3.5 h-3.5 text-amber-400" />
              <span>أدخل كلمة المرور:</span>
            </label>
            <div className="relative">
              <input
                type="password"
                required
                autoFocus
                value={passwordInput}
                onChange={(e) => setPasswordInput(e.target.value)}
                placeholder="2468"
                className="w-full bg-[#070c17] border-2 border-amber-500/30 focus:border-amber-400 text-white px-4 py-3 rounded-2xl outline-none font-mono text-base pr-11 shadow-inner focus:ring-2 focus:ring-amber-400/20 transition-all placeholder:text-slate-600 text-center tracking-widest"
              />
              <Lock className="w-4 h-4 text-amber-400/60 absolute right-4 top-3.5 pointer-events-none" />
            </div>
          </div>

          {/* Remember this device checkbox */}
          <label className="flex items-center gap-2 cursor-pointer text-slate-300 text-[11px] select-none pt-1">
            <input
              type="checkbox"
              checked={rememberDevice}
              onChange={(e) => setRememberDevice(e.target.checked)}
              className="rounded accent-amber-400 w-4 h-4 cursor-pointer"
            />
            <Laptop className="w-3.5 h-3.5 text-amber-400" />
            <span>البقاء متصلاً دائماً على هذا الجهاز (حفظ الجلسة تلقائياً)</span>
          </label>

          <button
            type="submit"
            className="w-full py-3.5 bg-gradient-to-r from-amber-500 via-amber-400 to-yellow-300 hover:from-amber-400 hover:to-yellow-200 text-slate-950 font-black text-sm rounded-2xl shadow-xl shadow-amber-500/25 transition-all flex items-center justify-center gap-2 transform hover:scale-[1.01] active:scale-95 cursor-pointer border border-amber-300/40 mt-2"
          >
            <span>دخول للمنظومة</span>
            <ArrowLeft className="w-4 h-4" />
          </button>
        </form>
      </div>
    </div>
  );
};
