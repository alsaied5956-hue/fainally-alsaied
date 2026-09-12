import React, { useEffect, useRef, useState } from "react";
import { X, Camera, RefreshCw, Zap, AlertCircle, CheckCircle2 } from "lucide-react";
import { normalizeBarcode } from "../utils/scannerUtils";

interface CameraScannerModalProps {
  isOpen: boolean;
  onClose: () => void;
  onScan: (barcode: string) => void;
}

export const CameraScannerModal: React.FC<CameraScannerModalProps> = ({
  isOpen,
  onClose,
  onScan,
}) => {
  const videoRef = useRef<HTMLVideoElement>(null);
  const streamRef = useRef<MediaStream | null>(null);
  const [cameraError, setCameraError] = useState<string | null>(null);
  const [facingMode, setFacingMode] = useState<"environment" | "user">("environment");
  const [torchOn, setTorchOn] = useState(false);
  const [hasTorch, setHasTorch] = useState(false);
  const [lastScannedCode, setLastScannedCode] = useState<string | null>(null);
  const scanCooldownRef = useRef(false);
  const animationFrameRef = useRef<number | null>(null);

  // Initialize Camera Stream
  useEffect(() => {
    if (!isOpen) {
      stopCamera();
      return;
    }

    let isMounted = true;

    async function startCamera() {
      setCameraError(null);
      stopCamera();

      try {
        if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
          throw new Error("الكاميرا غير مدعومة في هذا المتصفح");
        }

        const stream = await navigator.mediaDevices.getUserMedia({
          video: {
            facingMode,
            width: { ideal: 1280 },
            height: { ideal: 720 },
          },
          audio: false,
        });

        if (!isMounted) {
          stream.getTracks().forEach((t) => t.stop());
          return;
        }

        streamRef.current = stream;
        if (videoRef.current) {
          videoRef.current.srcObject = stream;
          await videoRef.current.play();
        }

        // Check torch capability
        const track = stream.getVideoTracks()[0];
        const capabilities = track.getCapabilities?.() as any;
        if (capabilities && capabilities.torch) {
          setHasTorch(true);
        } else {
          setHasTorch(false);
        }

        // Start scanning loop
        startScanningLoop();
      } catch (err: any) {
        console.warn("Camera start note:", err);
        if (isMounted) {
          if (err.name === "NotAllowedError" || err.name === "PermissionDeniedError") {
            setCameraError("تم رفض إذن الوصول للكاميرا. يرجى تفعيل إذن الكاميرا في إعدادات المتصفح.");
          } else {
            setCameraError("تعذر تشغيل الكاميرا: " + (err.message || "حدث خطأ غير معروف"));
          }
        }
      }
    }

    startCamera();

    return () => {
      isMounted = false;
      stopCamera();
    };
  }, [isOpen, facingMode]);

  const stopCamera = () => {
    if (animationFrameRef.current) {
      cancelAnimationFrame(animationFrameRef.current);
      animationFrameRef.current = null;
    }
    if (streamRef.current) {
      streamRef.current.getTracks().forEach((t) => t.stop());
      streamRef.current = null;
    }
    if (videoRef.current) {
      videoRef.current.srcObject = null;
    }
  };

  const toggleTorch = async () => {
    if (!streamRef.current) return;
    const track = streamRef.current.getVideoTracks()[0];
    try {
      const nextState = !torchOn;
      await (track as any).applyConstraints({
        advanced: [{ torch: nextState }],
      });
      setTorchOn(nextState);
    } catch (e) {
      console.warn("Torch error:", e);
    }
  };

  const flipCamera = () => {
    setFacingMode((prev) => (prev === "environment" ? "user" : "environment"));
  };

  // Continuous frame analysis
  const startScanningLoop = () => {
    let barcodeDetector: any = null;

    if (typeof window !== "undefined" && "BarcodeDetector" in window) {
      try {
        const BarcodeDetectorClass = (window as any).BarcodeDetector;
        barcodeDetector = new BarcodeDetectorClass({
          formats: [
            "qr_code",
            "code_128",
            "code_39",
            "ean_13",
            "ean_8",
            "upc_a",
            "upc_e",
          ],
        });
      } catch (e) {
        console.warn("BarcodeDetector init note:", e);
      }
    }

    const checkFrame = async () => {
      if (!videoRef.current || videoRef.current.readyState < 2) {
        animationFrameRef.current = requestAnimationFrame(checkFrame);
        return;
      }

      if (!scanCooldownRef.current && barcodeDetector) {
        try {
          const barcodes = await barcodeDetector.detect(videoRef.current);
          if (barcodes && barcodes.length > 0) {
            const rawVal = barcodes[0].rawValue;
            if (rawVal) {
              const cleanCode = normalizeBarcode(rawVal);
              if (cleanCode) {
                scanCooldownRef.current = true;
                setLastScannedCode(cleanCode);
                try {
                  navigator.vibrate?.([80, 40, 80]);
                } catch {}

                onScan(cleanCode);

                // 1.5s cooldown before reading next code to avoid double bursts
                setTimeout(() => {
                  scanCooldownRef.current = false;
                  setLastScannedCode(null);
                }, 1500);
              }
            }
          }
        } catch {}
      }

      animationFrameRef.current = requestAnimationFrame(checkFrame);
    };

    animationFrameRef.current = requestAnimationFrame(checkFrame);
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 bg-black/90 flex flex-col items-center justify-center p-4 backdrop-blur-md">
      <div className="relative w-full max-w-lg bg-slate-900 border border-amber-500/40 rounded-3xl overflow-hidden shadow-2xl flex flex-col">
        {/* Header */}
        <div className="px-5 py-4 bg-slate-800/80 border-b border-slate-700/60 flex items-center justify-between">
          <div className="flex items-center gap-2.5">
            <div className="p-2 rounded-xl bg-amber-500/20 text-amber-400">
              <Camera className="w-5 h-5" />
            </div>
            <div>
              <h3 className="text-sm font-extrabold text-white">مسح كروت الطلاب بالكاميرا</h3>
              <p className="text-[11px] text-slate-400">وجه الكاميرا نحو باركود أو QR كارت الطالب</p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-2 rounded-xl bg-slate-700 hover:bg-slate-600 text-slate-300 hover:text-white transition-colors cursor-pointer"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Viewfinder Viewport */}
        <div className="relative aspect-[4/3] w-full bg-black flex items-center justify-center overflow-hidden">
          {cameraError ? (
            <div className="p-6 text-center text-amber-300 flex flex-col items-center gap-3">
              <AlertCircle className="w-10 h-10 text-amber-400" />
              <p className="text-xs font-bold leading-relaxed">{cameraError}</p>
              <button
                onClick={() => setFacingMode((prev) => (prev === "environment" ? "user" : "environment"))}
                className="mt-2 px-4 py-2 bg-amber-500 hover:bg-amber-400 text-black font-black text-xs rounded-xl"
              >
                إعادة المحاولة
              </button>
            </div>
          ) : (
            <>
              <video
                ref={videoRef}
                playsInline
                muted
                className="absolute inset-0 w-full h-full object-cover"
              />

              {/* Reticle / Targeting Box */}
              <div className="absolute inset-0 pointer-events-none flex items-center justify-center p-8">
                <div className="relative w-64 h-64 border-2 border-amber-400/80 rounded-2xl shadow-[0_0_0_9999px_rgba(0,0,0,0.5)]">
                  {/* Glowing Corner Accents */}
                  <div className="absolute -top-1 -left-1 w-6 h-6 border-t-4 border-l-4 border-amber-400 rounded-tl-lg" />
                  <div className="absolute -top-1 -right-1 w-6 h-6 border-t-4 border-r-4 border-amber-400 rounded-tr-lg" />
                  <div className="absolute -bottom-1 -left-1 w-6 h-6 border-b-4 border-l-4 border-amber-400 rounded-bl-lg" />
                  <div className="absolute -bottom-1 -right-1 w-6 h-6 border-b-4 border-r-4 border-amber-400 rounded-br-lg" />

                  {/* Animated Laser Scanning Beam */}
                  <div className="absolute inset-x-0 h-1 bg-gradient-to-r from-transparent via-amber-400 to-transparent shadow-[0_0_12px_#f59e0b] animate-bounce" />
                </div>
              </div>

              {/* Success Scan Overlay */}
              {lastScannedCode && (
                <div className="absolute inset-0 bg-emerald-950/80 backdrop-blur-sm flex flex-col items-center justify-center gap-2 z-20 animate-fadeIn">
                  <CheckCircle2 className="w-14 h-14 text-emerald-400 animate-pulse" />
                  <span className="text-white font-black text-base">تم التعرف على الكود!</span>
                  <span className="text-emerald-300 font-mono text-sm bg-emerald-900/60 px-3 py-1 rounded-lg border border-emerald-500/40">
                    {lastScannedCode}
                  </span>
                </div>
              )}
            </>
          )}
        </div>

        {/* Bottom Camera Controls */}
        <div className="px-5 py-4 bg-slate-800/80 border-t border-slate-700/60 flex items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            {hasTorch && (
              <button
                type="button"
                onClick={toggleTorch}
                className={`px-3.5 py-2 rounded-xl text-xs font-bold flex items-center gap-1.5 transition-all cursor-pointer ${
                  torchOn
                    ? "bg-amber-500 text-black shadow-lg shadow-amber-500/30"
                    : "bg-slate-700 hover:bg-slate-600 text-slate-200"
                }`}
              >
                <Zap className="w-4 h-4" />
                <span>{torchOn ? "إطفاء الكشاف" : "تشغيل الكشاف"}</span>
              </button>
            )}

            <button
              type="button"
              onClick={flipCamera}
              className="px-3.5 py-2 rounded-xl bg-slate-700 hover:bg-slate-600 text-slate-200 text-xs font-bold flex items-center gap-1.5 transition-all cursor-pointer"
            >
              <RefreshCw className="w-4 h-4" />
              <span>تبديل الكاميرا</span>
            </button>
          </div>

          <button
            type="button"
            onClick={onClose}
            className="px-4 py-2 bg-slate-700 hover:bg-slate-600 text-slate-300 hover:text-white text-xs font-bold rounded-xl transition-colors cursor-pointer"
          >
            إغلاق الكاميرا
          </button>
        </div>
      </div>
    </div>
  );
};
