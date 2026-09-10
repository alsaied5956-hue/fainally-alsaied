import express, { Request, Response, NextFunction } from "express";
import path from "path";
import { createServer as createViteServer } from "vite";
import {
  analyzeStudentPerformance,
  generateSmartNotification,
  getAiServiceHealth,
} from "./src/server/geminiService";

async function startServer() {
  const app = express();
  const PORT = 3000;

  // Middleware for body parsing and request isolation
  app.use(express.json({ limit: "10mb" }));

  // Session & Request Tracking Middleware (Isolates multi-device calls)
  app.use((req: Request, res: Response, next: NextFunction) => {
    const requestId = `req_${Math.random().toString(36).substring(2, 9)}_${Date.now()}`;
    const sessionId = (req.headers["x-session-id"] as string) || "anonymous_session";
    const deviceId = (req.headers["x-device-id"] as string) || "anonymous_device";

    res.setHeader("X-Request-ID", requestId);
    res.setHeader("X-Session-ID", sessionId);
    res.setHeader("X-Device-ID", deviceId);

    (req as any).requestId = requestId;
    (req as any).sessionId = sessionId;
    (req as any).deviceId = deviceId;
    next();
  });

  // -------------------------------------------------------------
  // API Routes (Registered FIRST before Vite or static middlewares)
  // -------------------------------------------------------------

  // General server health check
  app.get("/api/health", (_req: Request, res: Response) => {
    res.json({
      status: "ok",
      uptime: process.uptime(),
      timestamp: new Date().toISOString(),
    });
  });

  // Gemini AI Service Health & Real-time Rate Limit Metrics
  app.get("/api/ai/health", (_req: Request, res: Response) => {
    try {
      const health = getAiServiceHealth();
      res.json(health);
    } catch (err: any) {
      res.status(500).json({ status: "error", message: err?.message || "Health check failed" });
    }
  });

  // Asynchronous & Isolated Student Diagnostic Evaluation
  app.post("/api/ai/analyze-student", async (req: Request, res: Response) => {
    const { student, attendanceRate, examAvg, isUnpaid, notes } = req.body;
    const deviceId = (req as any).deviceId;
    const sessionId = (req as any).sessionId;

    if (!student || !student.name || !student.barcode) {
      return res.status(400).json({
        success: false,
        error: "Missing required student profile data (name, barcode).",
      });
    }

    try {
      const result = await analyzeStudentPerformance({
        student,
        attendanceRate: typeof attendanceRate === "number" ? attendanceRate : 100,
        examAvg: typeof examAvg === "number" ? examAvg : 80,
        isUnpaid: Boolean(isUnpaid),
        notes: notes || "",
        deviceId,
        sessionId,
      });

      res.json({
        success: true,
        data: result,
      });
    } catch (error: any) {
      console.error(`[API /api/ai/analyze-student Error] ${error?.message}`);
      res.status(500).json({
        success: false,
        error: "Failed to generate student diagnosis",
        details: error?.message,
      });
    }
  });

  // Tailored Smart Notification Generator
  app.post("/api/ai/smart-notification", async (req: Request, res: Response) => {
    const { student, messageType, contextData } = req.body;
    const deviceId = (req as any).deviceId;
    const sessionId = (req as any).sessionId;

    if (!student || !student.name) {
      return res.status(400).json({
        success: false,
        error: "Missing required student profile data.",
      });
    }

    try {
      const result = await generateSmartNotification({
        student,
        messageType: messageType || "تشجيع",
        contextData: contextData || "",
        deviceId,
        sessionId,
      });

      res.json({
        success: true,
        data: result,
      });
    } catch (error: any) {
      console.error(`[API /api/ai/smart-notification Error] ${error?.message}`);
      res.status(500).json({
        success: false,
        error: "Failed to generate smart notification",
        details: error?.message,
      });
    }
  });

  // Global Error Handler for API routes
  app.use("/api", (err: any, _req: Request, res: Response, _next: NextFunction) => {
    console.error("[API Server Error]", err);
    res.status(err.status || 500).json({
      error: "Internal Server Error",
      message: err.message || "An unexpected error occurred",
    });
  });

  // -------------------------------------------------------------
  // Vite Middleware & SPA Static Asset Serving
  // -------------------------------------------------------------
  if (process.env.NODE_ENV !== "production") {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa",
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), "dist");
    app.use(express.static(distPath));
    app.get("*", (_req: Request, res: Response) => {
      res.sendFile(path.join(distPath, "index.html"));
    });
  }

  app.listen(PORT, "0.0.0.0", () => {
    console.log(`[Server] Production-ready full-stack server running on http://0.0.0.0:${PORT}`);
  });
}

startServer().catch((err) => {
  console.error("[Server Bootstrap Fatal Error]:", err);
  process.exit(1);
});
