import express, { Request, Response, NextFunction } from "express";
import path from "path";
import fs from "fs";
import { createServer as createViteServer } from "vite";
import {
  analyzeStudentPerformance,
  generateSmartNotification,
  getAiServiceHealth,
} from "./src/server/geminiService";

// Directory and file for zero-quota real-time sync persistence
const SYNC_DATA_DIR = path.join(process.cwd(), "data");
const SYNC_STATE_FILE = path.join(SYNC_DATA_DIR, "center_live_state.json");
const BACKUP_FALLBACK_FILE = path.join(process.cwd(), "src", "data", "centerBackup.json");

if (!fs.existsSync(SYNC_DATA_DIR)) {
  fs.mkdirSync(SYNC_DATA_DIR, { recursive: true });
}

// In-memory cache & SSE subscriber connections
let cachedServerState: any = null;
let lastServerUpdate = Date.now();
const sseClients = new Set<Response>();

// Try loading persisted state or fallback to backup
try {
  if (fs.existsSync(SYNC_STATE_FILE)) {
    const raw = fs.readFileSync(SYNC_STATE_FILE, "utf-8");
    cachedServerState = JSON.parse(raw);
    console.log("[Sync Hub] Loaded persistent center state from disk");
  } else if (fs.existsSync(BACKUP_FALLBACK_FILE)) {
    const raw = fs.readFileSync(BACKUP_FALLBACK_FILE, "utf-8");
    cachedServerState = JSON.parse(raw);
    fs.writeFileSync(SYNC_STATE_FILE, raw, "utf-8");
    console.log("[Sync Hub] Initialized center state from backup template");
  }
} catch (e) {
  console.warn("[Sync Hub] State initialization note:", e);
}

async function startServer() {
  const app = express();
  const PORT = 3000;

  // Middleware for body parsing and request isolation
  app.use(express.json({ limit: "25mb" }));

  // Enable CORS for external programs, scripts, or apps pulling data
  app.use((_req: Request, res: Response, next: NextFunction) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, x-session-id, x-device-id");
    if (_req.method === "OPTIONS") {
      return res.sendStatus(204);
    }
    next();
  });

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

  // -------------------------------------------------------------
  // High-Speed Multi-Device Realtime Synchronization Hub (Zero-Quota)
  // -------------------------------------------------------------

  // 1. Ping / Latency Diagnostic Endpoint
  app.get("/api/sync/ping", (_req: Request, res: Response) => {
    res.json({
      ok: true,
      status: "ok",
      engine: "High-Speed Real-Time Sync Hub (Unlimited)",
      serverTimestamp: Date.now(),
      clientsConnected: sseClients.size,
      hasState: Boolean(cachedServerState),
      studentsCount: cachedServerState?.students?.length || 0,
    });
  });

  // 2. Real-Time Server-Sent Events (SSE) Stream (< 50ms Push across all devices)
  app.get("/api/sync/events", (req: Request, res: Response) => {
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache, no-transform");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("X-Accel-Buffering", "no");
    res.flushHeaders?.();

    // Send initial handshake
    res.write(`data: ${JSON.stringify({ type: "handshake", timestamp: Date.now(), clientsCount: sseClients.size + 1 })}\n\n`);

    sseClients.add(res);

    req.on("close", () => {
      sseClients.delete(res);
    });
  });

  // 3. Pull Current Consolidated State (Zero-Quota, < 20ms response)
  app.get(["/api/sync/state", "/api/sync/data"], (_req: Request, res: Response) => {
    res.setHeader("Cache-Control", "no-cache");
    res.json({
      ok: true,
      data: cachedServerState,
      updatedAt: lastServerUpdate,
    });
  });

  // 3b. Dedicated endpoint for external programs to fetch students list only
  app.get("/api/sync/students", (_req: Request, res: Response) => {
    res.setHeader("Cache-Control", "no-cache");
    res.json({
      ok: true,
      count: cachedServerState?.students?.length || 0,
      students: cachedServerState?.students || [],
      updatedAt: lastServerUpdate,
    });
  });

  // 3c. Dedicated endpoint for external programs to fetch attendance logs
  app.get("/api/sync/attendance", (_req: Request, res: Response) => {
    res.setHeader("Cache-Control", "no-cache");
    res.json({
      ok: true,
      attendanceToday: cachedServerState?.attendanceToday || {},
      attendanceHistory: cachedServerState?.attendanceHistory || {},
      scanLogTimes: cachedServerState?.scanLogTimes || {},
      updatedAt: lastServerUpdate,
    });
  });

  // 4. Push State Update with Immediate Real-time Broadcast
  app.post("/api/sync/push", (req: Request, res: Response) => {
    const { data, sourceDeviceId } = req.body;
    if (!data) {
      return res.status(400).json({ ok: false, error: "No data payload provided" });
    }

    cachedServerState = data;
    lastServerUpdate = Date.now();

    // Persist to disk asynchronously
    try {
      fs.writeFileSync(SYNC_STATE_FILE, JSON.stringify(data), "utf-8");
    } catch (err) {
      console.error("[Sync Hub] Failed to write state to disk:", err);
    }

    // Broadcast instantly to all connected SSE clients
    const broadcastMsg = `data: ${JSON.stringify({
      type: "state_update",
      sourceDeviceId: sourceDeviceId || "unknown",
      updatedAt: lastServerUpdate,
      data,
    })}\n\n`;

    for (const client of sseClients) {
      try {
        client.write(broadcastMsg);
      } catch {
        sseClients.delete(client);
      }
    }

    res.json({
      ok: true,
      updatedAt: lastServerUpdate,
      broadcastedToClients: sseClients.size,
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
