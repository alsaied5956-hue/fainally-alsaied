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
const DEVICES_DATA_DIR = path.join(SYNC_DATA_DIR, "devices");
const DEVICE_REGISTRY_FILE = path.join(DEVICES_DATA_DIR, "device_registry.json");
const ENTRY_EXIT_LOGS_FILE = path.join(SYNC_DATA_DIR, "center_entry_exit_logs.json");

if (!fs.existsSync(SYNC_DATA_DIR)) {
  fs.mkdirSync(SYNC_DATA_DIR, { recursive: true });
}
if (!fs.existsSync(DEVICES_DATA_DIR)) {
  fs.mkdirSync(DEVICES_DATA_DIR, { recursive: true });
}

// In-memory cache & SSE subscriber connections
let cachedServerState: any = null;
let lastServerUpdate = Date.now();

// Device Registry & Device Scans In-Memory Buffers
const registeredDevices = new Map<string, any>();
const deviceEventsCache = new Map<string, any[]>();
let centerEntryExitLogs: any[] = [];

// Load initial Device Registry and Entry/Exit Logs
try {
  if (fs.existsSync(DEVICE_REGISTRY_FILE)) {
    const raw = fs.readFileSync(DEVICE_REGISTRY_FILE, "utf-8");
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      parsed.forEach((d) => registeredDevices.set(d.deviceId, d));
    }
  }
} catch (e) {
  console.warn("[Device Hub] Device registry initialization note:", e);
}

try {
  if (fs.existsSync(ENTRY_EXIT_LOGS_FILE)) {
    const raw = fs.readFileSync(ENTRY_EXIT_LOGS_FILE, "utf-8");
    centerEntryExitLogs = JSON.parse(raw);
  }
} catch (e) {
  console.warn("[Device Hub] Entry/Exit logs initialization note:", e);
}

// Helper to get device events
function getDeviceEvents(deviceId: string): any[] {
  if (deviceEventsCache.has(deviceId)) {
    return deviceEventsCache.get(deviceId)!;
  }
  const filePath = path.join(DEVICES_DATA_DIR, `device_${deviceId}_events.json`);
  if (fs.existsSync(filePath)) {
    try {
      const raw = fs.readFileSync(filePath, "utf-8");
      const list = JSON.parse(raw);
      deviceEventsCache.set(deviceId, list);
      return list;
    } catch {}
  }
  deviceEventsCache.set(deviceId, []);
  return [];
}

// Helper to save device events
function saveDeviceEvents(deviceId: string, events: any[]): void {
  deviceEventsCache.set(deviceId, events);
  try {
    const filePath = path.join(DEVICES_DATA_DIR, `device_${deviceId}_events.json`);
    fs.writeFileSync(filePath, JSON.stringify(events), "utf-8");
  } catch (err) {
    console.error(`[Device Hub] Failed to save device ${deviceId} events:`, err);
  }
}

// Helper to save device registry
function persistDeviceRegistry(): void {
  try {
    const list = Array.from(registeredDevices.values());
    fs.writeFileSync(DEVICE_REGISTRY_FILE, JSON.stringify(list), "utf-8");
  } catch (err) {
    console.error("[Device Hub] Failed to persist device registry:", err);
  }
}

// Helper to save entry/exit logs
function persistEntryExitLogs(): void {
  try {
    fs.writeFileSync(ENTRY_EXIT_LOGS_FILE, JSON.stringify(centerEntryExitLogs), "utf-8");
  } catch (err) {
    console.error("[Device Hub] Failed to persist entry/exit logs:", err);
  }
}

// SSE Subscriber Connection Tracking with Scope Filtering
interface SseConnection {
  id: string;
  res: Response;
  scope: "all" | "unified" | "device";
  deviceId?: string;
}
const sseSubscribers = new Map<string, SseConnection>();

// Dispatchers for isolated vs unified SSE events
function broadcastToUnified(payload: any): void {
  const msg = `data: ${JSON.stringify(payload)}\n\n`;
  for (const [id, conn] of sseSubscribers.entries()) {
    if (conn.scope === "all" || conn.scope === "unified") {
      try {
        conn.res.write(msg);
      } catch {
        sseSubscribers.delete(id);
      }
    }
  }
}

function broadcastToDevice(deviceId: string, payload: any): void {
  const msg = `data: ${JSON.stringify(payload)}\n\n`;
  for (const [id, conn] of sseSubscribers.entries()) {
    if (conn.scope === "all" || (conn.scope === "device" && conn.deviceId === deviceId)) {
      try {
        conn.res.write(msg);
      } catch {
        sseSubscribers.delete(id);
      }
    }
  }
}

function broadcastGlobal(payload: any): void {
  const msg = `data: ${JSON.stringify(payload)}\n\n`;
  for (const [id, conn] of sseSubscribers.entries()) {
    try {
      conn.res.write(msg);
    } catch {
      sseSubscribers.delete(id);
    }
  }
}

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

  // Strict Anti-Cache Headers for all API routes (Eliminates Stale/Cash issues completely)
  app.use("/api", (_req: Request, res: Response, next: NextFunction) => {
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0");
    res.setHeader("Pragma", "no-cache");
    res.setHeader("Expires", "0");
    res.setHeader("Surrogate-Control", "no-store");
    next();
  });

  // General server health check
  app.get("/api/health", (_req: Request, res: Response) => {
    res.json({
      status: "ok",
      uptime: process.uptime(),
      timestamp: new Date().toISOString(),
    });
  });

  // -------------------------------------------------------------
  // SYSTEM 1: Unified Center Synchronization Hub (الموقع الموحد)
  // -------------------------------------------------------------

  // 1. Ping / Diagnostic Endpoint
  app.get("/api/sync/ping", (_req: Request, res: Response) => {
    res.json({
      ok: true,
      status: "ok",
      engine: "Dual Architecture Hub (Unified Center + Independent Devices)",
      serverTimestamp: Date.now(),
      unifiedClientsConnected: Array.from(sseSubscribers.values()).filter((c) => c.scope === "unified" || c.scope === "all").length,
      deviceClientsConnected: Array.from(sseSubscribers.values()).filter((c) => c.scope === "device").length,
      totalSseConnections: sseSubscribers.size,
      registeredDevicesCount: registeredDevices.size,
      hasUnifiedState: Boolean(cachedServerState),
      studentsCount: cachedServerState?.students?.length || 0,
      totalEntryExitLogsCount: centerEntryExitLogs.length,
    });
  });

  // 2. Real-Time Unified Server-Sent Events (SSE) Stream
  app.get(["/api/sync/events", "/api/sync/unified/events"], (req: Request, res: Response) => {
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache, no-transform, no-store");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("X-Accel-Buffering", "no");
    res.flushHeaders?.();

    const connId = `unified_${Math.random().toString(36).substring(2, 9)}_${Date.now()}`;
    const conn: SseConnection = {
      id: connId,
      res,
      scope: "unified",
    };
    sseSubscribers.set(connId, conn);

    // Initial handshake
    res.write(`data: ${JSON.stringify({ type: "handshake", channel: "unified", timestamp: Date.now(), connId })}\n\n`);

    // Keep-alive heartbeat every 15 seconds to prevent browser/proxy disconnects
    const heartbeat = setInterval(() => {
      try {
        res.write(`: ping\n\n`);
      } catch {
        clearInterval(heartbeat);
        sseSubscribers.delete(connId);
      }
    }, 15000);

    req.on("close", () => {
      clearInterval(heartbeat);
      sseSubscribers.delete(connId);
    });
  });

  // 3. Pull Current Consolidated Unified State (Zero-Cache, Fresh from Source)
  app.get(["/api/sync/state", "/api/sync/data", "/api/sync/unified/state"], (_req: Request, res: Response) => {
    res.json({
      ok: true,
      data: cachedServerState,
      updatedAt: lastServerUpdate,
    });
  });

  // 3b. Dedicated endpoint to fetch students list only
  app.get("/api/sync/students", (_req: Request, res: Response) => {
    res.json({
      ok: true,
      count: cachedServerState?.students?.length || 0,
      students: cachedServerState?.students || [],
      updatedAt: lastServerUpdate,
    });
  });

  // 3c. Dedicated endpoint to fetch attendance logs
  app.get("/api/sync/attendance", (_req: Request, res: Response) => {
    res.json({
      ok: true,
      attendanceToday: cachedServerState?.attendanceToday || {},
      attendanceHistory: cachedServerState?.attendanceHistory || {},
      scanLogTimes: cachedServerState?.scanLogTimes || {},
      updatedAt: lastServerUpdate,
    });
  });

  // 4. Push Unified State Update with Immediate Real-time Broadcast
  app.post(["/api/sync/push", "/api/sync/unified/push"], (req: Request, res: Response) => {
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
      console.error("[Sync Hub] Failed to write unified state to disk:", err);
    }

    // Broadcast instantly to all unified screens
    broadcastToUnified({
      type: "state_update",
      sourceDeviceId: sourceDeviceId || "unknown",
      updatedAt: lastServerUpdate,
      data,
    });

    res.json({
      ok: true,
      updatedAt: lastServerUpdate,
      broadcastedToClients: sseSubscribers.size,
    });
  });

  // 5. Consolidated Entry & Exit Journal (All Devices Aggregated)
  app.get("/api/entry-exit/logs", (req: Request, res: Response) => {
    const { deviceId, type, dateKey, barcode, limit } = req.query;
    let filtered = [...centerEntryExitLogs];

    if (deviceId) {
      filtered = filtered.filter((e) => e.deviceId === String(deviceId).trim());
    }
    if (type) {
      filtered = filtered.filter((e) => e.type === String(type).trim());
    }
    if (dateKey) {
      filtered = filtered.filter((e) => e.dateKey === String(dateKey).trim());
    }
    if (barcode) {
      filtered = filtered.filter((e) => String(e.barcode).trim() === String(barcode).trim());
    }

    const maxItems = Math.min(Number(limit) || 100, 1000);
    res.json({
      ok: true,
      count: filtered.length,
      logs: filtered.slice(0, maxItems),
      updatedAt: Date.now(),
    });
  });

  // -------------------------------------------------------------
  // SYSTEM 2: Independent Device APIs (الموقع المستقل - أجهزة الدخول والخروج)
  // -------------------------------------------------------------

  // 1. Record Live Scan from an Independent Device (دخول أو خروج)
  app.post(["/api/devices/:deviceId/scan", "/api/entry-exit/scan"], (req: Request, res: Response) => {
    const paramDeviceId = req.params.deviceId;
    const body = req.body || {};
    const deviceId = String(paramDeviceId || body.deviceId || (req as any).deviceId || "standalone_device").trim();
    const barcode = String(body.barcode || "").trim();

    if (!barcode) {
      return res.status(400).json({ ok: false, error: "Missing barcode" });
    }

    // Normalize scan type: "دخول" أو "خروج"
    const rawType = String(body.type || "دخول").toLowerCase();
    const normalizedType: "دخول" | "خروج" =
      rawType === "exit" || rawType === "out" || rawType === "خروج" ? "خروج" : "دخول";

    // Lookup student in unified students database if not provided in payload
    let studentName = body.studentName ? String(body.studentName).trim() : "";
    let grade = body.grade ? String(body.grade).trim() : "";
    let days = body.days ? String(body.days).trim() : "";

    if (!studentName && cachedServerState?.students) {
      const found = cachedServerState.students.find(
        (s: any) => String(s.barcode).trim() === barcode
      );
      if (found) {
        studentName = found.name;
        grade = found.groupGrade || "";
        days = found.groupDays || "";
      }
    }

    const now = new Date();
    const timeDisplay = now.toLocaleTimeString("ar-EG", { hour: "2-digit", minute: "2-digit", hour12: true });
    const dateKey = now.toISOString().split("T")[0];

    // Device metadata
    const existingDevice = registeredDevices.get(deviceId);
    const deviceName = body.deviceName || existingDevice?.deviceName || `جهاز #${deviceId.substring(0, 6)}`;
    const deviceLocation = body.deviceLocation || existingDevice?.deviceLocation || "بوابة عامة";

    const scanEvent = {
      id: `scan_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`,
      deviceId,
      deviceName,
      deviceLocation,
      barcode,
      studentName: studentName || "طالب غير مسجل",
      grade,
      days,
      type: normalizedType,
      timestamp: Date.now(),
      timeIso: now.toISOString(),
      timeDisplay,
      dateKey,
      notes: body.notes || "",
      syncedToUnified: false,
    };

    // 1. Add to Device's Isolated Events Ledger
    const currentDeviceEvents = getDeviceEvents(deviceId);
    currentDeviceEvents.unshift(scanEvent);
    if (currentDeviceEvents.length > 1000) {
      currentDeviceEvents.length = 1000; // Cap at last 1,000 events per device
    }
    saveDeviceEvents(deviceId, currentDeviceEvents);

    // 2. Add to Center Aggregated Entry/Exit Logs
    centerEntryExitLogs.unshift(scanEvent);
    if (centerEntryExitLogs.length > 3000) {
      centerEntryExitLogs.length = 3000;
    }
    persistEntryExitLogs();

    // 3. Update Device Registry & Heartbeat
    const updatedDevice = {
      deviceId,
      deviceName,
      deviceLocation,
      lastActive: Date.now(),
      ip: req.ip || (req.headers["x-forwarded-for"] as string) || "127.0.0.1",
      status: "online",
      totalScansCount: (existingDevice?.totalScansCount || 0) + 1,
      lastScanType: normalizedType,
      lastScanStudentName: scanEvent.studentName,
      lastScanTime: timeDisplay,
    };
    registeredDevices.set(deviceId, updatedDevice);
    persistDeviceRegistry();

    // 4. Optional Safe Bridge to Unified Attendance (If Entry 'دخول')
    const syncToUnified = body.syncToUnifiedAttendance !== false;
    if (normalizedType === "دخول" && syncToUnified && cachedServerState) {
      if (!cachedServerState.attendanceToday) cachedServerState.attendanceToday = {};
      if (!cachedServerState.scanLogTimes) cachedServerState.scanLogTimes = {};
      if (!Array.isArray(cachedServerState.scanLogOrder)) cachedServerState.scanLogOrder = [];

      cachedServerState.attendanceToday[barcode] = "حضور";
      cachedServerState.scanLogTimes[barcode] = scanEvent.timeIso;

      if (!cachedServerState.scanLogOrder.includes(barcode)) {
        cachedServerState.scanLogOrder.unshift(barcode);
      }
      scanEvent.syncedToUnified = true;

      // Persist state asynchronously
      try {
        fs.writeFileSync(SYNC_STATE_FILE, JSON.stringify(cachedServerState), "utf-8");
      } catch (err) {
        console.error("[Device Hub] Bridge write note:", err);
      }

      // Notify unified monitors with a lightweight delta event (No heavy full reload needed!)
      broadcastToUnified({
        type: "device_entry_notification",
        deviceId,
        scanEvent,
        barcode,
        status: "حضور",
        timeIso: scanEvent.timeIso,
      });
    }

    // 5. Broadcast to the Specific Device's SSE Stream
    broadcastToDevice(deviceId, {
      type: "device_scan",
      deviceId,
      event: scanEvent,
    });

    res.json({
      ok: true,
      message: `تم تسجيل حركة (${normalizedType}) بنجاح للجهاز (${deviceId})`,
      event: scanEvent,
      device: updatedDevice,
    });
  });

  // 2. Fetch Strictly Device-Specific Original Live Logs (Zero-Cache Guarantee)
  app.get("/api/devices/:deviceId/logs", (req: Request, res: Response) => {
    const deviceId = String(req.params.deviceId).trim();
    const events = getDeviceEvents(deviceId);
    const device = registeredDevices.get(deviceId);

    res.json({
      ok: true,
      deviceId,
      deviceInfo: device || { deviceId, status: "unknown" },
      count: events.length,
      logs: events,
      updatedAt: Date.now(),
    });
  });

  // 3. Device-Specific Real-Time Server-Sent Events (SSE) Stream
  // Ensures ONLY events for THIS device arrive, preventing noise & lag!
  app.get("/api/devices/:deviceId/events", (req: Request, res: Response) => {
    const deviceId = String(req.params.deviceId).trim();

    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache, no-transform, no-store");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("X-Accel-Buffering", "no");
    res.flushHeaders?.();

    const connId = `dev_${deviceId}_${Math.random().toString(36).substring(2, 8)}_${Date.now()}`;
    const conn: SseConnection = {
      id: connId,
      res,
      scope: "device",
      deviceId,
    };
    sseSubscribers.set(connId, conn);

    // Initial handshake acknowledging connection to device-specific stream
    res.write(`data: ${JSON.stringify({
      type: "handshake",
      channel: "device_isolated",
      deviceId,
      timestamp: Date.now(),
      connId,
    })}\n\n`);

    // Keep-alive heartbeat every 15s
    const heartbeat = setInterval(() => {
      try {
        res.write(`: ping\n\n`);
      } catch {
        clearInterval(heartbeat);
        sseSubscribers.delete(connId);
      }
    }, 15000);

    req.on("close", () => {
      clearInterval(heartbeat);
      sseSubscribers.delete(connId);
    });
  });

  // 4. Device Registration / Configuration / Ping
  app.post("/api/devices/:deviceId/config", (req: Request, res: Response) => {
    const deviceId = String(req.params.deviceId).trim();
    const { deviceName, deviceLocation } = req.body || {};

    const existing = registeredDevices.get(deviceId) || {
      deviceId,
      totalScansCount: 0,
      status: "online",
    };

    const updated = {
      ...existing,
      deviceId,
      deviceName: deviceName ? String(deviceName).trim() : existing.deviceName || `جهاز #${deviceId.substring(0, 6)}`,
      deviceLocation: deviceLocation ? String(deviceLocation).trim() : existing.deviceLocation || "بوابة عامة",
      lastActive: Date.now(),
      status: "online",
    };

    registeredDevices.set(deviceId, updated);
    persistDeviceRegistry();

    res.json({
      ok: true,
      message: "تم تحديث بيانات الجهاز بنجاح",
      device: updated,
    });
  });

  // 5. Get Device Metadata & Status
  app.get("/api/devices/:deviceId/info", (req: Request, res: Response) => {
    const deviceId = String(req.params.deviceId).trim();
    const device = registeredDevices.get(deviceId);
    const events = getDeviceEvents(deviceId);

    res.json({
      ok: true,
      device: device || {
        deviceId,
        deviceName: `جهاز #${deviceId.substring(0, 6)}`,
        deviceLocation: "غير محدد",
        status: "offline",
        totalScansCount: events.length,
        lastActive: 0,
      },
      scansCount: events.length,
      updatedAt: Date.now(),
    });
  });

  // 6. List All Registered Devices in the Center
  app.get("/api/devices", (_req: Request, res: Response) => {
    const list = Array.from(registeredDevices.values()).map((d) => {
      const isOnline = Date.now() - (d.lastActive || 0) < 60000; // active in last 60s
      return {
        ...d,
        status: isOnline ? "online" : "idle",
      };
    });

    res.json({
      ok: true,
      count: list.length,
      devices: list,
      updatedAt: Date.now(),
    });
  });

  // 7. Clear Device Logs (For fresh device session)
  app.delete("/api/devices/:deviceId/logs", (req: Request, res: Response) => {
    const deviceId = String(req.params.deviceId).trim();
    saveDeviceEvents(deviceId, []);
    res.json({
      ok: true,
      message: `تم مسح سجل الجهاز (${deviceId}) بنجاح`,
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
