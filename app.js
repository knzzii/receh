"use strict";

/**
 * app.js
 * ------------------------------------------------------------
 * Telegram Worker (Express) - jembatan antara coinreceh.store/api.php
 * dan akun Telegram (MTProto/GramJS) yang berinteraksi dengan
 * @Kedai_cryptobot.
 *
 * Endpoint:
 *   GET  /            status service
 *   GET  /status       status telegram, queue, session
 *   POST /buy           terima order baru dari api.php
 *
 * Node.js TIDAK membaca/menulis database.json. Semua update
 * transaksi dilakukan lewat callback HTTP ke api.php.
 * ------------------------------------------------------------
 */

const fs = require("fs");
const path = require("path");

const envPath = path.join(process.cwd(), ".env");
const dotenvResult = require("dotenv").config({ path: envPath });

// --- Diagnostik .env (khusus troubleshooting deploy, aman dihapus nanti) ---
console.log("[debug] cwd:", process.cwd());
console.log("[debug] .env ada?:", fs.existsSync(envPath));
if (dotenvResult.error) {
  console.log("[debug] dotenv ERROR saat parse:", dotenvResult.error.message);
} else {
  console.log("[debug] dotenv berhasil parse, jumlah key:", Object.keys(dotenvResult.parsed || {}).length);
  console.log("[debug] key yang terbaca:", Object.keys(dotenvResult.parsed || {}));
}
console.log("[debug] TG_API_ID terbaca?:", !!process.env.TG_API_ID);
// cek BOM / karakter aneh di awal file
const rawBuf = fs.readFileSync(envPath);
console.log("[debug] 8 byte pertama file (hex):", rawBuf.slice(0, 8).toString("hex"));
// ---------------------------------------------------------------------

const crypto = require("crypto");
const express = require("express");
const rateLimit = require("express-rate-limit");
const axios = require("axios");

const { TelegramWorker } = require("./telegram");

// ==============================================================
// LOGGER SEDERHANA (tidak pernah log data sensitif)
// ==============================================================
const logger = {
  info: (...a) => console.log(new Date().toISOString(), "[INFO]", ...a),
  warn: (...a) => console.warn(new Date().toISOString(), "[WARN]", ...a),
  error: (...a) => console.error(new Date().toISOString(), "[ERROR]", ...a),
};

// ==============================================================
// KONFIGURASI DARI .env
// ==============================================================
const CFG = {
  port: parseInt(process.env.PORT || "3300", 10),
  apiId: parseInt(process.env.TG_API_ID, 10),
  apiHash: process.env.TG_API_HASH,
  botUsername: process.env.TG_BOT_USERNAME || "Kedai_cryptobot",

  // Teks tombol "Transaksi" di custom keyboard (menu) bot Kedai Crypto.
  // Ini BUKAN inline button, jadi tidak bisa di-klik - hanya bisa dikirim
  // sebagai teks pesan biasa persis seperti label tombolnya (termasuk emoji).
  menuButtonTransaksi: process.env.MENU_BUTTON_TRANSAKSI || "💎 Transaksi",

  apiKey: process.env.API_KEY,
  apiSecret: process.env.API_SECRET,
  timestampTolerance: parseInt(process.env.TIMESTAMP_TOLERANCE || "30", 10),

  callbackUrl: process.env.CALLBACK_URL,
  callbackApiKey: process.env.CALLBACK_API_KEY,
  callbackApiSecret: process.env.CALLBACK_API_SECRET,

  enableQuickTunnel: (process.env.ENABLE_QUICK_TUNNEL || "false").toLowerCase() === "true",

  coinWhitelist: (process.env.COIN_WHITELIST || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean),

  rateLimitWindowMs: parseInt(process.env.RATE_LIMIT_WINDOW_MS || "60000", 10),
  rateLimitMax: parseInt(process.env.RATE_LIMIT_MAX || "20", 10),

  timing: {
    stepDelay: parseInt(process.env.STEP_DELAY_MS || "2000", 10),
    confirmDelay: parseInt(process.env.CONFIRM_DELAY_MS || "6000", 10),
    waitTimeout: parseInt(process.env.WAIT_MESSAGE_TIMEOUT_MS || "15000", 10),
    strukTimeout: parseInt(process.env.STRUK_TIMEOUT_MS || "15000", 10),
    maxRetry: parseInt(process.env.MAX_RETRY || "3", 10),
  },
};

if (!CFG.apiId || !CFG.apiHash) {
  logger.error("TG_API_ID / TG_API_HASH belum diset di .env");
  process.exit(1);
}
if (!CFG.apiKey || !CFG.apiSecret) {
  logger.error("API_KEY / API_SECRET belum diset di .env");
  process.exit(1);
}

// ==============================================================
// PENYIMPANAN LOKAL WORKER (BUKAN database.json milik website)
// - processed_ids.json  -> idempotency, id order yang sudah diproses
// - pending_callbacks.json -> callback yang gagal dikirim, dikirim ulang saat service hidup
// ==============================================================
const DATA_DIR = __dirname;
const PROCESSED_IDS_FILE = path.join(DATA_DIR, "processed_ids.json");
const PENDING_CALLBACKS_FILE = path.join(DATA_DIR, "pending_callbacks.json");

function readJsonSafe(file, fallback) {
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    return fallback;
  }
}
function writeJsonSafe(file, data) {
  fs.writeFileSync(file, JSON.stringify(data, null, 2), "utf8");
}

let processedIds = new Set(readJsonSafe(PROCESSED_IDS_FILE, []));
function markProcessed(id) {
  processedIds.add(id);
  writeJsonSafe(PROCESSED_IDS_FILE, Array.from(processedIds));
}

// ==============================================================
// HASIL ORDER PER-ID (PENGGANTI PUSH CALLBACK)
// Worker TIDAK lagi push ke api.php. Tiap order selesai, hasilnya
// (sukses/gagal) disimpan di sini keyed by id transaksi. api.php yang
// PULL lewat GET /order-status/:id, jadi gak ada lagi "respons tidak
// dikenali" / retry / pending yang numpuk.
// ==============================================================
const ORDER_RESULTS_FILE = path.join(DATA_DIR, "order_results.json");
const ORDER_RESULTS_MAX = 800; // batasi biar file gak tumbuh tanpa henti

function saveOrderResult(id, data) {
  if (!id) return;
  const store = readJsonSafe(ORDER_RESULTS_FILE, {});
  store[id] = { ...data, savedAt: Date.now() };
  const ids = Object.keys(store);
  if (ids.length > ORDER_RESULTS_MAX) {
    ids.sort((a, b) => (store[a].savedAt || 0) - (store[b].savedAt || 0));
    for (const oldId of ids.slice(0, ids.length - ORDER_RESULTS_MAX)) delete store[oldId];
  }
  writeJsonSafe(ORDER_RESULTS_FILE, store);
  logger.info(`[result] Disimpan hasil id=${id} status=${data.status || "-"}`);
}
function getOrderResult(id) {
  const store = readJsonSafe(ORDER_RESULTS_FILE, {});
  return store[id] || null;
}

// Migrasi SEKALI: pindahkan sisa pending_callbacks.json (dari skema push
// lama) ke order_results.json supaya transaksi yang sempat nyangkut
// tetap bisa di-pull api.php by id. Setelah dipindah, file pending
// dikosongkan.
function migratePendingCallbacksToResults() {
  const pending = readJsonSafe(PENDING_CALLBACKS_FILE, []);
  if (!Array.isArray(pending) || !pending.length) return;
  let moved = 0;
  for (const item of pending) {
    const p = item && item.payload ? item.payload : item;
    if (p && p.id) { saveOrderResult(p.id, p); moved++; }
  }
  writeJsonSafe(PENDING_CALLBACKS_FILE, []);
  logger.info(`[result] Migrasi ${moved} pending callback lama -> order_results.json (pending dikosongkan)`);
}

// ==============================================================
// HMAC SIGNATURE HELPERS
// Skema: signature = HMAC_SHA256(secret, `${timestamp}.${rawBody}`)
// Header wajib: X-API-Key, X-Timestamp, X-Signature
// ==============================================================
function computeSignature(secret, timestamp, rawBody) {
  return crypto
    .createHmac("sha256", secret)
    .update(`${timestamp}.${rawBody}`)
    .digest("hex");
}

function safeEqual(a, b) {
  const bufA = Buffer.from(a || "", "utf8");
  const bufB = Buffer.from(b || "", "utf8");
  if (bufA.length !== bufB.length) return false;
  return crypto.timingSafeEqual(bufA, bufB);
}

// middleware verifikasi request masuk (dari api.php)
function verifyIncomingRequest(req, res, next) {
  const apiKey = req.header("X-API-Key");
  const timestamp = req.header("X-Timestamp");
  const signature = req.header("X-Signature");

  if (!apiKey || !timestamp || !signature) {
    return res.status(401).json({ success: false, error: "Header autentikasi tidak lengkap" });
  }

  if (!safeEqual(apiKey, CFG.apiKey)) {
    return res.status(401).json({ success: false, error: "API Key tidak valid" });
  }

  const now = Math.floor(Date.now() / 1000);
  const ts = parseInt(timestamp, 10);
  if (!ts || Math.abs(now - ts) > CFG.timestampTolerance) {
    return res.status(401).json({ success: false, error: "Timestamp kadaluarsa" });
  }

  const expectedSig = computeSignature(CFG.apiSecret, timestamp, req.rawBody || "");
  if (!safeEqual(signature, expectedSig)) {
    return res.status(401).json({ success: false, error: "Signature tidak valid" });
  }

  next();
}

// ==============================================================
// EXPRESS APP
// ==============================================================
const app = express();
// Worker diakses lewat Cloudflare Tunnel/reverse proxy -> percaya header
// X-Forwarded-For dari proxy terdekat. Tanpa ini, express-rate-limit
// nge-throw ValidationError dan bikin semua request /buy gagal.
app.set('trust proxy', 1);
app.use(
  express.json({
    verify: (req, res, buf) => {
      req.rawBody = buf.toString("utf8");
    },
  })
);

const buyLimiter = rateLimit({
  windowMs: CFG.rateLimitWindowMs,
  max: CFG.rateLimitMax,
  standardHeaders: true,
  legacyHeaders: false,
  message: { success: false, error: "Terlalu banyak request, coba lagi nanti" },
});

// ==============================================================
// QUEUE SEDERHANA - CONCURRENCY = 1
// ==============================================================
class SequentialQueue {
  constructor(worker) {
    this.items = []; // { order, resolve, reject }
    this.processing = false;
    this.worker = worker;
    this.currentId = null;
  }

  push(order) {
    return new Promise((resolve, reject) => {
      this.items.push({ order, resolve, reject });
      this._tick();
    });
  }

  size() {
    return this.items.length + (this.currentId ? 1 : 0);
  }

  async _tick() {
    if (this.processing) return;
    const next = this.items.shift();
    if (!next) return;

    this.processing = true;
    this.currentId = next.order.id;

    try {
      const result = await this.worker.processOrder(next.order);
      next.resolve(result);
    } catch (err) {
      next.reject(err);
    } finally {
      this.currentId = null;
      this.processing = false;
      // lanjut ke item berikutnya (kalau ada)
      setImmediate(() => this._tick());
    }
  }
}

// ==============================================================
// (DIHAPUS) PUSH CALLBACK KE WEBSITE
// Skema lama: worker push hasil ke api.php lewat HTTP + retry, dan kalau
// gagal disimpan ke pending_callbacks.json. Ini yang bikin "respons tidak
// dikenali" + retry numpuk. Sekarang diganti model PULL: hasil disimpan
// lewat saveOrderResult() dan api.php ambil sendiri via GET /order-status/:id.
// ==============================================================

// ==============================================================
// VALIDASI BODY /buy
// ==============================================================
function validateBuyBody(body) {
  if (!body || typeof body !== "object") return "Body tidak valid";
  const { id, coin, nominal, wallet } = body;

  if (!id || typeof id !== "string") return "id wajib diisi";
  if (processedIds.has(id)) return "DUPLICATE_ID";
  if (!coin || !CFG.coinWhitelist.includes(coin)) return "Coin tidak ada dalam whitelist";
  if (typeof nominal !== "number" || nominal <= 0) return "Nominal harus lebih dari 0";
  if (!wallet || typeof wallet !== "string" || !wallet.trim()) return "Wallet tidak boleh kosong";

  return null;
}

// ==============================================================
// QUICK TUNNEL (opsional, khusus testing) — spawn cloudflared
// sebagai proses anak dan tangkap URL https://xxxx.trycloudflare.com
// dari stdout/stderr-nya.
// ==============================================================
const { spawn } = require("child_process");

let quickTunnelUrl = null;

function startQuickTunnel(port) {
  logger.info("[tunnel] Menjalankan cloudflared quick tunnel...");
  const proc = spawn("npx", ["cloudflared", "tunnel", "--url", `http://localhost:${port}`], {
    shell: true,
  });

  const urlRegex = /https:\/\/[a-z0-9-]+\.trycloudflare\.com/i;

  const onData = (data) => {
    const text = data.toString();
    const match = text.match(urlRegex);
    if (match && !quickTunnelUrl) {
      quickTunnelUrl = match[0];
      logger.info("=".repeat(60));
      logger.info(`[tunnel] URL PUBLIK SIAP: ${quickTunnelUrl}`);
      logger.info(`[tunnel] Endpoint /buy   : ${quickTunnelUrl}/buy`);
      logger.info(`[tunnel] Set ini di TG_WORKER_URL pada api.php`);
      logger.info("=".repeat(60));
    }
  };

  proc.stdout.on("data", onData);
  proc.stderr.on("data", onData); // cloudflared nulis log-nya ke stderr

  proc.on("exit", (code) => {
    logger.warn(`[tunnel] cloudflared berhenti (code ${code}), mencoba restart dalam 5 detik...`);
    quickTunnelUrl = null;
    setTimeout(() => startQuickTunnel(port), 5000);
  });

  proc.on("error", (err) => {
    logger.error("[tunnel] Gagal menjalankan cloudflared:", err.message);
  });
}


// ==============================================================
// BOOTSTRAP
// ==============================================================
async function main() {
  const tgWorker = new TelegramWorker(CFG, logger);
  await tgWorker.init();

  const queue = new SequentialQueue(tgWorker);

  // migrasi sisa callback lama (skema push) ke order_results.json
  migratePendingCallbacksToResults();

  // --------------------------------------------------------
  // GET / - status service
  // --------------------------------------------------------
  app.get("/", (req, res) => {
    res.json({ success: true, service: "telegram-worker", status: "running" });
  });

  // --------------------------------------------------------
  // GET /health - dipanggil api.php (isTelegramWorkerAlive) SEBELUM
  // bikin QRIS. Harus balikin HTTP 200 kalau worker + sesi Telegram
  // siap. Kalau belum ready, tetap balikin HTTP 200 tapi dengan flag
  // ready:false, supaya api.php baca dari isi JSON-nya (bukan cuma
  // dari HTTP code).
  // --------------------------------------------------------
  app.get("/health", (req, res) => {
    const ready = tgWorker.isReady();
    res.status(200).json({
      success: true,
      ready,
      ok: ready,
      connected: ready,
    });
  });

  // --------------------------------------------------------
  // GET /status - status telegram, queue, session
  // --------------------------------------------------------
  app.get("/status", (req, res) => {
    res.json({
      success: true,
      telegram: {
        connected: tgWorker.isReady(),
        loggingIn: tgWorker.loggingIn,
      },
      queue: {
        waiting: queue.items.length,
        processing: queue.currentId,
        total: queue.size(),
      },
      session: {
        exists: fs.existsSync(path.join(__dirname, "session.txt")) &&
          fs.readFileSync(path.join(__dirname, "session.txt"), "utf8").trim().length > 0,
      },
      tunnel: {
        enabled: CFG.enableQuickTunnel,
        url: quickTunnelUrl,
      },
    });
  });

  // --------------------------------------------------------
  // GET /order-status/:id - api.php PULL hasil order by id transaksi
  // (pengganti push callback). Dilindungi HMAC yang sama dengan /buy.
  //   found=true  -> ada hasil final (completed/failed) di data
  //   202         -> masih diproses/antri, cek lagi nanti
  //   404         -> id gak dikenali worker
  // --------------------------------------------------------
  app.get("/order-status/:id", verifyIncomingRequest, (req, res) => {
    const id = String(req.params.id || "");
    const result = getOrderResult(id);
    if (result) {
      return res.json({ success: true, found: true, data: result });
    }
    const inQueue = queue.currentId === id || queue.items.some((o) => o.id === id);
    if (inQueue || processedIds.has(id)) {
      return res.status(202).json({ success: true, found: false, status: "processing" });
    }
    return res.status(404).json({ success: false, found: false, error: "Order tidak dikenali" });
  });

  // --------------------------------------------------------
  // POST /buy - terima order baru dari api.php
  // --------------------------------------------------------
  app.post("/buy", buyLimiter, verifyIncomingRequest, async (req, res) => {
    const body = req.body;
    const error = validateBuyBody(body);

    if (error === "DUPLICATE_ID") {
      return res.status(409).json({ success: false, error: "ID sudah pernah diproses" });
    }
    if (error) {
      return res.status(400).json({ success: false, error });
    }

    const order = {
      id: body.id,
      coin: body.coin,
      nominal: body.nominal,
      wallet: body.wallet,
    };

    // tandai sebagai "sudah diterima" segera untuk cegah duplikasi request beruntun
    markProcessed(order.id);

    // masukkan ke queue, balas segera bahwa order diterima (status: waiting/processing)
    res.json({ success: true, message: "Order diterima dan masuk antrian", id: order.id });

    // proses async, hasil DISIMPAN per-id (bukan di-push). api.php yang
    // PULL lewat GET /order-status/:id.
    queue
      .push(order)
      .then((result) => {
        if (result.success) {
          saveOrderResult(order.id, {
            id: order.id,
            status: "completed",
            coin: result.data.coin || order.coin,
            qty: result.data.qty || "",
            wallet: result.data.wallet || order.wallet,
            txid: result.data.txid || "",
            date: result.data.date || "",
            raw_message: result.data.raw_message || "",
          });
        } else {
          saveOrderResult(order.id, {
            id: order.id,
            status: "failed",
            reason: result.reason || "UNKNOWN_ERROR",
          });
        }
      })
      .catch((err) => {
        logger.error(`[queue] Error tak terduga untuk order ${order.id}: ${err.message}`);
        saveOrderResult(order.id, {
          id: order.id,
          status: "failed",
          reason: err.message || "QUEUE_ERROR",
        });
      });
  });

  app.listen(CFG.port, () => {
    logger.info(`[app] Telegram worker berjalan di port ${CFG.port}`);
    if (CFG.enableQuickTunnel) {
      startQuickTunnel(CFG.port);
    }
  });
}

main().catch((err) => {
  logger.error("[app] Gagal start:", err.message);
  process.exit(1);
});