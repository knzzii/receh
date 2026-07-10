"use strict";

/**
 * telegram.js
 * ------------------------------------------------------------
 * Menangani koneksi MTProto (GramJS) ke akun Telegram user,
 * login/OTP/2FA, penyimpanan session, auto-reconnect, dan
 * seluruh flow otomasi transaksi ke @Kedai_cryptobot.
 *
 * TIDAK menyentuh database.json. Semua hasil dikembalikan
 * ke app.js untuk dikirim sebagai callback ke api.php.
 *
 * FLOW TOMBOL: urutan step (Transaksi -> Cryptocurrency -> pilih
 * coin -> nominal -> wallet -> Sesuai) adalah FLOW BAKU/DEFAULT
 * yang SELALU jadi fallback. Kalau AiNavigator diaktifkan (lihat
 * app.js), setiap step KLIK TOMBOL akan konsultasi ke AI dulu
 * untuk cek apakah tombol default masih benar / bot sudah ganti
 * UI. Kalau AI gagal, timeout, mati, atau ragu-ragu -> otomatis
 * pakai tombol default tanpa AI sama sekali. Nominal & wallet
 * TIDAK PERNAH ditentukan AI, murni dari data order.
 * ------------------------------------------------------------
 */

const fs = require("fs");
const path = require("path");
const { TelegramClient, Api } = require("telegram");
const { StringSession } = require("telegram/sessions");
const { NewMessage } = require("telegram/events");
const input = require("input");

const SESSION_FILE = path.join(__dirname, "session.txt");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

class TelegramWorker {
  /**
   * @param {object} config
   * @param {object} logger
   * @param {object|null} aiNavigator - instance AiNavigator (opsional).
   *   Kalau null/undefined, worker jalan 100% pakai flow baku (tanpa AI).
   */
  constructor(config, logger, aiNavigator) {
    this.cfg = config;
    this.log = logger || console;
    this.nav = aiNavigator || null;
    this.client = null;
    this.botEntity = null;
    this.connected = false;
    this.loggingIn = false;
    this._watchInterval = null;
  }

  // ==========================================================
  // SESSION HELPERS
  // ==========================================================
  _readSession() {
    try {
      return fs.readFileSync(SESSION_FILE, "utf8").trim();
    } catch {
      return "";
    }
  }

  _saveSession(str) {
    fs.writeFileSync(SESSION_FILE, str || "", "utf8");
  }

  _clearSession() {
    this._saveSession("");
  }

  // ==========================================================
  // INIT / LOGIN
  // Jika session.txt valid -> pakai langsung (tidak login ulang)
  // Jika kosong / revoked -> login interaktif (nomor, OTP, 2FA)
  // ==========================================================
  async init() {
    const existing = this._readSession();
    const session = new StringSession(existing);

    this.client = new TelegramClient(session, this.cfg.apiId, this.cfg.apiHash, {
      connectionRetries: 10,
      autoReconnect: true,
      retryDelay: 2000,
    });

    if (typeof this.client.setLogLevel === "function") {
      this.client.setLogLevel("none");
    }

    try {
      await this.client.connect();

      const me = await this.client.getMe();
      if (!me) throw new Error("SESSION_INVALID");

      this.connected = true;
      this.log.info("[telegram] Session valid, login sebagai:", me.username || me.id);
    } catch (err) {
      const msg = String(err && err.message ? err.message : err);
      if (
        msg.includes("AUTH_KEY_UNREGISTERED") ||
        msg.includes("SESSION_REVOKED") ||
        msg.includes("SESSION_INVALID") ||
        !existing
      ) {
        this.log.warn("[telegram] Session tidak valid / kosong, memulai login ulang...");
        this._clearSession();
        await this._interactiveLogin();
      } else {
        throw err;
      }
    }

    this.botEntity = await this.client.getEntity(this.cfg.botUsername);
    this._watchConnection();

    if (this.nav) {
      this.log.info("[telegram] AI navigator AKTIF (fallback ke flow baku jika AI gagal/timeout).");
    } else {
      this.log.info("[telegram] AI navigator TIDAK aktif, pakai flow baku sepenuhnya.");
    }

    return this;
  }

  async _interactiveLogin() {
    this.loggingIn = true;
    try {
      await this.client.start({
        phoneNumber: async () => await input.text("Masukkan nomor HP (format +62...): "),
        password: async () => await input.text("Masukkan password 2FA (kosongkan jika tidak ada): "),
        phoneCode: async () => await input.text("Masukkan kode OTP yang dikirim Telegram: "),
        onError: (err) => this.log.error("[telegram] Login error:", err.message),
      });

      const sessionString = this.client.session.save();
      this._saveSession(sessionString);
      this.connected = true;
      this.log.info("[telegram] Login berhasil, session tersimpan.");
      // catatan: JANGAN log OTP / password / session string di sini.
    } finally {
      this.loggingIn = false;
    }
  }

  // ==========================================================
  // AUTO RECONNECT
  // ==========================================================
  _watchConnection() {
    if (this._watchInterval) {
      clearInterval(this._watchInterval);
    }

    this._watchInterval = setInterval(async () => {
      try {
        if (!this.client.connected) {
          this.log.warn("[telegram] Terputus, mencoba reconnect...");
          await this.client.connect();
        }
        this.connected = !!this.client.connected;
      } catch (err) {
        this.connected = false;
        this.log.error("[telegram] Reconnect gagal:", err.message);
      }
    }, 15000);
  }

  // Panggil ini saat shutdown / sebelum re-init supaya interval lama bersih.
  stop() {
    if (this._watchInterval) {
      clearInterval(this._watchInterval);
      this._watchInterval = null;
    }
    if (this.client) {
      try {
        this.client.disconnect();
      } catch (err) {
        this.log.warn("[telegram] Gagal disconnect saat stop():", err.message);
      }
    }
    this.connected = false;
  }

  isReady() {
    return this.connected && this.client && this.client.connected;
  }

  // ==========================================================
  // MENUNGGU PESAN BARU DARI BOT (dengan timeout & filter optional)
  // addEventHandler dipasang SINKRON begitu Promise dibuat, makanya
  // aman dibungkus Promise.all bareng aksi kirim/klik di _sendAndWait().
  // ==========================================================
  _waitForMessage(timeoutMs, filterFn) {
    return new Promise((resolve, reject) => {
      const eventFilter = new NewMessage({ fromUsers: [this.botEntity] });

      const handler = (event) => {
        const msg = event.message;
        if (!filterFn || filterFn(msg)) {
          cleanup();
          resolve(msg);
        }
      };

      const timer = setTimeout(() => {
        cleanup();
        reject(new Error("TIMEOUT_WAITING_MESSAGE"));
      }, timeoutMs);

      const cleanup = () => {
        clearTimeout(timer);
        this.client.removeEventHandler(handler, eventFilter);
      };

      this.client.addEventHandler(handler, eventFilter);
    });
  }

  // Pasang listener BARENG dengan aksi kirim/klik (bukan sleep dulu),
  // supaya pesan balasan cepat dari bot tidak lewat begitu saja.
  async _sendAndWait(action, waitTimeout, filterFn) {
    const [, msg] = await Promise.all([
      Promise.resolve().then(action),
      this._waitForMessage(waitTimeout, filterFn),
    ]);
    return msg;
  }

  // ==========================================================
  // FLOOD WAIT HANDLING
  // GramJS melempar error dengan properti `.seconds` (FloodWaitError)
  // atau pesan mengandung "FLOOD_WAIT_<detik>" kalau akun kena rate
  // limit Telegram. Ini WAJIB ditangani secara eksplisit - nunggu
  // PERSIS sesuai durasi yang diminta Telegram, bukan retry buta,
  // karena mengabaikan/retry cepat bisa memperpanjang cooldown atau
  // meningkatkan risiko akun di-restrict.
  // ==========================================================
  _extractFloodWaitSeconds(err) {
    if (!err) return null;
    if (typeof err.seconds === "number") return err.seconds;
    const msg = String(err.message || err.errorMessage || err);
    const m = msg.match(/FLOOD_WAIT_(\d+)/i) || msg.match(/wait of (\d+) seconds/i);
    return m ? parseInt(m[1], 10) : null;
  }

  async _withFloodWaitRetry(fn, maxFloodRetries = 3) {
    let attempt = 0;
    for (;;) {
      try {
        return await fn();
      } catch (err) {
        const waitSec = this._extractFloodWaitSeconds(err);
        if (waitSec === null || attempt >= maxFloodRetries) throw err;
        attempt++;
        const waitMs = (waitSec + 1) * 1000; // +1 detik buffer
        this.log.warn(
          `[telegram] Kena FLOOD_WAIT ${waitSec}s (percobaan flood ${attempt}/${maxFloodRetries}), menunggu ${Math.round(waitMs / 1000)}s sebelum retry...`
        );
        await sleep(waitMs);
      }
    }
  }

  // Varian _sendAndWait yang AMAN dari flood-wait DAN tetap gak kena race
  // condition: listener pesan dipasang BARENGAN (Promise.all) sama aksi
  // kirim/klik, persis kayak _sendAndWait biasa - bedanya aksinya dibungkus
  // _withFloodWaitRetry, jadi kalau kena FLOOD_WAIT dia otomatis nunggu &
  // retry SAMBIL listener tetap nyala nunggu di background. Ini penting
  // karena beberapa aksi (misal bot yang HAPUS pesan lama lalu langsung
  // kirim pesan baru) responsnya bisa secepat itu sampai listener yang
  // dipasang SETELAH aksi selesai (versi sekuensial sebelumnya) malah
  // ketinggalan pesannya.
  async _sendAndWaitSafe(action, waitTimeout, filterFn) {
    const [, msg] = await Promise.all([
      this._withFloodWaitRetry(action),
      this._waitForMessage(waitTimeout, filterFn),
    ]);
    return msg;
  }

  // Filter buat _waitForMessage: cuma anggap "match" kalau pesannya PUNYA
  // tombol. Dipakai di step yang abis itu bakal langsung diklik tombolnya,
  // supaya pesan selingan tanpa tombol (peringatan, iklan, notifikasi lain)
  // otomatis dilewatin sampai ketemu pesan yang beneran punya tombol.
  _filterHasButtons = (m) => Array.isArray(m.buttons) && m.buttons.length > 0;

  // ==========================================================
  // AMBIL DAFTAR TEKS TOMBOL DARI SEBUAH PESAN
  // ==========================================================
  _getButtonTexts(message) {
    if (!message || !message.buttons) return [];
    const texts = [];
    for (const row of message.buttons) {
      for (const btn of row) texts.push((btn.text || "").trim());
    }
    return texts;
  }

  // Strip simbol/emoji di depan teks tombol, contoh "💎 Transaksi" -> "Transaksi"
  _normalizeButtonText(text) {
    return (text || "").replace(/^[^\p{L}\p{N}]+/u, "").trim();
  }

  // ==========================================================
  // KLIK TOMBOL (inline callback ATAU reply keyboard) BERDASARKAN TEKS
  //
  // Pencocokan: exact match teks mentah ATAU exact match versi
  // ternormalisasi (strip emoji/simbol di depan). Ini menghindari
  // dua masalah sekaligus:
  //  - salah klik gara-gara substring match polos, misal target
  //    "Sesuai" ke-klik ke tombol "Tidak Sesuai"
  //  - gagal cocok gara-gara tombolnya berlabel emoji, misal
  //    target "Transaksi" tidak ketemu di tombol "💎 Transaksi"
  // ==========================================================
  async _clickButton(message, matchText) {
    if (!message || !message.buttons) {
      this.log.warn(`[flow] Pesan tidak punya buttons sama sekali. Isi pesan: "${(message && message.message) || ""}"`);
      throw new Error(`NO_BUTTONS_FOUND (expected: ${matchText})`);
    }

    const isKeyboardButton = message.replyMarkup
      && message.replyMarkup.className === "ReplyKeyboardMarkup";

    const targetNorm = this._normalizeButtonText(matchText);
    const allTexts = [];
    const candidates = [];

    for (const row of message.buttons) {
      for (const btn of row) {
        const text = (btn.text || "").trim();
        allTexts.push(text);
        const norm = this._normalizeButtonText(text);
        if (text === matchText || norm === targetNorm) {
          candidates.push({ text, btn });
        }
      }
    }

    if (candidates.length === 0) {
      this.log.warn(`[flow] Tombol "${matchText}" TIDAK ketemu. Tombol yang tersedia: ${JSON.stringify(allTexts)}`);
      throw new Error(`BUTTON_NOT_FOUND: ${matchText}`);
    }

    // Kalau lebih dari satu kandidat (jarang), ambil yang teksnya paling pendek
    // (paling spesifik/dekat dengan target).
    candidates.sort((a, b) => a.text.length - b.text.length);
    const chosen = candidates[0];

    if (candidates.length > 1) {
      this.log.warn(
        `[flow] Ambiguitas tombol untuk target "${matchText}": kandidat = ${JSON.stringify(candidates.map((c) => c.text))}. Dipilih: "${chosen.text}"`
      );
    }

    if (isKeyboardButton) {
      this.log.info(`[flow] Kirim teks tombol KEYBOARD: "${chosen.text}" (target: "${matchText}")`);
      await this.client.sendMessage(this.botEntity, { message: chosen.text });
    } else {
      this.log.info(`[flow] Klik tombol INLINE: "${chosen.text}" (target: "${matchText}")`);
      await chosen.btn.click({});
    }
    return true;
  }

  // ==========================================================
  // RESOLVE TOMBOL MANA YANG DIKLIK UNTUK SUATU STEP.
  //
  // MEKANISME FALLBACK (INTI PERMINTAAN): kalau AI navigator aktif,
  // coba tanya AI dulu. Kalau AI TIDAK aktif, ATAU AI error/timeout/
  // circuit-open/hasil tidak valid/confidence rendah -> LANGSUNG
  // pakai defaultTarget (flow baku), tanpa AI sama sekali. Ini
  // dibungkus try/catch ketat supaya kegagalan AI TIDAK PERNAH
  // menggagalkan transaksi.
  // ==========================================================
  async _resolveButtonTarget(msg, stepName, defaultTarget, goal, orderContext, history) {
    if (!this.nav) return defaultTarget;

    const availableButtons = this._getButtonTexts(msg);

    try {
      const decision = await this.nav.decide({
        stepName,
        goal,
        botMessage: msg.message || "",
        availableButtons,
        defaultTarget,
        orderContext,
        recentHistory: history,
      });

      if (decision.action === "click_button" && decision.confidence === "high") {
        if (decision.target !== defaultTarget) {
          this.log.warn(
            `[flow-ai] Step "${stepName}": AI pilih tombol BEDA dari default ("${decision.target}" vs default "${defaultTarget}"). Kemungkinan bot update UI, pakai saran AI.`
          );
        } else {
          this.log.info(`[flow-ai] Step "${stepName}": AI konfirmasi tombol default "${defaultTarget}" (source: ${decision.source}).`);
        }
        return decision.target;
      }

      // action unknown_flow atau confidence low -> jangan dipakai
      this.log.warn(`[flow-ai] Step "${stepName}": keputusan AI tidak dipakai (${JSON.stringify(decision)}). Fallback ke default "${defaultTarget}".`);
      return defaultTarget;
    } catch (err) {
      // AI mati / timeout / circuit open / parse error / hallucinated button, dsb.
      // -> DIAM-DIAM fallback ke flow baku, jangan sampai transaksi gagal
      // gara-gara AI-nya bermasalah.
      this.log.warn(`[flow-ai] Step "${stepName}": AI gagal/tidak tersedia (${err.message}). Fallback ke default "${defaultTarget}".`);
      return defaultTarget;
    }
  }

  // ==========================================================
  // PARSING STRUK HASIL TRANSAKSI
  // ==========================================================
  _parseStruk(text) {
    const grab = (labels) => {
      for (const label of labels) {
        const re = new RegExp(label + "\\s*[:\\-]?\\s*([^\\n]+)", "i");
        const m = text.match(re);
        if (m) return m[1].trim();
      }
      return "";
    };

    return {
      coin: grab(["coin", "koin"]),
      nominal: grab(["nominal", "total bayar", "harga"]),
      qty: grab(["qty", "jumlah coin", "jumlah"]),
      wallet: grab(["wallet", "alamat wallet", "address"]),
      point: grab(["point", "poin"]),
      txid: grab(["txid", "tx id", "hash", "transaction id"]),
      date: grab(["date", "tanggal", "waktu"]),
      raw_message: text,
    };
  }

  // ==========================================================
  // FLOW UTAMA (satu attempt, tanpa retry - retry ditangani caller)
  //
  // Struktur step TETAP BAKU/DETERMINISTIK seperti sebelumnya:
  // /start -> Transaksi -> Cryptocurrency -> pilih coin -> kirim
  // nominal -> kirim wallet -> klik Sesuai -> tunggu struk.
  // Yang berubah: tiap step KLIK TOMBOL lewat _resolveButtonTarget
  // yang otomatis fallback ke teks tombol default kalau AI
  // bermasalah. Step kirim nominal/wallet TIDAK melibatkan AI sama
  // sekali (nilainya selalu dari order, waktunya divalidasi regex).
  // ==========================================================
  async _runFlowOnce(order, state) {
    const { stepDelay, waitTimeout, strukTimeout } = this.cfg.timing;
    const bot = this.botEntity;
    const goal = "Beli koin crypto lewat bot sampai dapat struk transaksi (txid/hash)";
    const orderContext = { coin: order.coin, nominal: order.nominal, wallet: order.wallet };
    const history = []; // { step, botMessage } - konteks ringkas buat AI

    const pushHistory = (step, msg) => {
      history.push({ step, botMessage: (msg.message || "").slice(0, 200) });
      if (history.length > 5) history.shift();
    };

    this.log.info(`[flow] Order ${order.id}: kirim /start`);
    let msg = await this._sendAndWaitSafe(
      () => this.client.sendMessage(bot, { message: "/start" }),
      waitTimeout,
      this._filterHasButtons
    );
    this.log.info(`[flow] Balasan diterima: "${(msg.message || "").slice(0, 80)}..."`);
    pushHistory("start", msg);
    await sleep(stepDelay);

    // --- klik menu "Transaksi" (default dari CFG, biasanya berlabel emoji) ---
    const targetTransaksi = await this._resolveButtonTarget(
      msg, "click_transaksi", this.cfg.menuButtonTransaksi || "Transaksi", goal, orderContext, history
    );
    msg = await this._sendAndWaitSafe(() => this._clickButton(msg, targetTransaksi), waitTimeout, this._filterHasButtons);
    this.log.info(`[flow] Balasan diterima: "${(msg.message || "").slice(0, 80)}..."`);
    pushHistory("click_transaksi", msg);
    await sleep(stepDelay);

    // --- klik "Cryptocurrency" ---
    const targetCrypto = await this._resolveButtonTarget(
      msg, "click_cryptocurrency", "Cryptocurrency", goal, orderContext, history
    );
    msg = await this._sendAndWaitSafe(() => this._clickButton(msg, targetCrypto), waitTimeout, this._filterHasButtons);
    this.log.info(`[flow] Balasan diterima: "${(msg.message || "").slice(0, 80)}..."`);
    pushHistory("click_cryptocurrency", msg);
    await sleep(stepDelay);

    // --- pilih coin ---
    const targetCoin = await this._resolveButtonTarget(
      msg, "click_coin", order.coin, goal, orderContext, history
    );
    msg = await this._sendAndWaitSafe(() => this._clickButton(msg, targetCoin), waitTimeout);
    this.log.info(`[flow] Balasan diterima: "${(msg.message || "").slice(0, 80)}..."`);
    pushHistory("click_coin", msg);
    await sleep(stepDelay);

    // --- kirim nominal langsung setelah pilih coin (TANPA AI, TANPA nunggu
    // validasi kata kunci - urutan bot dipercaya linear: abis pilih coin,
    // bot pasti nanya nominal). Ini menghindari macet/diam kalau kalimat
    // asli bot beda dari tebakan regex kita. Pesan balasannya boleh gak
    // ada tombol (wajar, ini prompt teks), jadi TANPA filter. ---
    this.log.info(`[flow] Kirim nominal: ${order.nominal}`);
    msg = await this._sendAndWaitSafe(
      () => this.client.sendMessage(bot, { message: String(order.nominal) }),
      waitTimeout
    );
    this.log.info(`[flow] Balasan diterima: "${(msg.message || "").slice(0, 80)}..."`);
    pushHistory("send_nominal", msg);
    await sleep(stepDelay);

    // --- kirim wallet langsung setelah nominal (TANPA AI). Balasannya WAJIB
    // ditunggu sampe ketemu yang ADA TOMBOLNYA - soalnya di sini biasanya
    // bot kirim pesan peringatan dulu (tanpa tombol) SEBELUM baru kirim
    // "Konfirmasi Transaksi" yang ada tombol Sesuai/Batalkan-nya. Kalau
    // gak difilter, kita bisa nyangkut nyoba klik tombol di pesan
    // peringatan yang gak ada tombolnya sama sekali -> gagal -> retry
    // dari /start lagi (padahal harusnya cuma tinggal klik Sesuai). ---
    this.log.info(`[flow] Kirim wallet: ${order.wallet}`);
    msg = await this._sendAndWaitSafe(
      () => this.client.sendMessage(bot, { message: order.wallet }),
      waitTimeout,
      this._filterHasButtons
    );
    this.log.info(`[flow] Balasan diterima: "${(msg.message || "").slice(0, 80)}..."`);
    pushHistory("send_wallet", msg);
    await sleep(stepDelay);

    // --- klik konfirmasi "Sesuai" SEKALIGUS pasang listener struk BARENGAN
    // (pakai _sendAndWaitSafe, sama seperti step-step lain). Ini FIX untuk
    // race condition: sebelumnya klik "Sesuai" -> sleep(confirmDelay) dulu
    // -> baru pasang listener. Kalau bot Kedai Crypto kirim struk LEBIH
    // CEPAT dari confirmDelay itu (misal struk masuk detik ke-2, padahal
    // confirmDelay 6 detik), pesannya lewat begitu saja karena listener
    // GramJS itu event-based - kalau belum dipasang pas pesan lewat, pesan
    // itu HILANG, gak ketangkep sama sekali. Worker jadi nunggu dari nol
    // seolah struk belum ada, padahal udah lama nongol di chat, ujungnya
    // false-timeout walau transaksinya sendiri sukses.
    //
    // Filter DIPERKETAT: wajib ada indikator KUAT data transaksi (txid/hash)
    // atau header terstruktur "=== ... ===", BUKAN cuma kata umum "berhasil"
    // - soalnya bot suka kirim GIF/stiker perayaan dengan caption pendek
    // ("Transaksi Berhasil! 🎉") DULU sebelum struk detail beneran menyusul.
    // Kalau cuma andelin kata "berhasil", caption GIF itu bisa ke-anggap
    // struk padahal kosongan.
    const targetSesuai = await this._resolveButtonTarget(
      msg, "click_sesuai", "Sesuai", goal, orderContext, history
    );
    this.log.info(`[flow] Klik "Sesuai" sambil langsung pasang listener struk...`);
    const struk = await this._sendAndWaitSafe(
      () => this._clickButton(msg, targetSesuai),
      strukTimeout,
      (m) => {
        const t = m.message || "";
        return /txid|tx\s*id|transaction\s*id|hash/i.test(t) || /===.*(struk|receipt|invoice)/i.test(t);
      }
    );
    pushHistory("click_sesuai", struk);

    state.receiptReceived = true;
    return this._parseStruk(struk.message || "");
  }

  // ==========================================================
  // FLOW DENGAN RETRY MAKSIMAL N KALI
  // Jika struk sudah muncul sebelum error, JANGAN ulangi transaksi.
  // ==========================================================
  async processOrder(order) {
    const maxRetry = this.cfg.timing.maxRetry;
    let lastError = null;

    for (let attempt = 1; attempt <= maxRetry; attempt++) {
      const state = { receiptReceived: false };
      try {
        if (!this.isReady()) {
          await this.client.connect();
        }
        const result = await this._runFlowOnce(order, state);
        return { success: true, data: result };
      } catch (err) {
        lastError = err;
        this.log.error(
          `[telegram] Order ${order.id} percobaan ${attempt}/${maxRetry} gagal: ${err.message}`
        );

        if (state.receiptReceived) {
          this.log.warn(`[telegram] Struk sudah diterima untuk order ${order.id}, tidak retry ulang transaksi.`);
          break;
        }

        if (attempt < maxRetry) {
          await sleep(3000);
          continue;
        }
      }
    }

    return {
      success: false,
      reason: lastError ? lastError.message : "UNKNOWN_ERROR",
    };
  }
}

module.exports = { TelegramWorker };
