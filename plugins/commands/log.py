"""
plugins/commands/log.py
────────────────────────
Logging ke channel owner:
  - Notif saat bot masuk grup baru
  - /list (owner DM) → lihat semua grup aktif
  - Log deteksi alasan pesan dihapus (group=3)

Desain log SERAGAM: semua pakai header ❖ ICON LABEL ❖ + isi sebagai teks
biasa (TIDAK pakai <blockquote> — Pyrogram 2.0.106 menolak sebagian pesan
dengan ENTITY_BOUNDS_INVALID saat tag ini dipakai, lihat riwayat perubahan).

ICON + LABEL header SELALU diambil dari core/violation_types.py (SATU
sumber kebenaran) — TIDAK ADA keyword-matching atau icon_map lokal di
file ini lagi. Setiap jenis pelanggaran punya kode VIOLATION_* sendiri,
dan kode yang sama dipakai juga untuk panel log per grup (insert_group_
action_log(..., jenis=...)), sehingga LOG_CHANNEL dan panel grup selalu
tampil dengan icon + label yang identik untuk jenis pelanggaran yang sama.
"""

import os
import re
import time
import html
import hashlib
import asyncio
from datetime import datetime
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ParseMode, MessageEntityType
from pyrogram.errors import PeerIdInvalid, ChannelInvalid, ChatIdInvalid, FloodWait

from database import (
    config_db, get_config, is_admin, regex_db, messages_db, db,
    GLOBAL_EXPIRY, TZ_WIB,
    set_global_flood_backoff, wait_global_flood_backoff,
)
from core.regex_utils import remove_mentions_for_regex, match_with_leet
from plugins.nexus.engine import pipeline_pembersihan
from core.violation_types import (
    VIOLATION_REGEX_GLOBAL, VIOLATION_REGEX_GRUP, VIOLATION_DUPLIKAT_LOKAL,
    VIOLATION_FLOOD_DUPLIKAT_RAM,
    VIOLATION_GCAST_GLOBAL, VIOLATION_BIO_LINK, VIOLATION_LINK_PESAN,
    VIOLATION_MENTION_NON_MEMBER, VIOLATION_MENTION_BIO_GRUP, VIOLATION_MUTE_SENYAP,
    VIOLATION_NEXUS_AI, VIOLATION_AI_MANUAL_AUTO, VIOLATION_SISTEM_GRUP_BARU,
    VIOLATION_MASS_FLOOD_BURST,
    format_violation_header,
)

OWNER_ID    = int(os.environ.get("OWNER_ID", 0))
LOG_CHANNEL = int(os.environ.get("LOG_CHANNEL", 0))

free_col            = db["free_per_group"]
group_regex_db      = db["regex_per_group"]
_log_local_regex_cache: dict[int, tuple[list, float]] = {}

_log_channel_valid: bool | None = None
_log_channel_fail_ts: float = 0.0
_LOG_CHANNEL_RETRY_INTERVAL = 300  # 5 menit sebelum retry setelah gagal

# ── BATCHING LOG QUEUE ────────────────────────────────────────────────────────
# STRATEGI (ganti dari "flush tiap N detik apapun isinya" → "flush tiap N
# ENTRI, atau kalau antrian diam") — supaya jumlah panggilan Telegram API
# ("aksi" — send_message ke LOG_CHANNEL) makin irit, tapi log tetap tidak
# pernah nyangkut lama:
#
#   1. Begitu antrian mencapai LOG_BATCH_MIN_ENTRIES (default 10) → langsung
#      di-flush (harapannya match 1x kirim pesan — tapi kalau total karakter
#      10 entri itu > LOG_MAX_CHARS, tetap otomatis kepecah beberapa pesan
#      seperti sebelumnya, lihat _flush_log_queue_once).
#   2. Kalau antrian DIAM (tidak ada entri BARU masuk) selama
#      LOG_IDLE_FLUSH_SECS (default 60 detik) TAPI masih ada sisa yang
#      belum di-flush (kurang dari 10 entri) → tetap dipaksa post apa
#      adanya, supaya log tidak nyangkut lama menunggu entri ke-10 yang
#      belum tentu datang dalam waktu dekat (mis. grup lagi sepi).
#
# LOG_FLUSH_INTERVAL sekarang jadi POLL INTERVAL internal murni (loop worker
# cuma cek panjang list Python di RAM tiap sekian detik — BUKAN panggil
# Telegram API). Jadi aman diset kecil (default 3 detik) tanpa menambah
# beban API sama sekali; API call (send_message) CUMA terjadi saat salah
# satu dari 2 syarat di atas benar-benar terpenuhi.
LOG_FLUSH_INTERVAL    = int(os.environ.get("LOG_FLUSH_INTERVAL", 3))       # poll interval (RAM-only, murah)
LOG_BATCH_MIN_ENTRIES = int(os.environ.get("LOG_BATCH_MIN_ENTRIES", 10))   # syarat 1 — jumlah entri
LOG_IDLE_FLUSH_SECS   = int(os.environ.get("LOG_IDLE_FLUSH_SECS", 60))     # syarat 2 — antrian diam
LOG_MAX_CHARS       = 3500
LOG_MAX_QUEUE       = 500

_log_queue: list[str] = []
_log_queue_lock = asyncio.Lock()
_log_dropped_count = 0
_log_queue_last_add_ts: float = 0.0  # kapan entri TERAKHIR masuk antrian (buat cek "diam")


async def _enqueue_log(text: str) -> None:
    global _log_dropped_count, _log_queue_last_add_ts
    if not LOG_CHANNEL:
        return
    async with _log_queue_lock:
        if len(_log_queue) >= LOG_MAX_QUEUE:
            _log_dropped_count += 1
            return
        _log_queue.append(text)
        _log_queue_last_add_ts = time.time()


async def _send_log(client: Client, text: str) -> bool:
    await _enqueue_log(text)
    return True


async def _flush_log_queue_once(client: Client) -> None:
    global _log_channel_valid, _log_channel_fail_ts, _log_dropped_count
    if not LOG_CHANNEL:
        return

    await wait_global_flood_backoff()

    async with _log_queue_lock:
        if not _log_queue and _log_dropped_count == 0:
            return

        # ── Gating: hanya lanjut (panggil Telegram) kalau salah satu syarat
        # terpenuhi. Kalau tidak, biarkan tetap di antrian — worker loop
        # akan cek lagi di poll berikutnya (murah, RAM-only, tidak ada cost).
        q_len       = len(_log_queue)
        idle_secs   = time.time() - _log_queue_last_add_ts
        should_flush = (
            _log_dropped_count > 0                    # selalu flush kalau ada yg dibuang (peringatan)
            or q_len >= LOG_BATCH_MIN_ENTRIES          # syarat 1: sudah cukup banyak
            or idle_secs >= LOG_IDLE_FLUSH_SECS        # syarat 2: antrian diam terlalu lama
        )
        if not should_flush:
            return

        pending = _log_queue.copy()
        _log_queue.clear()
        dropped = _log_dropped_count
        _log_dropped_count = 0

    if _log_channel_valid is False:
        if time.time() - _log_channel_fail_ts >= _LOG_CHANNEL_RETRY_INTERVAL:
            _log_channel_valid = None
        else:
            async with _log_queue_lock:
                _log_queue[0:0] = pending
                _log_dropped_count += dropped
            return

    if dropped:
        pending.append(
            f"⚠️ <b>{dropped} entri log dibuang</b> (antrian penuh saat flood tinggi)."
        )

    batches: list[str] = []
    current = ""
    sep = "\n\n— — —\n\n"
    for entry in pending:
        candidate = (current + sep + entry) if current else entry
        if len(candidate) > LOG_MAX_CHARS and current:
            batches.append(current)
            current = entry
        else:
            current = candidate
    if current:
        batches.append(current)

    not_sent: list[str] = []
    for i, batch_text in enumerate(batches):
        if i > 0:
            await asyncio.sleep(0.5)
        try:
            await client.send_message(
                LOG_CHANNEL, batch_text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            _log_channel_valid = True
        except (PeerIdInvalid, ChannelInvalid, ChatIdInvalid) as e:
            if _log_channel_valid is None:
                print(f"[LOG] LOG_CHANNEL tidak valid ({LOG_CHANNEL}): {e}. "
                      f"Akan retry dalam {_LOG_CHANNEL_RETRY_INTERVAL//60} menit.")
            _log_channel_valid = False
            _log_channel_fail_ts = time.time()
            not_sent.extend(batches[i:])
            break
        except FloodWait as e:
            print(f"[LOG] FloodWait {e.value}s — batch ditunda, dikembalikan ke antrian.")
            set_global_flood_backoff(e.value)
            not_sent.extend(batches[i:])
            break
        except Exception as e:
            print(f"[LOG ERROR] {e}")
            not_sent.extend(batches[i:])
            break

    if not_sent:
        async with _log_queue_lock:
            _log_queue[0:0] = not_sent


async def log_flush_worker_loop(client: Client) -> None:
    while True:
        try:
            await _flush_log_queue_once(client)
        except Exception as e:
            print(f"[LOG FLUSH WORKER ERROR] {e}")
        await asyncio.sleep(LOG_FLUSH_INTERVAL)


async def _get_local_patterns_log(chat_id: int):
    now = time.monotonic()
    hit = _log_local_regex_cache.get(chat_id)
    if hit:
        if (now - hit[1]) < 300:
            return hit[0]
        # v2: sebelumnya entry kedaluwarsa TIDAK dihapus di sini — cuma
        # "dianggap miss" dan DITIMPA ulang di bawah kalau grup ini masih
        # aktif dipakai. Tapi kalau grup ini SUDAH TIDAK PERNAH dipanggil
        # lagi (bot sudah tidak di grup itu dll), list compiled-regex lama
        # menggantung di RAM selamanya. Dibuang eksplisit di sini.
        _log_local_regex_cache.pop(chat_id, None)
    patterns = []
    async for doc in group_regex_db.find({"chat_id": chat_id}):
        try:
            patterns.append((re.compile(doc["pattern"], re.IGNORECASE), doc.get("raw", doc["pattern"])))
        except Exception:
            pass
    _log_local_regex_cache[chat_id] = (patterns, now)
    return patterns


# ── Helper: format waktu ──────────────────────────────────────────────────────
def _fmt_waktu() -> str:
    return datetime.now(TZ_WIB).strftime("%d/%m/%Y %H:%M:%S WIB")


# ── Helper: baris user ────────────────────────────────────────────────────────
def _user_line(uid: int, name: str) -> str:
    safe_name = html.escape(name or str(uid))
    return f"<a href='tg://user?id={uid}'>{safe_name}</a> (<code>{uid}</code>)"


# ── LOG 1: Bot masuk grup baru ────────────────────────────────────────────────
@Client.on_message(filters.service, group=10)
async def log_new_group(client: Client, message: Message):
    if not message.new_chat_members or not LOG_CHANNEL:
        return
    me = client.me
    for member in message.new_chat_members:
        if member.id == me.id:
            chat  = message.chat
            text  = (
                f"<b>❖ {format_violation_header(VIOLATION_SISTEM_GRUP_BARU)} ❖</b>\n"
                f"◈ <b>Grup:</b> {html.escape(chat.title or str(chat.id))}\n"
                f"◈ <b>ID:</b> <code>{chat.id}</code>\n"
                f"◈ <b>Username:</b> @{chat.username if chat.username else '—'}\n"
                f"◈ <b>Waktu:</b> {_fmt_waktu()}\n"
                "<i>Firewall aktif pada grup ini.</i>"
            )
            await _send_log(client, text)


# ── LOG 2: /list — daftar semua grup (tampilan sama dengan panel Nexus >
#           Owner Bot > Grup Terdaftar, tanpa tombol kembali ke mainframe) ──
@Client.on_message(filters.command("list") & filters.private & filters.user(OWNER_ID))
async def list_grup_pengguna(client: Client, message: Message):
    msg = await message.reply("⏳ <i>Menarik data node grup dari server...</i>", parse_mode=ParseMode.HTML)

    from plugins.nexus.nexus_handlers import build_grup_terdaftar_text
    text = await build_grup_terdaftar_text(client)

    # Sama seperti panel Nexus: bisa melebihi batas 4096 karakter Telegram
    # kalau grup banyak — pecah per ~3900 karakter, potong di batas baris
    # antar-grup ("\n\n") supaya 1 entri grup tidak terbelah di tengah.
    if len(text) <= 3900:
        await msg.edit(text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        return

    entries = text.split("\n\n")
    chunks, current_chunk = [], ""
    for entry in entries:
        if current_chunk and len(current_chunk) + len(entry) + 2 > 3900:
            chunks.append(current_chunk)
            current_chunk = ""
        current_chunk += (entry + "\n\n") if current_chunk == "" else entry + "\n\n"
    if current_chunk:
        chunks.append(current_chunk)

    await msg.edit(chunks[0], parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    for extra in chunks[1:]:
        await message.reply(extra, parse_mode=ParseMode.HTML, disable_web_page_preview=True)


# ── LOG 3: Log alasan pesan dihapus (group=3) ────────────────────────────────
@Client.on_message((filters.group | filters.forum) & ~filters.service, group=3)
async def log_deletion_trigger(client: Client, message: Message):
    if not message.from_user or not LOG_CHANNEL:
        return
    # Handler ini SENGAJA cuma nge-spawn background task lalu langsung
    # return — supaya dispatch group=3 Pyrogram tidak ketahan nunggu delay
    # di bawah untuk SETIAP pesan yang masuk (termasuk pesan bersih).
    asyncio.create_task(_log_deletion_trigger_delayed(client, message))


# Jeda sebelum shadow-redetector di bawah mulai kerja — kasih waktu ke
# pipeline utama (core/antispam_queue.py::_process_detection, jalan di
# background queue terpisah dari dispatch on_message ini) buat sempat
# klaim + kirim log-nya sendiri kalau memang ada gate yang menang (mis.
# Gate F/Ubot Detect, lihat mark_log_channel_sent() di _gate_ubot).
# Tanpa jeda ini, fungsi di bawah HAMPIR SELALU jalan duluan (dispatch
# on_message group=3 nyaris instan, sedangkan _process_detection baru
# mulai proses dari antrean async terpisah) — makanya dulu shadow-redetector
# ini nyaris selalu ikut generate log sendiri (mis. "Filter Owner Global")
# meski pesannya sudah tercatat sebagai UBOT_DETECT oleh gate lain.
_SHADOW_REDETECT_DELAY = 1.5


async def _log_deletion_trigger_delayed(client: Client, message: Message):
    await asyncio.sleep(_SHADOW_REDETECT_DELAY)

    cid = message.chat.id
    uid = message.from_user.id

    from database import is_log_channel_sent
    if is_log_channel_sent(cid, message.id):
        # Sudah ada log LOG_CHANNEL langsung dari gate lain (mis. Ubot
        # Detect) untuk pesan ini — jangan tebak ulang & kirim log kedua.
        return

    if await is_admin(client, cid, uid):
        return

    if await free_col.find_one({"user_id": uid, "chat_id": cid}):
        return

    content = (message.text or message.caption or "").strip()
    if not content or content.startswith("/"):
        return

    cfg    = await get_config(cid)
    kode   = None   # kode VIOLATION_* (core/violation_types.py) — bukan teks bebas
    detail = ""
    now_ts = time.time()
    regex_safe       = remove_mentions_for_regex(message)
    teks_super_clean = pipeline_pembersihan(content)

    # AI Manual (Gate E, auto-detect) — dicek DULUAN, mengikuti urutan gate
    # asli di core/antispam_queue.py (AI manual → Trigger AI observer).
    # FIX: batas minimal 8 karakter dihapus, disamakan dengan gate asli
    # (core/antispam_queue.py::_gate_nexus_ai) yang sudah tidak punya batas.
    # FIX: pesan link MURNI (tanpa teks lain) dilewatkan di sini juga —
    # disamakan dengan gate asli yang sekarang menyerahkan keputusan pesan
    # link murni sepenuhnya ke Gate B / toggle "anti_link" (lihat komentar
    # di _gate_nexus_ai untuk alasan lengkap: mencegah Bayes/PatternMemory
    # menghapus link walau toggle anti_link sedang off).
    _text_no_url_log = re.sub(
        r"(https?://\S+|t\.me/\S+|wa\.me/\S+|bit\.ly/\S+|s\.id/\S+|"
        r"linktr\.ee/\S+|cutt\.ly/\S+|@[a-zA-Z0-9_]{4,})",
        "", content,
    ).strip()
    if cfg.get("anti_spam_ai", False) is True and _text_no_url_log:
        try:
            from nexus.ai_core.bridge import nexus_ai_auto_detect
            from nexus.ai_core.constants import NEXUS_MIN_CONFIDENCE
            ai_result = await nexus_ai_auto_detect(
                text=content,
                metadata={"chat_id": cid, "user_id": uid},
                min_confidence=NEXUS_MIN_CONFIDENCE,
            )
            if ai_result is not None:
                kode   = VIOLATION_AI_MANUAL_AUTO
                top    = ai_result.reasons[0][:80] if ai_result.reasons else "-"
                detail = (
                    f"◈ <b>Confidence:</b> <code>{ai_result.confidence * 100:.0f}%</code>\n"
                    f"◈ <b>Layer:</b> <code>{html.escape(str(ai_result.layer))}</code>\n"
                    f"◈ <b>Keterangan:</b> {html.escape(top)}"
                )
                # v5.8 — rincian angka per-layer (Bayes/Feature/Category/PatternMemory
                # + rumus & bobot .env yang dipakai), supaya begitu ada log yang
                # kelihatan janggal, alasan matematisnya langsung kebaca tanpa
                # perlu tanya balik / bongkar kode.
                if ai_result.debug:
                    detail += (
                        f"\n◈ <b>Rincian Skor:</b>\n"
                        f"<pre>{html.escape(ai_result.debug)}</pre>"
                    )

        except Exception:
            pass

    # Regex global (Trigger AI) — SENGAJA DIHAPUS (v8.0): Trigger AI Global
    # sekarang PURE OBSERVER di gate asli (core/antispam_queue.py::_gate_regex)
    # — cuma kirim ke spam_claim_queue buat training, TIDAK PERNAH menghapus
    # pesan lagi. Dulu di sini ada blok yang nebak "kalau teks match Trigger
    # AI Global → kode=VIOLATION_REGEX_GLOBAL" dan langsung kirim log
    # "🪤 Filter Owner Global" ke LOG_CHANNEL — padahal pesannya belum tentu
    # (dan sekarang TIDAK PERNAH) betulan dihapus karena match itu. Itu bikin
    # LOG_CHANNEL nunjukin log "menghapus" palsu utk pesan yg sebenarnya cuma
    # dipakai buat training, gak kesentuh sama sekali. JANGAN ditambah lagi
    # kecuali Trigger AI Global beneran dihidupkan lagi buat hapus pesan.

    # Regex lokal (Group Filter)
    if not kode:
        for pat, raw_pattern in await _get_local_patterns_log(cid):
            if match_with_leet(pat, regex_safe):
                kode   = VIOLATION_REGEX_GRUP
                detail = (
                    f"◈ <b>Kata yang cocok:</b> <code>{html.escape(str(raw_pattern))}</code>\n"
                    f"◈ <b>Keterangan:</b> Pesan mengandung kata terlarang yang diset admin grup ini"
                )
                break

    # Anti-duplikasi lokal
    if not kode and cfg.get("local") is True:
        lokal_record = await messages_db.find_one({
            "chat_id": cid, "msg_id": message.id, "type": "local_track"
        })
        if lokal_record and lokal_record.get("warned") is True:
            kode   = VIOLATION_DUPLIKAT_LOKAL
            detail = (
                "◈ <b>Keterangan:</b> User mengirim pesan yang sama berulang kali di grup ini\n"
                f"◈ <b>Jeda deteksi:</b> {cfg.get('expiry', 60)} detik"
            )

    # Anti-gcast global
    # PENTING: key harus dihitung PERSIS sama seperti core/antispam_queue.py
    # (_gate_gcast) — yaitu md5 dari content yang SUDAH dinormalisasi lewat
    # simplify(), bukan content mentah. Sebelumnya di sini hash dihitung dari
    # content mentah, jadi _id yang dicari nyaris tidak pernah sama dengan
    # dokumen yang benar-benar ditulis oleh gate asli begitu simplify()
    # mengubah teksnya — akibatnya entri GCAST_GLOBAL nyaris tidak pernah
    # sukses tercatat ke LOG_CHANNEL walau pesannya memang dihapus sebagai gcast.
    if not kode and cfg.get("global") is True:
        from core.regex_utils import simplify as _simplify_gcast_log
        content_norm = _simplify_gcast_log(content) or content
        content_hash = hashlib.md5(content_norm.encode()).hexdigest()
        global_key   = f"glob_{uid}_{content_hash}"
        existing     = await messages_db.find_one({"_id": global_key})
        if existing and (now_ts - existing.get("time", 0)) < GLOBAL_EXPIRY:
            locs = existing.get("locations", {}) or {}
            if len(locs) >= 2:
                kode   = VIOLATION_GCAST_GLOBAL
                detail = (
                    f"◈ <b>Keterangan:</b> Pesan yang sama dikirim serentak ke {len(locs)} grup sekaligus\n"
                    "◈ <b>Indikator:</b> Pola broadcast massal — konten identik muncul di banyak grup dalam waktu singkat"
                )

    # Bio link
    if not kode and cfg.get("bio_check") is True:
        try:
            # FIX: nama cache di bio.py adalah _mem_cache (bukan _bio_cache),
            # dan key-nya adalah (chat_id, user_id) — bukan uid saja.
            from plugins.filters.bio import _mem_cache as _bio_mem_cache
            hit = _bio_mem_cache.get((cid, uid))
            if hit and hit[0] is True:
                kode   = VIOLATION_BIO_LINK
                detail = (
                    "◈ <b>Keterangan:</b> Bio profil Telegram user mengandung link\n"
                    "◈ <b>Kebijakan:</b> Grup ini tidak mengizinkan member dengan bio berisi link"
                )
        except ImportError:
            pass

    # Link detector — FIX: sebelumnya tidak mengecek toggle "anti_link" sama
    # sekali (beda dengan semua blok lain di atas yang selalu cek cfg dulu),
    # padahal gate hapus asli (core/antispam_queue.py::_gate_link) SELALU
    # mengecek cfg.get("anti_link", True) sebelum menghapus. Akibatnya kalau
    # toggle "anti_link" grup di-OFF-kan, pesan berisi link memang benar
    # TIDAK dihapus oleh gate asli — tapi shadow-redetector ini tetap
    # menebak & mengirim log "Link di Pesan" ke LOG_CHANNEL seolah-olah
    # pesannya dihapus karena link, padahal tidak tersentuh sama sekali.
    if not kode and cfg.get("anti_link", True) is True:
        url_types    = {MessageEntityType.URL, MessageEntityType.TEXT_LINK}
        all_entities = list(message.entities or []) + list(message.caption_entities or [])
        if any(e.type in url_types for e in all_entities):
            kode   = VIOLATION_LINK_PESAN
            detail = (
                "◈ <b>Keterangan:</b> Pesan berisi link atau tautan aktif\n"
                "◈ <b>Kebijakan:</b> Grup ini tidak mengizinkan pengiriman link"
            )

    # External mention — FIX: sebelumnya pakai _is_external_mention (versi
    # API lama) yang HANYA mengenali kind "channel"/"grup" dan tidak pernah
    # mengecek toggle "mention_batasi_akun" — akibatnya pesan yang dihapus
    # karena mention non_akun/non_member/bio_grup TIDAK PERNAH tercatat di
    # LOG_CHANNEL (padahal panel log per grup sudah mencatatnya benar lewat
    # gate asli). Sekarang dipersamakan: pakai _is_external_mention_cache_only
    # — fungsi YANG SAMA PERSIS dipakai gate hapus asli
    # (core/antispam_queue.py::_gate_mention_cache) — supaya tebakan di sini
    # selalu konsisten dengan alasan penghapusan yang sebenarnya, untuk
    # SEMUA sub-toggle (batasi_channel / batasi_grup / batasi_akun).
    if not kode and cfg.get("anti_mention", True) is True:
        try:
            from plugins.filters.antispam import _is_external_mention_cache_only
            _is_ext, _kind, _uname = await _is_external_mention_cache_only(client, message, cfg)
            # cache_miss & bio_pending = penghapusan SEMENTARA (belum pasti
            # spam, cuma nunggu resolve/scan bio di belakang layar) — SAMA
            # seperti di gate asli, jangan dicatat sebagai pelanggaran
            # permanen di LOG_CHANNEL.
            if _is_ext and _kind not in ("cache_miss", "bio_pending"):
                _kind_desc = {
                    "channel":  "Username milik channel Telegram",
                    "grup":     "Username milik grup / supergroup Telegram",
                    "non_akun": "Username tidak valid / akun tidak ditemukan",
                    "bio_grup": "Bio profil mempromosikan grup lain",
                    # "non_member" dihapus — tidak lagi dikembalikan oleh
                    # _is_external_mention_cache_only sejak fitur diubah
                    # menjadi "Batasi Tag Akun Promosi".
                }.get(_kind or "", "Bukan akun/channel yang diizinkan")
                kode   = VIOLATION_MENTION_BIO_GRUP if _kind == "bio_grup" else VIOLATION_MENTION_NON_MEMBER
                detail = (
                    f"◈ <b>Username:</b> @{_uname or '?'}\n"
                    f"◈ <b>Jenis:</b> {_kind_desc}\n"
                    f"◈ <b>Keterangan:</b> Pesan menyebut entitas yang tidak diizinkan di grup ini"
                )
        except Exception:
            pass

    # Hapus silent — user masih dalam masa mute aktif
    if not kode and cfg.get("local") is True:
        try:
            from database import get_local_mute
            mute_rec = await get_local_mute(cid, uid)
            if mute_rec.get("muted_until", 0.0) > now_ts:
                until_dt = datetime.fromtimestamp(mute_rec["muted_until"], tz=TZ_WIB)
                kode   = VIOLATION_MUTE_SENYAP
                detail = (
                    f"◈ <b>Keterangan:</b> User sedang dalam masa hukuman mute — pesan dihapus otomatis\n"
                    f"◈ <b>Mute berakhir:</b> {until_dt.strftime('%H:%M:%S WIB')}"
                )
        except Exception:
            pass

    if not kode:
        return

    user_mention = _user_line(uid, message.from_user.first_name)

    log_text = (
        f"<b>❖ {format_violation_header(kode)} ❖</b>\n"
        f"◈ <b>User:</b> {user_mention}\n"
        f"◈ <b>Grup:</b> {html.escape(message.chat.title or str(cid))} (<code>{cid}</code>)\n"
        f"◈ <b>Waktu:</b> {_fmt_waktu()}\n"
        f"{detail}\n\n"
        f"📨 <b>Konten:</b>\n<code>{html.escape(content[:500])}</code>"
    )
    await _send_log(client, log_text)


# ── INTEGRASI NEXUS ───────────────────────────────────────────────────────────

async def log_spam_global(client: Client, message: Message, pola: str, indikator: str):
    """Dipanggil oleh Nexus Engine untuk log pelanggaran GLOBAL."""
    uid          = message.from_user.id
    cid          = message.chat.id
    user_mention = _user_line(uid, message.from_user.first_name)

    konten = (message.text or message.caption or "").strip()
    log_text = (
        f"<b>❖ {format_violation_header(VIOLATION_NEXUS_AI)} ❖</b>\n"
        f"◈ <b>User:</b> {user_mention}\n"
        f"◈ <b>Grup:</b> {html.escape(message.chat.title or str(cid))} (<code>{cid}</code>)\n"
        f"◈ <b>Waktu:</b> {_fmt_waktu()}\n"
        f"◈ <b>Keterangan:</b> AI Nexus mendeteksi pola spam yang sama tersebar di banyak grup\n"
        f"◈ <b>Sinyal AI:</b> <code>{html.escape(str(indikator))}</code>\n"
        f"◈ <b>Pola:</b> <code>{html.escape(str(pola)[:80])}</code>\n\n"
        f"📨 <b>Pesan terakhir:</b>\n<code>{html.escape(konten[:400])}</code>"
    )
    await _send_log(client, log_text)


async def log_spam_lokal(client: Client, message: Message, pola: str, indikator: str):
    """Dipanggil oleh Nexus Engine untuk log pelanggaran LOKAL (Owner)."""
    uid          = message.from_user.id
    cid          = message.chat.id
    user_mention = _user_line(uid, message.from_user.first_name)

    konten = (message.text or message.caption or "").strip()
    log_text = (
        f"<b>❖ {format_violation_header(VIOLATION_NEXUS_AI)} ❖</b>\n"
        f"◈ <b>User:</b> {user_mention}\n"
        f"◈ <b>Grup:</b> {html.escape(message.chat.title or str(cid))} (<code>{cid}</code>)\n"
        f"◈ <b>Waktu:</b> {_fmt_waktu()}\n"
        f"◈ <b>Keterangan:</b> AI Nexus mencocokkan pesan dengan filter kata yang diset pemilik bot\n"
        f"◈ <b>Sinyal AI:</b> <code>{html.escape(str(indikator))}</code>\n"
        f"◈ <b>Pola:</b> <code>{html.escape(str(pola)[:80])}</code>\n\n"
        f"📨 <b>Pesan terakhir:</b>\n<code>{html.escape(konten[:400])}</code>"
    )
    await _send_log(client, log_text)


async def log_duplikat_lokal(client: Client, message, pola: str, indikator: str):
    """Dipanggil antispam.py untuk LOCAL_FLOOD_RAM — duplikasi pesan per-user
    (Proteksi C, bagian dari toggle "anti_flood"). SENGAJA pakai kode
    VIOLATION_FLOOD_DUPLIKAT_RAM (bukan VIOLATION_DUPLIKAT_LOKAL) supaya
    headernya beda dari Anti-Spam Lokal (toggle "local") — dua mekanisme
    beda yang sebelumnya berbagi header sama & membingungkan di LOG_CHANNEL."""
    uid          = message.from_user.id
    cid          = message.chat.id
    user_mention = _user_line(uid, message.from_user.first_name)
    konten       = (message.text or message.caption or "").strip()

    log_text = (
        f"<b>❖ {format_violation_header(VIOLATION_FLOOD_DUPLIKAT_RAM)} ❖</b>\n"
        f"◈ <b>User:</b> {user_mention}\n"
        f"◈ <b>Grup:</b> {html.escape(message.chat.title or str(cid))} (<code>{cid}</code>)\n"
        f"◈ <b>Waktu:</b> {_fmt_waktu()}\n"
        f"◈ <b>Keterangan:</b> User mengirim pesan yang sama berulang kali di grup ini\n"
        f"◈ <b>Jeda deteksi:</b> deteksi RAM (dalam hitungan detik)\n\n"
        f"📨 <b>Pesan terakhir:</b>\n<code>{html.escape(konten[:400])}</code>"
    )
    await _send_log(client, log_text)


async def log_duplikat_lokal_fuzzy(client: Client, message, similarity: float = 0.0):
    """
    Dipanggil core/antispam_queue.py::_gate_local_dup — deteksi duplikasi
    lokal via ANTRIAN (fuzzy-match rapidfuzz terhadap riwayat pesan di DB,
    bukan RAM instan seperti log_duplikat_lokal di atas). Sebelumnya jalur
    ini HANYA mencatat ke panel log grup (insert_group_action_log), TIDAK
    pernah terkirim ke LOG_CHANNEL — diperbaiki di sini supaya owner/admin
    yang memantau LOG_CHANNEL juga melihat pelanggaran jenis ini.
    """
    uid          = message.from_user.id
    cid          = message.chat.id
    user_mention = _user_line(uid, message.from_user.first_name)
    konten       = (message.text or message.caption or "").strip()

    sim_line = f"◈ <b>Kemiripan:</b> ~{similarity:.0f}%\n" if similarity else ""

    log_text = (
        f"<b>❖ {format_violation_header(VIOLATION_DUPLIKAT_LOKAL)} ❖</b>\n"
        f"◈ <b>User:</b> {user_mention}\n"
        f"◈ <b>Grup:</b> {html.escape(message.chat.title or str(cid))} (<code>{cid}</code>)\n"
        f"◈ <b>Waktu:</b> {_fmt_waktu()}\n"
        f"◈ <b>Keterangan:</b> Pesan mirip dengan pesan sebelumnya dari user yang sama di grup ini\n"
        f"{sim_line}"
        f"◈ <b>Jeda deteksi:</b> antrian (fuzzy-match riwayat pesan)\n\n"
        f"📨 <b>Pesan terakhir:</b>\n<code>{html.escape(konten[:400])}</code>"
    )
    await _send_log(client, log_text)


async def log_mass_flood(client: Client, message, pola: str, indikator: str):
    """Dipanggil antispam.py untuk MASS_FLOOD_BURST_RAM — serangan massal banyak akun."""
    uid          = message.from_user.id
    cid          = message.chat.id
    user_mention = _user_line(uid, message.from_user.first_name)
    konten       = (message.text or message.caption or "").strip()

    # FIX: gunakan VIOLATION_MASS_FLOOD_BURST bukan VIOLATION_GCAST_GLOBAL.
    # Kedua jenis berbeda: GCAST = pesan sama lintas BANYAK GRUP,
    # MASS_FLOOD_BURST = banyak AKUN BERBEDA di SATU GRUP (Proteksi B RAM).
    log_text = (
        f"<b>❖ {format_violation_header(VIOLATION_MASS_FLOOD_BURST)} ❖</b>\n"
        f"◈ <b>User:</b> {user_mention}\n"
        f"◈ <b>Grup:</b> {html.escape(message.chat.title or str(cid))} (<code>{cid}</code>)\n"
        f"◈ <b>Waktu:</b> {_fmt_waktu()}\n"
        f"◈ <b>Keterangan:</b> Pesan yang sama dikirim serentak oleh banyak akun berbeda dalam waktu singkat\n"
        f"◈ <b>Indikator:</b> Pola serangan massal (kloning akun / koordinasi spam)\n\n"
        f"📨 <b>Pesan terakhir:</b>\n<code>{html.escape(konten[:400])}</code>"
    )
    await _send_log(client, log_text)


async def log_sistem(client: Client, judul: str, pesan: str):
    """Log notifikasi sistem ke channel — tetap pakai judul bebas (bukan
    pelanggaran), TIDAK lewat registry violation_types karena ini bukan
    salah satu jenis pelanggaran yang diregistrasi."""
    log_text = (
        f"<b>❖ ⚡ {judul.upper()} ❖</b>\n"
        f"◈ <b>Waktu:</b> {_fmt_waktu()}\n"
        f"{pesan}"
    )
    await _send_log(client, log_text)
