"""
plugins/filters/antispam.py
────────────────────────────
Filter utama pesan grup:
  1. Regex global (Trigger AI) & lokal (Regex Grup) — TANPA pengaruh Whitelist Nexus
  2. External mention
  3. Link detector
  4. Anti duplikasi lokal (per user per grup) — RAM flood-counter (PROTEKSI C)
     + jalur fuzzy di core/antispam_queue.py (sort/limit by local_spam_limit).
     [DIHAPUS] Fast-path exact-match-bypass (_exact_match_local_bypass) yang
     dulu ada di sini — perannya (deteksi kalimat 100% identik berulang)
     sekarang diwakili oleh fitur DETEKSI UBOT (core/ubot_detect.py +
     plugins/filters/ubot_detect_filter.py), yang memakai memori Mongo
     terpisah (ubot_sentence_tracker) dan independen dari toggle "local".
  5. Anti duplikasi global (anti-gcast lintas grup) — PROTEKSI MASSAL ANTI-CLONE

SISTEM LOGGING:
  Telah dihubungkan secara penuh dengan plugins.commands.log (log_spam_lokal)
  sehingga setiap tindakan Fast-Path RAM langsung dilaporkan ke log worker/channel.
────────────────────────────
TOGGLE-DRIVEN DETECTION:
  Setiap fitur deteksi (bukan hanya hukuman) dimatikan sepenuhnya saat toggle OFF.
  - anti_flood OFF → PROTEKSI A, B, & C (RAM mass-burst + RAM per-user) tidak
    berjalan sama sekali. Toggle TUNGGAL ini (default ON, tombol "Anti Flood"
    di panel pengaturan grup) SUDAH TIDAK terikat pada toggle "global"
    (anti-gcast, jalur DB) maupun "local" (anti-duplikat lokal fuzzy-match,
    jalur DB) — keduanya independen sepenuhnya sekarang.
  - Logika detection_queue mengikuti toggle masing-masing fitur (global/local/
    anti_link/anti_mention/dst, TIDAK termasuk anti_flood yang sudah dipisah)
────────────────────────────
PERUBAHAN ARSITEKTUR (pindah admin/VIP/RAM-flood ke dalam lorong):
  main_antispam_filter (group=2, FRONT HANDLER) sekarang HANYA melakukan
  pemeriksaan murni-RAM tanpa I/O (is_message_handled, ada teks, bukan
  command) lalu langsung enqueue_for_detection() — TIDAK lagi cek
  is_admin/VIP maupun menjalankan Proteksi A/B/C di sini.

  Semua itu (admin, VIP free_col, VIP bio, Proteksi A/B/C RAM flood) sudah
  pindah ke DALAM lorong (core/antispam_queue.py::_process_detection),
  supaya tiap pesan diperiksa di lorong pesan itu SENDIRI — tidak ada lagi
  pemeriksaan admin/VIP yang terjadi di luar sistem antrian & jadi titik
  antre bersama untuk semua grup. check_ram_flood_protections() di bawah
  ini dipanggil dari dalam lorong (fungsinya tetap di sini karena state
  RAM-nya — _local_flood_cache dkk — memang milik modul ini).
"""

import os
import re
import time
import asyncio
import hashlib
from datetime import datetime
from pyrogram import Client, filters
from pyrogram.enums import MessageEntityType, ChatMemberStatus
from pyrogram.types import ChatMemberUpdated
from pyrogram.errors import UserNotParticipant, PeerIdInvalid, RPCError, UsernameNotOccupied

LOG_CHANNEL = int(os.environ.get("LOG_CHANNEL", 0))

from database import (
    regex_db, get_config, is_admin, db,
    mark_message_handled, is_message_handled,
    mark_message_queued, is_message_queued,
    get_local_mute, reset_local_mute,
    insert_group_action_log,
    check_bot_permissions,
    ensure_group_registered,
    delete_queue,
)
from core.regex_utils import simplify, remove_mentions_for_regex, match_with_leet

# ── IMPOR FUNGSI HUKUMAN & LOG BAWAAN ANDA ───────────────────────────────────
from core.punishment import check_and_punish
from plugins.commands.log import log_spam_lokal, log_duplikat_lokal, log_mass_flood

group_regex_db = db["regex_per_group"]
free_col       = db["free_per_group"]

# ── 1. Cache Per-User (Bom Spam dari 1 Akun Tunggal) ──────────────────────────
_local_flood_cache: dict[int, dict[int, tuple[str, float, int]]] = {}
_FLOOD_WINDOW   = 5.0  
_MAX_DUPLICATE  = 2    

# ── 2. Cache Lintas-User (Serangan Massal Banyak Akun Kloning / Userbot) ──────
# Menghitung jumlah PESAN dengan teks identik dalam window — tidak peduli
# dari 1 user yang sama atau beberapa user berbeda, keduanya dianggap sama
# (spam beruntun 1 akun cepat pun tetap indikasi flood yang valid).
#
# Menyimpan (message, ts) — BUKAN cuma timestamp — supaya begitu ambang
# _MASS_BURST_LIMIT tercapai, PESAN-PESAN SEBELUMNYA yang sudah lolos
# duluan (belum ke-flag karena saat itu hitungannya belum sampai limit)
# BISA ikut ditoleh-ke-belakang & dihapus + dihukum juga — bukan cuma
# pesan yang kebetulan jadi "pemicu ke-N" saja. Lihat check_ram_flood_
# protections Proteksi B untuk detail.
_global_text_tracker: dict[int, dict[str, list[tuple[object, float]]]] = {}
_global_text_blacklist: dict[int, dict[str, float]] = {}

_MASS_BURST_WINDOW = 1.5
_MASS_BURST_LIMIT  = 2    # jumlah pesan identik dalam window sebelum di-flag
_LOCK_DURATION     = 10.0 

# ── Cache regex ───────────────────────────────────────────────────────────────
_regex_cache:     list  = []
_regex_cache_ts:  float = 0.0
_local_regex_cache: dict[int, tuple[list, float]] = {}
REGEX_TTL = 300

_URL_ENTITY_TYPES = {MessageEntityType.URL, MessageEntityType.TEXT_LINK}

# ── Single-flight dedup untuk resolusi @username ke Telegram API ─────────────
# Kalau 2 lorong (2 pesan beda, bisa beda grup) kebetulan sama-sama perlu
# nembak client.get_chat(uname) untuk @username YANG SAMA persis di waktu
# yang berdekatan, cuma request PERTAMA yang benar-benar jalan ke Telegram —
# request berikutnya untuk uname yang sama cukup menunggu (await) hasil yang
# sama, tidak ikut menembak API kedua kalinya. @username yang BEDA sama
# sekali tidak saling tunggu — tetap independen penuh antar lorong.
_mention_resolve_inflight: dict[str, asyncio.Future] = {}


async def _resolve_chat_singleflight(client: Client, uname: str):
    """
    Wrapper single-flight di sekitar client.get_chat(uname). Aman dipakai
    dari banyak lorong bersamaan karena asyncio single-threaded — cek+set
    dict di titik non-await selalu atomik.
    """
    existing = _mention_resolve_inflight.get(uname)
    if existing is not None:
        return await existing

    fut = asyncio.get_running_loop().create_future()
    _mention_resolve_inflight[uname] = fut
    try:
        result = await client.get_chat(uname)
        fut.set_result(result)
        return result
    except Exception as e:
        fut.set_exception(e)
        # Retrieve exception segera supaya asyncio tidak menganggap "never
        # retrieved" saat fut di-garbage-collect (kasus umum: tidak ada
        # lorong lain yang sempat numpang `await existing` di atas — jadi
        # tidak ada siapa pun yang memanggil .result()/.exception() pada
        # future ini selain di sini). Exception asli tetap di-raise normal
        # ke pemanggil lewat jalur biasa (bukan lewat future).
        fut.exception()
        raise
    finally:
        # Hasil (sukses/gagal) sudah tersimpan di future — future lama yang
        # masih dipegang lorong lain yang sempat "numpang" tetap valid walau
        # entry dict ini sudah dibersihkan untuk request BERIKUTNYA.
        _mention_resolve_inflight.pop(uname, None)


def _has_url_entity(message) -> bool:
    entities = list(message.entities or []) + list(message.caption_entities or [])
    return any(e.type in _URL_ENTITY_TYPES for e in entities)


async def _get_global_patterns():
    global _regex_cache, _regex_cache_ts
    now = time.monotonic()
    if now - _regex_cache_ts < REGEX_TTL:
        return _regex_cache
    patterns = []
    async for doc in regex_db.find({"pattern": {"$exists": True}}):
        try:
            raw = doc.get("raw") or doc.get("pattern", "")
            patterns.append((re.compile(doc["pattern"], re.IGNORECASE), raw))
        except Exception:
            pass
    _regex_cache = patterns
    _regex_cache_ts = now
    return _regex_cache


async def _get_local_patterns(chat_id: int):
    now = time.monotonic()
    hit = _local_regex_cache.get(chat_id)
    if hit and (now - hit[1]) < REGEX_TTL:
        return hit[0]
    patterns = []
    async for doc in group_regex_db.find({"chat_id": chat_id}):
        try:
            raw = doc.get("raw") or doc.get("pattern", "")
            patterns.append((re.compile(doc["pattern"], re.IGNORECASE), raw))
        except Exception:
            pass
    _local_regex_cache[chat_id] = (patterns, now)
    return patterns


def invalidate_local_regex_cache(chat_id: int) -> None:
    _local_regex_cache.pop(chat_id, None)


async def _is_external_mention(
    client: Client, message, cfg: dict, found: "asyncio.Event | None" = None,
) -> "tuple[bool, str | None, str | None]":
    """
    Deteksi apakah pesan mengandung mention yang dilarang di grup ini.

    CATATAN: fungsi ini SUDAH TIDAK dipanggil dari pipeline otomatis
    (core/antispam_queue.py::_process_detection) — jalur otomatis sekarang
    HANYA memakai _is_external_mention_cache_only() (cache-only, tanpa API).
    Fungsi penuh (pakai API) ini masih dipertahankan sebagai utilitas untuk
    pengecekan manual 1 pesan (mis. plugins/commands/log.py).

    Urutan cek per mention (@username):
      0. Whitelist grup → skip seluruh cek, lanjut entity berikutnya
      1. Global channel/grup cache → cek toggle batasi_channel / batasi_grup
      2. Cache-miss murni →
         a. Kalau gate LAIN di pesan yang SAMA (mis. regex) sudah lebih
            dulu menandai pesan ini spam (`found` event sudah di-set) →
            pesan ini bakal dihapus lewat gate itu juga, jadi TIDAK perlu
            API buat mention ini. Cukup titip username ke database khusus
            (mention_pending_resolve) untuk diresolusi belakangan.
         b. Kalau belum, dan ini bukan mention cache-miss PERTAMA di pesan
            ini (kasus multi-mention) → juga dititip ke database khusus,
            supaya tidak nembak API berkali-kali berurutan untuk 1 pesan.
         c. Baru mention cache-miss PERTAMA (dan `found` belum di-set) yang
            benar-benar nembak API real-time (single-flight — kalau
            username yang SAMA kebetulan lagi diresolusi lorong/pesan lain,
            cukup numpang tunggu hasilnya, TIDAK menembak API dua kali).

    `found` (opsional, default None) — Event yang di-share ke gate lain yang
    mengecek pesan yang sama, dipakai kalau caller punya konteks race
    antar-gate. Caller saat ini (mis. plugins/commands/log.py, pengecekan
    manual 1 pesan) tidak punya konteks itu jadi cukup panggil tanpa `found`.

    Toggle sub-fitur (dari cfg grup):
      mention_batasi_channel — batasi mention ke channel (default False)
      mention_batasi_grup    — batasi mention ke grup/supergroup (default False)

    Whitelist berlaku untuk semua jenis entity — tidak per-jenis.

    Return SELALU 3-tuple (is_external, kind, username). Caller wajib unpack
    3 nilai.
    """
    if not message.entities:
        return False, None, None

    # Ambil sub-toggle dari config grup
    batasi_channel = cfg.get("mention_batasi_channel", False)
    batasi_grup    = cfg.get("mention_batasi_grup",    False)

    # Kalau semua sub-toggle OFF → tidak ada yang perlu dicek
    if not batasi_channel and not batasi_grup:
        return False, None, None

    msg_text = message.text or message.caption or ""
    cid = message.chat.id

    # Ambil whitelist sekali di luar loop
    from database import (
        mention_cache_get_by_uid, mention_cache_get_by_username,
        mention_cache_refresh_ttl, mention_cache_set,
        mention_global_get, mention_global_set,
        mention_wl_get, mention_pending_add,
    )
    whitelist = set(await mention_wl_get(cid))

    try:
        from monitor_bot_reference import check_member_via_monitor
        _monitor_available = True
    except Exception:
        _monitor_available = False

    # Sudah pakai jatah 1 API call real-time untuk PESAN ini? (multi-mention)
    api_call_used_this_message = False

    for entity in message.entities:
        # Hanya proses @username mention biasa.
        # TEXT_MENTION di-skip — hanya bisa dilakukan ke member aktif.
        if entity.type != MessageEntityType.MENTION:
            continue

        uname = msg_text[entity.offset:entity.offset + entity.length].lstrip("@").lower()
        if not uname:
            continue

        # Skip username sistem Telegram
        if uname in ("botfather", "telegram", "admin"):
            continue

        # ── 0. Whitelist grup ────────────────────────────────────────────────
        if uname in whitelist:
            continue

        # ── 1. Global channel/grup cache ─────────────────────────────────────
        global_doc = await mention_global_get(uname)
        if global_doc is not None:
            kind = global_doc.get("kind")
            if kind == "channel" and batasi_channel:
                return True, "channel", uname
            elif kind == "grup" and batasi_grup:
                return True, "grup", uname
            elif kind in ("non_akun", "channel", "grup"):
                continue  # akun biasa, atau toggle terkait OFF → skip
            # kind lain → fall through ke API

        # ── 2. Per-grup member cache (akun biasa — sudah diketahui bukan
        #      channel/grup, tidak ada lagi yang perlu dicek) ────────────────
        cached = await mention_cache_get_by_username(cid, uname)
        if cached is not None:
            asyncio.create_task(mention_cache_refresh_ttl(cid, username=uname))
            continue

        # ── 3. Cache-miss murni — belum ketahuan jenisnya lewat cache manapun ──

        # 3a. Gate LAIN di pesan yang SAMA (regex/ubot/cas/dst) sudah lebih
        #     dulu menandai pesan ini spam → pesan ini pasti kehapus lewat
        #     gate itu juga. Nembak API cuma buat nentuin jenis mention
        #     (yang hasilnya toh tidak dipakai lagi) itu buang-buang waktu
        #     & kuota API. Titip ke database khusus, keluar total.
        if found is not None and found.is_set():
            asyncio.create_task(mention_pending_add(uname, cid))
            return False, None, None

        # 3b. Bukan mention cache-miss pertama di pesan ini (multi-mention)
        #     → sisanya dititip juga, tidak ikut menembak API berurutan.
        if api_call_used_this_message:
            asyncio.create_task(mention_pending_add(uname, cid))
            continue

        # 3c. Baru di sini benar-benar nembak API (single-flight — kalau
        #     username yang SAMA kebetulan lagi diresolusi lorong/pesan
        #     lain, cukup numpang tunggu, tidak nembak API dua kali).
        #
        # Sejak toggle "Batasi Tag Akun" dihapus, fungsi ini hanya perlu
        # membedakan CHANNEL vs GRUP/SUPERGROUP publik lewat get_chat().
        # Akun pribadi (member atau bukan) tidak lagi ditindak sama sekali —
        # kalau get_chat_member berhasil menunjukkan ini akun biasa, cukup
        # skip ke entity berikutnya tanpa apa pun.
        api_call_used_this_message = True
        try:
            from pyrogram.enums import ChatType

            try:
                member = await client.get_chat_member(cid, uname)
                is_member = _resolve_is_member(member)
                uid_obj = member.user.id if member.user else None
                if uid_obj is not None:
                    asyncio.create_task(mention_cache_set(cid, uid_obj, is_member, username=uname))
                continue  # sudah jelas akun biasa → entity berikutnya

            except UserNotParticipant:
                # Akun asli, cuma bukan member grup ini — tidak ditindak.
                continue

            except (PeerIdInvalid, RPCError, KeyError, ValueError):
                # Ambigu lewat get_chat_member: BISA berarti target ini
                # channel/grup (bukan user, jadi get_chat_member memang
                # tidak berlaku), BISA JUGA gagal resolve teknis. Lanjut ke
                # fallback get_chat() di bawah untuk membedakan.
                pass

            try:
                chat_obj = await _resolve_chat_singleflight(client, uname)
            except UsernameNotOccupied:
                # Username tidak ada sama sekali — bukan channel/grup, tidak
                # perlu ditindak (toggle akun sudah dihapus).
                continue
            except (PeerIdInvalid, RPCError, KeyError, ValueError):
                # Masih gagal resolve juga. Titip ke antrian resolve tunda,
                # JANGAN hapus pesan untuk mention ini.
                asyncio.create_task(mention_pending_add(uname, cid))
                continue

            chat_type = chat_obj.type
            if chat_type == ChatType.CHANNEL:
                asyncio.create_task(mention_global_set(uname, "channel"))
                if batasi_channel:
                    return True, "channel", uname

            elif chat_type in (ChatType.GROUP, ChatType.SUPERGROUP):
                asyncio.create_task(mention_global_set(uname, "grup"))
                if batasi_grup:
                    return True, "grup", uname

            # chat_type == PRIVATE → akun biasa, tidak ditindak.

        except Exception:
            pass

    return False, None, None

async def _is_external_mention_cache_only(client: Client, message, cfg: dict) -> "tuple[bool, str | None, str | None]":
    """
    Satu-satunya jalur cek mention — HANYA membaca whitelist +
    mention_global_cache + mention_member_cache (MongoDB). TIDAK PERNAH
    memanggil Telegram API (tidak ada client.get_chat / get_chat_member
    sama sekali) — jadi aman dijalankan PARALEL bersama gate A-D/F (lihat
    core/antispam_queue.py::_gate_mention_cache) tanpa risiko flood-wait
    atau menahan gate lain. Tidak ada lagi jalur sequential (API penuh)
    sesudahnya — mention SELALU diputuskan di sini.

    Per @username:
      • Whitelist / cache HIT (channel, grup, atau member cache)
        → langsung diputuskan di sini.
      • Cache MISS (belum pernah di-resolve sama sekali) → SELALU dianggap
        SPAM SEMENTARA (kind="cache_miss") supaya caller hapus pesan
        LANGSUNG tanpa menunggu API sama sekali (lihat komentar di
        core/antispam_queue.py::_gate_mention_cache untuk penjelasan
        lengkap trade-off-nya: @username BARU yang sebenarnya bersih bisa
        kehapus di kemunculan PERTAMA, baru "selamat" di kemunculan
        berikutnya setelah mention_pending_resolve_loop (database.py)
        sempat mengkategorikannya + memperbarui cache di belakang layar).

    Kalau toggle "Batasi Tag Akun Promosi" ON (cfg["mention_batasi_akun"]),
    akun biasa (bukan channel/grup) ikut dinilai lewat kind tambahan:
      • "non_akun"    — username sudah PASTI tidak valid (global cache).
      • "bio_grup"    — bio-nya kedapatan promosi grup lain, TIDAK PEDULI
                         apakah tag target adalah member atau bukan
                         (hasil core/mention_bio_scan.py).
      • "bio_pending" — bio BELUM pernah discan → pesan dihapus SEMENTARA
                         (tidak dicatat permanent), sekalian dititip ke
                         antrian scan bio. (lihat _gate_mention_cache)
    Catatan: "non_member" (akun nyata tapi bukan member) TIDAK lagi
    ditindak — fitur ini bernama "Batasi Tag Akun PROMOSI", bukan
    "Batasi Tag Akun Non-Member". Status keanggotaan tidak relevan,
    hanya bio yang dinilai.

    Return SELALU 3-tuple, sama seperti versi penuh.
    """
    if not message.entities:
        return False, None, None

    batasi_channel = cfg.get("mention_batasi_channel", False)
    batasi_grup    = cfg.get("mention_batasi_grup",    False)
    batasi_akun    = cfg.get("mention_batasi_akun",    False)

    if not batasi_channel and not batasi_grup and not batasi_akun:
        return False, None, None

    msg_text = message.text or message.caption or ""
    cid = message.chat.id

    from database import (
        mention_cache_get_by_username, mention_cache_refresh_ttl,
        mention_global_get, mention_wl_get, mention_bio_scan_get,
    )
    whitelist = set(await mention_wl_get(cid))

    for entity in message.entities:
        if entity.type != MessageEntityType.MENTION:
            continue

        uname = msg_text[entity.offset:entity.offset + entity.length].lstrip("@").lower()
        if not uname:
            continue

        if uname in ("botfather", "telegram", "admin"):
            continue

        # ── 0. Whitelist grup ────────────────────────────────────────────────
        if uname in whitelist:
            continue

        # ── 1. Global channel/grup cache ─────────────────────────────────────
        global_doc = await mention_global_get(uname)
        if global_doc is not None:
            kind = global_doc.get("kind")
            if kind == "channel" and batasi_channel:
                return True, "channel", uname
            elif kind == "grup" and batasi_grup:
                return True, "grup", uname
            elif kind == "non_akun" and batasi_akun:
                # Username sudah PASTI tidak valid (bukan sekadar gagal
                # sementara) — bagian "non valid" dari Batasi Tag Akun
                # Promosi. Cache ini sendiri sudah ber-TTL pendek
                # (MENTION_GLOBAL_NON_AKUN_TTL), jadi otomatis dicoba lagi
                # nanti kalau ternyata username itu didaftarkan ulang.
                return True, "non_akun", uname
            elif kind in ("non_akun", "channel", "grup"):
                continue  # akun biasa, atau toggle terkait OFF → skip, tidak perlu API
            # kind lain (tak dikenal) → fall through, cek per-grup cache di bawah

        # ── 2. Per-grup member cache ──────────────────────────────────────────
        cached = await mention_cache_get_by_username(cid, uname)
        if cached is not None:
            asyncio.create_task(mention_cache_refresh_ttl(cid, username=uname))

            if batasi_akun:
                # Tidak peduli member atau bukan — "Batasi Tag Akun Promosi"
                # hanya menindak akun dengan bio promosi grup lain.
                # Status keanggotaan (cached True/False) tidak relevan.
                bio_flag = await mention_bio_scan_get(uname)
                if bio_flag is True:
                    return True, "bio_grup", uname
                if bio_flag is None:
                    # Bio belum pernah discan sama sekali — hapus SEMENTARA
                    # (tidak dicatat sebagai pelanggaran permanen), sekalian
                    # titipkan ke antrian scan bio (lihat _gate_mention_cache).
                    # Kemunculan berikutnya setelah scan selesai sudah kena
                    # cache — kalau bersih, aman (TTL 7 hari).
                    return True, "bio_pending", uname
                # bio_flag is False → bio bersih, lewatkan.

            continue

        # ── Cache MISS total ──────────────────────────────────────────────────
        # Belum pernah di-resolve sama sekali — SELALU anggap spam SEMENTARA,
        # biar caller (_gate_mention_cache) hapus pesan TANPA menunggu API.
        # Resolusi jenis sebenarnya (channel/grup/akun) + status keanggotaan
        # per grup dilakukan pelan-pelan di belakang layar oleh
        # mention_pending_resolve_loop (database.py) — di sini murni baca
        # cache, tidak boleh sentuh API sama sekali.
        return True, "cache_miss", uname

    return False, None, None



def _resolve_is_member(member) -> bool:
    """
    FIX BUG UTAMA "tag akun asli non-member tidak dihapus":

      `client.get_chat_member(cid, uid)` BISA berhasil (tidak raise
      UserNotParticipant) walau user itu SUDAH BUKAN member grup lagi.
      Khusus supergroup, Telegram tetap menyimpan record lama untuk user
      yang PERNAH tercatat, dan get_chat_member mengembalikan objek
      ChatMember dengan status LEFT/BANNED alih-alih melempar error.
      UserNotParticipant HANYA muncul untuk user yang memang belum pernah
      tercatat di grup itu sama sekali.

      Kode lama menyimpulkan `is_member = member is not None`, yang SELALU
      True selama tidak exception — jadi user asli yang sudah keluar tapi
      pernah jadi member, begitu di-tag pertama kali (cache miss), langsung
      ter-cache is_member=True secara permanen (dan makin awet tiap kali
      di-tag lagi karena refresh_ttl). Tag ke dia tidak pernah dihapus.

      Fix: cek member.status secara eksplisit.
        - LEFT / BANNED           → jelas bukan member.
        - RESTRICTED              → ambigu: bisa berarti masih member tapi
          dibatasi (mis. kena mute dari bot ini sendiri), BISA JUGA berarti
          sudah dikick dengan restriksi. Pyrogram expose field `is_member`
          pada objek ini persis untuk membedakan dua kasus tsb — dipakai
          di sini, bukan diasumsikan salah satu.
        - MEMBER / ADMINISTRATOR / OWNER → member asli.
    """
    if member is None:
        return False
    status = member.status
    if status in (ChatMemberStatus.LEFT, ChatMemberStatus.BANNED):
        return False
    if status == ChatMemberStatus.RESTRICTED:
        return bool(getattr(member, "is_member", True))
    return True  # MEMBER, ADMINISTRATOR, OWNER


# ─────────────────────────────────────────────────────────────────────────────
#  Main filter (group=2) — FRONT HANDLER, murni RAM tanpa I/O
#  (admin/VIP/RAM-flood sekarang dicek DI DALAM lorong masing-masing pesan —
#  lihat core/antispam_queue.py::_process_detection & _resolve_bypass)
# ─────────────────────────────────────────────────────────────────────────────
@Client.on_chat_member_updated(group=6)
async def mention_cache_track_leave(client: Client, update: ChatMemberUpdated):
    """
    BUG "Batasi Tag Akun tidak hapus tag akun asli yang sudah bukan member":

      mention_cache_remove_member() SEBELUMNYA hanya dipanggil dari bot
      pemantau Security OS (security_os/monitor_bot_reference.py::_on_join)
      — fitur TERPISAH & OPSIONAL. Grup yang belum/tidak pasang Security OS
      tidak pernah menerima event ini sama sekali, jadi entry is_member=True
      di cache mention_member_cache tidak pernah diperbarui setelah user
      benar-benar keluar/di-kick dari grup.

      Ini diperparah oleh mention_cache_refresh_ttl(): setiap ada mention ke
      username itu lagi (cache HIT), TTL entry LAMA (is_member=True) cuma
      diperpanjang lagi tanpa verifikasi ulang — jadi makin sering di-tag,
      makin awet cache salahnya, dan tag ke akun asli yang sudah bukan
      member TIDAK PERNAH terhapus.

      Fix: pasang listener yang sama di BOT UTAMA (selalu aktif untuk
      semua grup terdaftar, tidak bergantung Security OS) — begitu
      Telegram kirim ChatMemberUpdated status KELUAR/DIBAN untuk seorang
      member, cache langsung ditandai is_member=False saat itu juga,
      terlepas grup itu pakai Security OS atau tidak.

    TAMBAHAN (menyempurnakan fix di atas):
      1. Status RESTRICTED tidak lagi selalu dianggap "keluar". RESTRICTED
         itu ambigu — bisa berarti user masih member tapi dibatasi (mis.
         kena mute dari bot ini sendiri lewat check_and_punish), BISA JUGA
         berarti sudah dikick dengan restriksi. Dipakai field `is_member`
         bawaan Telegram/Pyrogram pada objek ini untuk membedakannya —
         sama seperti _resolve_is_member() di atas — supaya member yang
         cuma di-mute TIDAK ikut ke-cache sebagai non-member (yang tadinya
         bisa bikin tag ke member asli malah ikut kehapus).
      2. Ditambah arah sebaliknya: kalau user REJOIN (status kembali jadi
         MEMBER/ADMINISTRATOR/OWNER, atau RESTRICTED tapi is_member=True),
         cache di-update balik jadi is_member=True. Tanpa ini, user yang
         sempat keluar (ke-cache False) lalu join lagi, tag ke dia akan
         terus salah kehapus walau dia sekarang sudah member asli lagi.
    """
    try:
        new_member = update.new_chat_member
        if not new_member or not new_member.user or new_member.user.is_bot:
            return

        chat_id = update.chat.id
        user_id = new_member.user.id
        status = new_member.status

        if status in (ChatMemberStatus.LEFT, ChatMemberStatus.BANNED):
            from database import mention_cache_remove_member
            await mention_cache_remove_member(chat_id, user_id)
            return

        if status == ChatMemberStatus.RESTRICTED:
            still_member = bool(getattr(new_member, "is_member", True))
            if not still_member:
                from database import mention_cache_remove_member
                await mention_cache_remove_member(chat_id, user_id)
            else:
                from database import mention_cache_set
                await mention_cache_set(chat_id, user_id, True, username=new_member.user.username)
            return

        if status in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
            from database import mention_cache_set
            await mention_cache_set(chat_id, user_id, True, username=new_member.user.username)
    except Exception as e:
        print(f"[MentionCache] gagal invalidasi saat member berubah status: {e}")


@Client.on_message((filters.group | filters.forum) & ~filters.service, group=1)
async def _auto_register_group_lapis2(client, message):
    """
    LAPIS 2 pemulihan config_db — jaring pengaman UTAMA (lihat docstring
    lengkap di database.py::ensure_group_registered dan
    bootstrap_groups_from_dialogs untuk konteks kenapa ini perlu ada).

    Berbeda dari main_antispam_filter (group=2) yang skip pesan
    kosong/command, handler ini SENGAJA jalan untuk SEMUA jenis pesan grup
    (teks, media, command, apa pun — selama bukan pesan service) supaya
    cakupannya seluas mungkin: begitu ADA 1 pesan APA PUN dari member
    mana pun di grup manapun, grup itu otomatis terdaftar/disegarkan ke
    config_db. Throttle 1x/jam per grup sudah ditangani di dalam
    ensure_group_registered() sendiri — pemanggilan di sini SELALU murah
    (cache-hit di luar jam pertama).

    group=1 — sebelum main_antispam_filter (group=2), supaya pendaftaran
    ini tidak pernah ikut ter-skip oleh early-return apa pun di sana.
    """
    try:
        chat = message.chat
        await ensure_group_registered(
            chat.id, chat.title or str(chat.id), chat.username, chat.type.name,
        )
    except Exception as e:
        print(f"[AutoRegister] {e}")


@Client.on_message((filters.group | filters.forum) & ~filters.service, group=2)
async def main_antispam_filter(client, message):
    if not message.from_user:
        return
    cid, mid = message.chat.id, message.id

    if is_message_handled(cid, mid):
        return

    content = (message.text or message.caption or "").strip()
    if not content or content.startswith("/"):
        return

    # ── Enqueue ke lorong grup ini — admin/VIP/flood-RAM/gate deteksi semua
    #    dicek di dalam sana, per-pesan, per-lorong. ─────────────────────────
    from core.antispam_queue import enqueue_for_detection
    berhasil_enqueue = await enqueue_for_detection(client, message)
    if berhasil_enqueue:
        # BUG FIX (race consec_spam): tandai SINKRON di sini — sebelum
        # group=10 (_clean_message_tracker) sempat jalan — supaya dia tahu
        # pesan ini masih akan dievaluasi async oleh _process_detection()
        # dan TIDAK menganggapnya "bersih" duluan (lihat
        # database.py::mark_message_queued untuk detail race yang diperbaiki).
        mark_message_queued(cid, mid)


async def check_ram_flood_protections(client, message, cfg: dict, cid: int, uid: int, mid: int, content: str) -> bool:
    """
    Proteksi A/B/C — instant-delete berbasis RAM (tanpa I/O DB), dipanggil
    dari DALAM lorong (core/antispam_queue.py::_process_detection), SETELAH
    Fase 0 (resolusi bypass admin/VIP) DAN cek izin bot selesai — jadi aman,
    tidak akan kena admin/VIP, dan caller sudah pastikan bot punya izin
    hapus sebelum memanggil fungsi ini.

    Dikontrol oleh SATU toggle tunggal "anti_flood" (default ON, tombol
    baru di panel pengaturan grup) — SUDAH TIDAK tunduk pada toggle
    "global" (anti-gcast, jalur DB di Gate D core/antispam_queue.py) maupun
    "local" (anti-duplikat lokal fuzzy-match, jalur DB di Gate C). Proteksi
    A/B/C ini murni RAM instan dan independen sepenuhnya dari kedua fitur
    itu — admin bisa matikan anti-gcast/anti-duplikat-lokal tanpa ikut
    mematikan proteksi flood RAM, atau sebaliknya.

    FIX: sebelumnya proteksi ini memanggil message.delete() LANGSUNG
    (asyncio.create_task terpisah per pesan) — beda jalur dari 5 gate lain
    yang semuanya lewat delete_queue/delete_worker (database.py). Saat
    banyak kelompok teks berbeda ke-flag nyaris bersamaan (mis. serangan
    multi-pola), banyak panggilan delete() individual bisa nembak API
    Telegram tanpa rate-limit floor sama sekali — berisiko FloodWait.
    Sekarang SEMUA hapus dari proteksi ini juga masuk delete_queue,
    di-batch & dijadwalkan oleh delete_worker seperti gate lain — konsisten
    & aman dari FloodWait untuk seluruh jalur hapus pesan di bot ini.

    Return True jika salah satu proteksi menangani pesan ini (sudah
    dititip-hapus + dihukum) — caller (_process_detection) harus berhenti
    di situ, tidak lanjut ke gate deteksi lain.
    """
    anti_flood_on = cfg.get("anti_flood", True) is True

    if not anti_flood_on:
        return False

    content_hash = hashlib.md5(content.encode("utf-8", errors="ignore")).hexdigest()
    now_ts = time.time()

    # ── PROTEKSI A: Karantina RAM Sementara (Serangan Massal Banyak Akun) ──────
    if cid in _global_text_blacklist and content_hash in _global_text_blacklist[cid]:
        if now_ts < _global_text_blacklist[cid][content_hash]:
            mark_message_handled(cid, mid)
            asyncio.create_task(check_and_punish(client, message, "MASS_FLOOD_BURST_RAM", content))
            asyncio.create_task(log_mass_flood(client, message, pola=content[:80], indikator="MASS_FLOOD_BURST_RAM"))
            await delete_queue.put((cid, [mid]))
            return True
        else:
            _global_text_blacklist[cid].pop(content_hash, None)

    # ── PROTEKSI B: Deteksi Serangan Massal Banyak Akun Kloning (Lintas User) ──
    if cid not in _global_text_tracker:
        _global_text_tracker[cid] = {}

    if content_hash not in _global_text_tracker[cid]:
        _global_text_tracker[cid][content_hash] = []

    _global_text_tracker[cid][content_hash].append((message, now_ts))

    _global_text_tracker[cid][content_hash] = [
        (m, ts) for (m, ts) in _global_text_tracker[cid][content_hash]
        if (now_ts - ts) <= _MASS_BURST_WINDOW
    ]

    burst_msgs = _global_text_tracker[cid][content_hash]

    if len(burst_msgs) >= _MASS_BURST_LIMIT:
        if cid not in _global_text_blacklist:
            _global_text_blacklist[cid] = {}

        _global_text_blacklist[cid][content_hash] = now_ts + _LOCK_DURATION

        # ── RETROAKTIF: bukan cuma pesan pemicu (yang bikin hitungan
        # mencapai limit) yang dihapus+dihukum — SEMUA pesan identik yang
        # masih tercatat dalam window ini (termasuk pesan PERTAMA yang
        # tadinya lolos karena saat itu hitungannya belum sampai limit)
        # ikut disapu bersih. Ini berlaku sama saja mau dari 1 akun yang
        # ngebut kirim ulang, atau beberapa akun berbeda kirim bareng.
        mids_to_delete: list[int] = []
        for old_msg, _ts in burst_msgs:
            old_cid = old_msg.chat.id
            old_mid = old_msg.id
            if is_message_handled(old_cid, old_mid):
                continue
            mark_message_handled(old_cid, old_mid)
            mids_to_delete.append(old_mid)
            old_content = (old_msg.text or old_msg.caption or content)
            asyncio.create_task(check_and_punish(client, old_msg, "MASS_FLOOD_BURST_RAM", old_content))

        asyncio.create_task(log_mass_flood(
            client, message, pola=content[:80], indikator="MASS_FLOOD_BURST_RAM",
        ))
        if mids_to_delete:
            await delete_queue.put((cid, mids_to_delete))

        # Reset tracker hash ini — kejadian sudah ditangani tuntas, biar
        # tidak ada mid basi yang nyangkut kalau hash yang sama muncul lagi
        # nanti setelah blacklist ini habis masa kuncinya.
        _global_text_tracker[cid][content_hash] = []
        return True

    # ── PROTEKSI C: Deteksi Duplikasi Tunggal Per-User ────────────────────────
    if cid not in _local_flood_cache:
        _local_flood_cache[cid] = {}

    user_flood_data = _local_flood_cache[cid].get(uid)

    if user_flood_data:
        last_hash, last_time, duplicate_count = user_flood_data

        if last_hash == content_hash and (now_ts - last_time) < _FLOOD_WINDOW:
            duplicate_count += 1
            _local_flood_cache[cid][uid] = (content_hash, now_ts, duplicate_count)

            if duplicate_count >= _MAX_DUPLICATE:
                mark_message_handled(cid, mid)
                asyncio.create_task(check_and_punish(client, message, "LOCAL_FLOOD_RAM", content))
                asyncio.create_task(log_duplikat_lokal(client, message, pola=content[:80], indikator="LOCAL_FLOOD_RAM"))
                await delete_queue.put((cid, [mid]))
                return True
        else:
            _local_flood_cache[cid][uid] = (content_hash, now_ts, 1)
    else:
        _local_flood_cache[cid][uid] = (content_hash, now_ts, 1)

    return False


async def _gcast_punish_other_group(
    client,
    chat_id: int,
    user_id: int,
    konten: str,
) -> None:
    """
    Terapkan eskalasi hukuman untuk grup LAIN (bukan grup asal deteksi) yang
    ikut kena pesan gcast yang sama. Reimplementasi terpisah dari
    check_and_punish() (core/punishment.py) karena tidak ada objek `message`
    untuk grup lain di sini (pesannya tidak pernah masuk ke grup ini).

    FIX: sebelumnya fungsi ini SELALU mute, tidak pernah cek
    cfg.get("punishment_mode") grup ini — jadi grup yang panelnya sudah
    di-set Mode Hukuman "Ban" tetap cuma dapat mute kalau user-nya kena
    lewat jalur gcast-ke-grup-lain ini (bukan grup asal pesan pertama kali
    terdeteksi). Sekarang dicek dulu sama seperti check_and_punish(), dan
    cabang ke ban permanen kalau memang di-set begitu.
    """
    from database import (
        get_local_mute, increment_local_spam, apply_local_mute,
        revert_failed_local_mute, insert_group_action_log, get_config,
    )
    from core.punishment import SPAM_MUTE_THRESHOLD
    from core.moderation_queue import queue_mute, queue_ban
    import time as _time
    now_ts = _time.time()
    mute_rec = await get_local_mute(chat_id, user_id)
    if mute_rec.get("muted_until", 0.0) > now_ts:
        return
    updated = await increment_local_spam(chat_id, user_id)
    consec  = updated.get("consec_spam", 1)
    if consec < SPAM_MUTE_THRESHOLD:
        return

    cfg = await get_config(chat_id)
    if cfg.get("punishment_mode", "mute") == "ban":
        async def _on_ban_done(success: bool):
            if not success:
                return
            try:
                from core.violation_types import VIOLATION_BAN_ESKALASI
                await insert_group_action_log(
                    chat_id, "BAN",
                    f"Ban permanen — {SPAM_MUTE_THRESHOLD}x pelanggaran berulang "
                    f"(apapun jenisnya, Mode Hukuman: Ban)",
                    user_id, str(user_id), konten,
                    jenis=VIOLATION_BAN_ESKALASI,
                )
            except Exception:
                pass

        queue_ban(chat_id, user_id, on_done=_on_ban_done)
        return

    duration_secs, level_before = await apply_local_mute(chat_id, user_id)
    duration_min = duration_secs // 60

    async def _on_done(success: bool):
        if not success:
            await revert_failed_local_mute(chat_id, user_id, level_before)
            return
        try:
            from core.violation_types import VIOLATION_MUTE_ESKALASI
            await insert_group_action_log(
                chat_id, "MUTE",
                f"Mute {duration_min} mnt — {SPAM_MUTE_THRESHOLD}x pelanggaran berulang (apapun jenisnya)",
                user_id, str(user_id), konten,
                jenis=VIOLATION_MUTE_ESKALASI,
            )
        except Exception:
            pass

    queue_mute(chat_id, user_id, duration_secs, on_done=_on_done)


# ─────────────────────────────────────────────────────────────────────────────
#  group=10 — Tracker pesan bersih
# ─────────────────────────────────────────────────────────────────────────────
@Client.on_message((filters.group | filters.forum) & ~filters.service, group=10)
async def _clean_message_tracker(client, message):
    if not message.from_user or message.from_user.is_bot:
        return
    cid = message.chat.id
    mid = message.id
    uid = message.from_user.id

    if is_message_handled(cid, mid):
        # Sudah diklaim spam oleh gate sinkron (bio/sticker_guard/CAS, dsb,
        # yang mark_message_handled() langsung di handler group < 10) — jelas
        # bukan pesan bersih.
        return
    if is_message_queued(cid, mid):
        # BUG FIX (race consec_spam): pesan ini baru saja di-enqueue ke
        # pipeline deteksi async (_process_detection) dan BELUM tentu
        # selesai dievaluasi — jangan ambil keputusan "bersih" di sini.
        # _process_detection() sendiri yang akan reset_local_mute() di
        # akhir pipeline KALAU memang tidak ada gate yang match — itu
        # satu-satunya titik yang tahu hasil final tanpa race.
        return
    asyncio.create_task(_reset_mute_async(cid, uid))


async def _reset_mute_async(chat_id: int, user_id: int) -> None:
    try:
        await reset_local_mute(chat_id, user_id)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
#  RAM cache janitor — FIX MEMORY LEAK
# ─────────────────────────────────────────────────────────────────────────────
# _local_flood_cache (Proteksi C, per-user duplikat) dan _global_text_tracker /
# _global_text_blacklist (Proteksi A/B, mass-burst) TIDAK PERNAH dihapus
# sebelumnya — entry per (chat_id, user_id) atau per content_hash menumpuk
# SELAMANYA sejak proses start, walau data itu sudah tidak relevan sama
# sekali cuma dalam hitungan detik (_FLOOD_WINDOW=5s, _LOCK_DURATION=10s).
#
# Di grup rame dengan banyak user unik (bukan banyak pesan/detik — tapi
# banyak ORANG BERBEDA yang pernah kirim pesan sejak bot terakhir restart),
# ini jadi memory leak murni: RAM naik terus, tidak pernah turun, tidak
# proporsional ke traffic real-time.
#
# Janitor ini jalan tiap 5 menit, buang entry yang sudah basi jauh melebihi
# window relevansinya (dengan margin aman) supaya cache ini kembali cuma
# berisi user/teks yang BENAR-BENAR masih dalam jendela deteksi aktif.
_JANITOR_INTERVAL = 300  # detik (5 menit)
_FLOOD_ENTRY_MAX_AGE = max(_FLOOD_WINDOW, 60.0)  # margin aman di atas 5 detik
_TEXT_TRACKER_MAX_AGE = max(_MASS_BURST_WINDOW, 60.0)


async def start_ram_cache_janitor() -> None:
    """
    Background task: bersihkan _local_flood_cache, _global_text_tracker, dan
    _global_text_blacklist dari entry basi setiap _JANITOR_INTERVAL detik.
    Panggil sekali dari main.py main() dengan asyncio.create_task()
    (pola sama seperti admin_session.start_cleanup_task()).
    """
    while True:
        await asyncio.sleep(_JANITOR_INTERVAL)
        now_ts = time.time()
        removed_flood = 0
        removed_tracker = 0
        removed_blacklist = 0

        # ── 1. _local_flood_cache: dict[cid][uid] = (hash, ts, count) ───────
        for cid in list(_local_flood_cache.keys()):
            per_user = _local_flood_cache[cid]
            for uid in list(per_user.keys()):
                _, entry_ts, _ = per_user[uid]
                if now_ts - entry_ts > _FLOOD_ENTRY_MAX_AGE:
                    per_user.pop(uid, None)
                    removed_flood += 1
            if not per_user:
                _local_flood_cache.pop(cid, None)

        # ── 2. _global_text_tracker: dict[cid][hash] = [(message, ts), ...] ─
        for cid in list(_global_text_tracker.keys()):
            per_hash = _global_text_tracker[cid]
            for content_hash in list(per_hash.keys()):
                entries = [
                    (m, ts) for (m, ts) in per_hash[content_hash]
                    if now_ts - ts <= _TEXT_TRACKER_MAX_AGE
                ]
                if entries:
                    per_hash[content_hash] = entries
                else:
                    per_hash.pop(content_hash, None)
                    removed_tracker += 1
            if not per_hash:
                _global_text_tracker.pop(cid, None)

        # ── 3. _global_text_blacklist: dict[cid][hash] = expiry_epoch ──────
        for cid in list(_global_text_blacklist.keys()):
            per_hash = _global_text_blacklist[cid]
            for content_hash in list(per_hash.keys()):
                if now_ts >= per_hash[content_hash]:
                    per_hash.pop(content_hash, None)
                    removed_blacklist += 1
            if not per_hash:
                _global_text_blacklist.pop(cid, None)

        # ── 4. Cache lain di seluruh proyek dengan pola sama (keyed per user,
        #      tidak pernah di-pop, cek TTL cuma saat dibaca) ────────────────
        removed_others = 0
        for modpath, funcname in (
            ("core.antispam_queue", "_sweep_vip_free_cache"),
            ("plugins.filters.bio", "_sweep_bio_caches"),
            ("plugins.filters.cas", "_sweep_cas_cache"),
            ("core.ns_bio_guard", "_sweep_recent_unadmin"),
            ("plugins.commands.antigcast_group", "_sweep_antigcast_cooldowns"),
            ("plugins.commands.govip", "_sweep_govip_cooldowns"),
            ("core.dm_peer_cache", "sweep"),
        ):
            try:
                import importlib
                mod = importlib.import_module(modpath)
                removed_others += getattr(mod, funcname)()
            except Exception:
                pass

        if removed_flood or removed_tracker or removed_blacklist or removed_others:
            print(
                f"[antispam.janitor] cleanup: {removed_flood} flood-entry, "
                f"{removed_tracker} text-tracker, {removed_blacklist} blacklist, "
                f"{removed_others} entry cache-lain dihapus."
            )
