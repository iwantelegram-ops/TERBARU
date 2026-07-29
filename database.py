"""
database.py — Multi-Backend Database Engine
─────────────────────────────────────────────
Otomatis memilih backend terbaik yang tersedia:

  Prioritas:
    1. MONGO_URL di .env  → MongoDB (via pymongo async)
    2. SQLITE_PATH di .env → SQLite lokal (default Termux)

  Saat startup, bot mencoba koneksi MongoDB terlebih dahulu.
  Jika gagal (URL tidak ada / error jaringan / auth gagal),
  otomatis fallback ke SQLite tanpa crash.

  Log backend yang aktif muncul di terminal Termux saat start.

  Semua Collection API identik di kedua backend sehingga
  TIDAK ADA file lain yang perlu diubah.
"""

from __future__ import annotations

import os
import json
import time
import uuid
import asyncio
import aiosqlite
from datetime import datetime, timedelta, timezone
from pyrogram.enums import ChatMemberStatus
from dotenv import load_dotenv
from pathlib import Path as _Path
from pymongo.errors import DuplicateKeyError
from pymongo import ReturnDocument
from pymongo import timeout as _mongo_op_timeout

from core import mongo_shard as _shard

# Cari .env relatif ke file ini, bukan CWD — aman dijalankan dari direktori manapun
load_dotenv(dotenv_path=_Path(__file__).parent / ".env", override=False)

# ── CODE_BOT: namespace isolasi database per-bot ─────────────────────────────
# Semua nama collection akan di-prefix dengan CODE_BOT.
# Dua bot dengan CODE_BOT sama → pakai database yang sama (berbagi data).
# Dua bot dengan CODE_BOT beda di MongoDB/SQLite yang sama → koleksi terpisah, tidak campur.
# Jika CODE_BOT kosong → nama collection tidak di-prefix (perilaku lama).
import re as _re
_CODE_BOT_RAW = os.environ.get("CODE_BOT", "").strip()
_CODE_BOT     = _re.sub(r"[^a-zA-Z0-9]", "_", _CODE_BOT_RAW).lower().strip("_") if _CODE_BOT_RAW else ""

def _ns(name: str) -> str:
    """Tambahkan CODE_BOT prefix ke nama collection.
    Contoh: CODE_BOT=mybot → 'nexus_kalimat' jadi 'mybot_nexus_kalimat'
    """
    return f"{_CODE_BOT}_{name}" if _CODE_BOT else name


# ══════════════════════════════════════════════════════════════════════════════
# KONFIGURASI
# ══════════════════════════════════════════════════════════════════════════════

MONGO_URL            = os.environ.get("MONGO_URL", "").strip()

# ── Data directory: selalu di home user, berdasarkan CODE_BOT ─────────────────
# Format: ~/.nexusai/<CODE_BOT>/
# Dengan ini data SELALU ditemukan dari direktori manapun bot dijalankan,
# dan CODE_BOT yang sama selalu mengakses data yang sama.
_BOT_KEY    = _CODE_BOT if _CODE_BOT else "_default"
_DATA_DIR   = _Path.home() / ".nexusai" / _BOT_KEY
_DATA_DIR.mkdir(parents=True, exist_ok=True)

# MONGO_DB_NAME: dari .env jika diset, fallback ke "nexusai_<CODE_BOT>"
# Sehingga dua CODE_BOT berbeda otomatis pakai database MongoDB berbeda
_MONGO_DB_DEFAULT = f"nexusai_{_BOT_KEY}"
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "").strip() or _MONGO_DB_DEFAULT

# SQLITE_PATH: dari .env jika diset (harus path absolut),
# fallback ke ~/.nexusai/<CODE_BOT>/nexus_bot.db — selalu ditemukan
_SQLITE_DEFAULT = str(_DATA_DIR / "nexus_bot.db")
_SQLITE_ENV     = os.environ.get("SQLITE_PATH", "").strip()
SQLITE_PATH     = _SQLITE_ENV if _SQLITE_ENV else _SQLITE_DEFAULT

GLOBAL_EXPIRY        = 15
DEFAULT_LOCAL_EXPIRY = 3600
TZ_WIB               = timezone(timedelta(hours=7))

# SPAM_MUTE_THRESHOLD: jumlah pelanggaran BERTURUT-TURUT sebelum mute/ban
# eskalasi diterapkan (lihat core/punishment.py::check_and_punish). Bisa
# di-custom lewat .env — SUMBER KEBENARAN TUNGGAL, dipakai juga oleh
# core/punishment.py & plugins/filters/antispam.py (import dari sini,
# bukan didefinisikan ulang) supaya tidak pernah ada 2 nilai berbeda.
# Dibatasi minimum 1 supaya tidak ada nilai config yang membuat sistem
# mute/ban langsung di percobaan ke-0 (nonsensical).
try:
    SPAM_MUTE_THRESHOLD = max(1, int(os.environ.get("SPAM_MUTE_THRESHOLD", 10)))
except (TypeError, ValueError):
    SPAM_MUTE_THRESHOLD = 10

DEFAULT_CONFIG = {
    "local":            False,
    "global":           True,
    "anti_flood":       True,  # Proteksi A/B/C RAM instant-delete (mass-burst lintas
                                # akun & duplikat beruntun 1 user) — lihat
                                # plugins/filters/antispam.py::check_ram_flood_protections.
                                # SENGAJA terpisah dari toggle "local" (anti duplikat
                                # lokal fuzzy-match, DB) & "global" (anti-gcast, DB) —
                                # anti_flood murni proteksi RAM instan, tidak bergantung
                                # sama sekali pada dua toggle itu. Default ON.
    "expiry":           DEFAULT_LOCAL_EXPIRY,
    "bio_check":        False,
    "bio_vip_text":     "",   # teks VIP bio — user dengan teks ini di bio = VIP, bebas dari semua filter
    # Flag internal (BUKAN toggle fitur, jangan dirender jadi tombol panel):
    # True jika force_disable_group_moderation() pernah mematikan grup ini
    # karena izin ban/mute hilang. Dibaca sebagai lapis pertahanan KEDUA di
    # _process_detection (core/antispam_queue.py) — supaya kalaupun
    # check_bot_permissions() salah bilang "masih punya izin" (fail-open
    # saat API Telegram error, atau cache 5 menitnya belum ter-invalidate),
    # bot tetap TIDAK menghapus pesan/menghukum selama flag ini True.
    "perm_forced_off":  False,
    # Flag internal (BUKAN toggle fitur, jangan dirender jadi tombol panel):
    # True jika force_disable_bio_mention_features() (core/monitor_watchdog.py)
    # pernah mematikan paksa seluruh sub-fitur "Bio Cek & Mention" karena bot
    # pemantau grup ini offline/belum terpasang/Privacy Mode masih ON DI
    # TENGAH JALAN (bukan cuma dicegah saat mau dinyalakan lewat cb_toggle).
    # TIDAK auto-restore — admin WAJIB klik ON lagi secara manual dari panel
    # setelah bot pemantau benar-benar siap (lihat cb_toggle).
    "monitor_forced_off": False,
    # Flag internal (BUKAN toggle fitur, jangan dirender jadi tombol panel):
    # True jika grup ini PERNAH terbukti confirmed bot punya kuasa penuh
    # (can_delete_messages + can_restrict_members) minimal 1x. Pembeda
    # krusial buat perm_watchdog: grup BARU yang belum sempat dipromosikan
    # admin (has_perms masih False sejak awal) TIDAK boleh kena auto-leave
    # cuma karena belum di-admin-in — beda kasus dari grup yang izinnya
    # SEMPAT ada lalu DICABUT belakangan. Lihat core/perm_watchdog.py.
    "ever_had_ban_perm": False,
    # Counter internal: berapa siklus perm_watchdog BERTURUT-TURUT grup ini
    # kedeteksi TIDAK punya izin ban/mute. Direset ke 0 begitu izin utuh
    # lagi. Dipakai sebagai syarat konfirmasi N siklus sebelum auto-leave
    # dieksekusi, supaya glitch API sesaat (get_chat_member salah/gagal
    # sekali) tidak langsung membuat bot kabur dari grup yang izinnya
    # sebenarnya masih utuh.
    "perm_lost_strikes": 0,
    # ID user yang menambahkan bot ke grup ini (pelaku add di service
    # message new_chat_members). Dipakai perm_watchdog untuk DM notifikasi
    # kalau bot auto-leave karena izin ban dicabut. None kalau tidak
    # tercatat (fallback ke OWNER_ID saat itu).
    "invited_by": None,
    "anti_mention":            True,   # master toggle — otomatis ON karena minimal 1 sub-toggle default True (lihat bawah)
    "mention_batasi_channel":  False,  # batasi mention ke channel — default OFF
    "mention_batasi_grup":     True,   # batasi mention ke grup/supergroup — default ON
    "mention_batasi_akun":     False,  # "Batasi Tag Akun Promosi" — cek akun biasa yang
                                        # ditag: non-valid/non-member dihapus, member yang
                                        # bio-nya promosi grup lain juga dihapus. Default
                                        # OFF (opt-in, sama seperti mention_batasi_channel).
                                        # Lihat core/mention_bio_scan.py & plugins/filters/
                                        # antispam.py::_is_external_mention_cache_only.
    "anti_link":        True,  # toggle link detector (URL/tautan aktif dalam pesan)
    "cas":              False,
    "punishment_mode":  "mute",  # "mute" (default) — Nx pelanggaran berturut (SPAM_MUTE_THRESHOLD,
                                  # custom via .env, default 10) → mute eskalasi (durasi berlipat
                                  # tiap kali terulang setelah masa mute habis).
                                  # "ban" — Nx pelanggaran berturut → LANGSUNG ban permanen dari
                                  # grup itu, bukan mute. Karena ban mengeluarkan user dari grup,
                                  # ini otomatis 1 user × 1 grup × 1x seumur hidup (tidak ada
                                  # eskalasi durasi/level seperti mute — cukup sekali tembak).
                                  # Toggle ini TIDAK memengaruhi notif "kena whitelist" dkk di
                                  # grup — hanya menentukan aksi SAAT ambang tercapai.
    "local_spam_limit": 1,    # berapa pesan terakhir yg diingat untuk cek duplikat lokal (1-5)
    "anti_spam_ai":     True, # Nexus AI murni + auto regex + Trigger AI global aktif/nonaktif per grup (default ON)
    "vip_title_enabled": False, # Title VIP: tag otomatis (setChatMemberTag) untuk SEMUA member
                             # VIP (manual /vip ATAU bio_vip) di grup ini.
    "vip_title":         "",   # Teks tag (maks 16 UTF-16 code unit, batas Telegram) yang dipasang
                             # ke setiap Member VIP saat vip_title_enabled=True. Kosong = fitur
                             # tidak memasang tag apapun walau enabled=True (sama seperti
                             # auto_title_names kosong di NewsCore).
    "ubot_detect":      True, # Deteksi Ubot: rekam kalimat per-user, tandai
                              # perilaku ubot kalau semua variasi kalimat user
                              # itu sudah terkirim \u22653x tanpa ada kalimat baru.
                              # Rekaman kalimat berjalan terus selama minimal 1
                              # fitur bot ON di grup ini, terlepas status toggle
                              # fitur ini sendiri (lihat core/ubot_detect.py).
    "welcome_enabled":   False, # Welcome member baru dikirim oleh bot pembantu
                              # (bot pemantau Security OS grup ini) — bukan bot
                              # utama, bukan admin. Butuh bot pemantau aktif di
                              # grup ini (lihat monitor_bot_reference.py), kalau
                              # belum ada, fitur tidak mengirim apapun.
    "welcome_delay":     30,   # detik sebelum pesan welcome dihapus otomatis.
    "welcome_text":      "",   # Template custom. Placeholder: {mention} {nama}
                              # {grup}. Kosong = pakai template default.
    "welcome_photo":     "",   # file_id foto welcome — HARUS file_id milik bot
                              # PEMBANTU grup ini sendiri (lihat catatan di
                              # plugins/ui/handlers_welcome.py: file_id Telegram
                              # terikat ke bot yang meng-upload, tidak bisa
                              # dipakai lintas-bot). Kosong = tanpa foto.
    "welcome_buttons":   [],  # list[{"text": str, "url": str}] — tombol URL
                              # di bawah pesan welcome, 1 tombol per baris.
}

# ── In-memory cache ────────────────────────────────────────────────────────────
_config_cache: dict[int, tuple[dict, float]] = {}
_admin_cache:  dict[tuple, tuple[bool, float]] = {}
CONFIG_TTL = 10
ADMIN_TTL  = int(os.environ.get("ADMIN_CACHE_TTL", 120))

# ── Panel UI cache (mempercepat tombol DM panel agar tidak query DB tiap klik) ─
_ns_config_cache:   dict[int, tuple[dict, float]]  = {}  # ns_get_config
_regex_count_cache: dict[int, tuple[int,  float]]  = {}  # count regex per grup
_free_count_cache:  dict[int, tuple[int,  float]]  = {}  # count VIP per grup
_admin_groups_cache: dict[int, tuple[list, float]] = {}  # get_my_admin_groups per user
NS_CONFIG_TTL    = 30   # detik — ns_config jarang berubah
COUNT_TTL        = 30   # detik — count regex/VIP
ADMIN_GROUPS_TTL = 120  # detik — daftar grup admin (2 menit)

# ── Nexus AI panel cache ───────────────────────────────────────────────────────
_nexus_kalimat_count_cache: tuple[tuple[int,int], float] | None = None
_nexus_regex_count_cache:   tuple[int, float]            | None = None
_nexus_wl_count_cache:      tuple[int, float]            | None = None
_nexus_owner_regex_count_cache: tuple[int, float]        | None = None
_nexus_grup_cache:          tuple[list, float]           | None = None
NEXUS_COUNT_TTL = 30   # detik

# v10: batas maksimal jumlah RAW di Record Data (nexus_kalimat) — kalau
# kelampaui, entri TERLAMA otomatis dihapus (FIFO) lewat cascade delete
# yang SUDAH ADA (nexus_delete_kalimat_by_id) — ikut menghapus semua
# varian turunannya di nexus_kalimat_variants_db DAN untrain dari Bayes
# (nexus/ai_core/bayes.py). Lihat nexus_enforce_record_cap() di bawah.
# 0 = nonaktif (tidak ada batas, perilaku lama).
NEXUS_RECORD_MAX = int(os.environ.get("NEXUS_RECORD_MAX", "10000"))

# ── Delete queue ───────────────────────────────────────────────────────────────
# Item: (cid, mids). mids BUKAN daftar kosong wajib berisi pesan yang mau
# dihapus — mids=[] adalah sinyal "keepalive" (lihat antispam_queue.py::
# _process_detection): dikirim untuk SETIAP pesan masuk (spam atau bukan)
# supaya delete_worker (di bawah) menjaga scheduler grup itu tetap standby
# selama grupnya masih ada aktivitas, dan cuma drop kalau BENAR-BENAR 0
# pesan (bukan cuma 0 spam) selama DELETE_IDLE_SECS.
delete_queue: asyncio.Queue = asyncio.Queue()

# ── Panel write queue ──────────────────────────────────────────────────────────
# Tujuan: tombol panel DM (toggle, +/-, dsb) terasa "ringan" — UI berubah instan
# karena cache di-update duluan (optimistic), sedangkan penulisan ke DB yang
# sesungguhnya diantrikan dan dieksekusi belakangan oleh satu worker tunggal,
# dengan jeda antar-item supaya tidak membebani DB/API saat banyak grup/klik
# bersamaan.
#
# Jika penulisan GAGAL PERMANEN (sudah di-retry beberapa kali, tetap gagal):
#   1. Cache untuk chat_id tersebut di-invalidate (paksa baca ulang dari DB
#      di klik berikutnya — otomatis dapat nilai asli, bukan nilai optimistic
#      yang ternyata tidak pernah tersimpan).
#   2. Jika item membawa info pesan panel asal (dm_chat_id + dm_msg_id),
#      panggil _panel_rollback_callback (didaftarkan oleh handlers_dm.py saat
#      startup) untuk mengoreksi tampilan panel itu + beri tahu admin.
# Jika sukses → tidak ada apa-apa (silent), karena UI sudah benar dari awal.
panel_write_queue: asyncio.Queue = asyncio.Queue()
PANEL_WRITE_DELAY   = 0.3   # detik — jeda antar penulisan ke DB
PANEL_WRITE_RETRIES = 3     # percobaan ulang sebelum dianggap gagal permanen

# ── Optimistic delete untuk panel berbasis LIST (regex, freelist, whitelist) ──
# Beda dengan toggle (satu field, gampang di-cache), item list di-render
# dengan query DB langsung tiap panel dibuka. Supaya tombol hapus tetap terasa
# instan, id yang baru ditekan hapus langsung ditandai "pending" dan
# DISEMBUNYIKAN dari hasil render berikutnya — SEBELUM operasi DB benar-benar
# selesai. Penghapusan asli tetap lewat panel_write_queue.
#
# Key di-namespace per jenis list ("regex", "free", "mention_wl", dst) supaya
# id yang kebetulan sama antar jenis (mis. ObjectId vs user_id) tidak pernah
# saling tabrakan walau berada di chat_id yang sama.
_pending_delete_ids: dict[tuple[int, str], set[str]] = {}


def mark_pending_delete(chat_id: int, namespace: str, item_id) -> None:
    """Sembunyikan item ini dari render panel berikutnya, instan (tanpa DB)."""
    _pending_delete_ids.setdefault((chat_id, namespace), set()).add(str(item_id))


def unmark_pending_delete(chat_id: int, namespace: str, item_id) -> None:
    """Lepas status pending — dipanggil worker setelah DB write selesai/gagal."""
    _pending_delete_ids.get((chat_id, namespace), set()).discard(str(item_id))


def is_pending_delete(chat_id: int, namespace: str, item_id) -> bool:
    return str(item_id) in _pending_delete_ids.get((chat_id, namespace), ())


def pending_delete_count(chat_id: int, namespace: str) -> int:
    return len(_pending_delete_ids.get((chat_id, namespace), ()))


def enqueue_regex_delete(
    chat_id: int, doc_id: str,
    dm_chat_id: int | None = None, dm_msg_id: int | None = None,
) -> None:
    """Antrikan penghapusan satu dokumen regex grup ke DB (non-blocking)."""
    panel_write_queue.put_nowait({
        "kind": "regex_delete", "chat_id": chat_id, "key": doc_id, "value": None,
        "dm_chat_id": dm_chat_id, "dm_msg_id": dm_msg_id,
    })


def enqueue_free_delete(
    chat_id: int, target_user_id: int,
    dm_chat_id: int | None = None, dm_msg_id: int | None = None,
) -> None:
    """Antrikan penghapusan satu Member VIP (free_per_group) ke DB (non-blocking)."""
    panel_write_queue.put_nowait({
        "kind": "free_delete", "chat_id": chat_id, "key": str(target_user_id), "value": None,
        "dm_chat_id": dm_chat_id, "dm_msg_id": dm_msg_id,
    })


def enqueue_mention_wl_delete(
    chat_id: int, username: str,
    dm_chat_id: int | None = None, dm_msg_id: int | None = None,
) -> None:
    """Antrikan penghapusan satu username whitelist mention ke DB (non-blocking)."""
    uname = username.lower().lstrip("@")
    panel_write_queue.put_nowait({
        "kind": "mention_wl_delete", "chat_id": chat_id, "key": uname, "value": None,
        "dm_chat_id": dm_chat_id, "dm_msg_id": dm_msg_id,
    })

# ── Shared FloodWait State — koordinasi lintas worker ─────────────────────────
# Saat salah satu worker kena FloodWait dari Telegram, worker lain harus
# berhenti juga agar tidak memperparah flood. Setiap worker memanggil
# wait_global_flood_backoff() sebelum API call berat, dan memanggil
# set_global_flood_backoff(seconds) saat kena FloodWait.
#
# Ini TIDAK menggantikan FloodWait handling lokal masing-masing worker —
# melainkan lapisan koordinasi ANTAR worker: jika delete_worker kena FloodWait
# 10 detik, log_flush_worker dan moderation_worker tidak ikut tembak API
# selama window itu.
import time as _time_module

_global_flood_until: float = 0.0   # monotonic timestamp saat backoff selesai


def set_global_flood_backoff(seconds: float) -> None:
    """Catat bahwa Telegram baru kirim FloodWait. Semua worker akan mundur."""
    global _global_flood_until
    deadline = _time_module.monotonic() + seconds
    if deadline > _global_flood_until:
        _global_flood_until = deadline


async def wait_global_flood_backoff() -> None:
    """
    Tunggu jika ada global flood backoff aktif.
    Dipanggil oleh tiap worker sebelum API call berat (send, delete, restrict).
    Jika backoff sudah lewat, langsung return tanpa delay.
    """
    remaining = _global_flood_until - _time_module.monotonic()
    if remaining > 0:
        await asyncio.sleep(remaining)

# Diisi oleh plugins/ui/handlers_dm.py via register_panel_rollback_callback().
# Signature: async def callback(client, kind, chat_id, key, dm_chat_id, dm_msg_id) -> None
_panel_rollback_callback = None


def register_panel_rollback_callback(fn) -> None:
    """Daftarkan fungsi yang dipanggil saat penulisan panel gagal permanen."""
    global _panel_rollback_callback
    _panel_rollback_callback = fn

# ── Handled messages tracker ──────────────────────────────────────────────────
_handled_msgs: dict[tuple[int, int], float] = {}
_HANDLED_TTL = 30.0

# ── LOG_CHANNEL "sudah dikirim langsung" tracker ───────────────────────────────
# Dipakai KHUSUS untuk gate yang mengirim log LOG_CHANNEL-nya sendiri secara
# langsung (lihat core/antispam_queue.py::_log_gate_to_channel — saat ini
# cuma dipakai Gate F/Ubot Detect). plugins/commands/log.py::log_deletion_trigger
# adalah "shadow re-detector" yang jalan independen di on_message (group=3) dan
# TIDAK tahu gate mana yang sebenarnya menghapus sebuah pesan — dia cuma
# menebak ulang dari isi pesan (regex/AI/dup/gcast/bio/link/mention). Kalau isi
# pesan yang sudah ditangani Ubot Detect KEBETULAN juga cocok pola regex, shadow
# re-detector ini akan generate log KEDUA yang independen kalau tidak dicegah.
# Marker ini yang jadi sinyal "sudah ada log langsung, jangan tebak ulang".
_log_channel_sent: dict[tuple[int, int], float] = {}
_LOG_CHANNEL_SENT_TTL = 30.0

# ── Backend state ─────────────────────────────────────────────────────────────
_BACKEND: str = "sqlite"   # "mongo" | "sqlite"
_mongo_db = None           # pymongo async database instance (jika aktif)
_sqlite_conn: aiosqlite.Connection | None = None


# ══════════════════════════════════════════════════════════════════════════════
# BACKEND DETECTION — dipanggil sekali di setup_db()
# ══════════════════════════════════════════════════════════════════════════════

async def _try_mongo(url: str, db_name: str):
    """
    Coba koneksi ke MongoDB. Return database object (PyMongo Async API)
    jika berhasil, None jika gagal. Timeout 5 detik agar tidak hang di Termux.
    """
    try:
        # DNS untuk resolve mongodb+srv:// — ikut CUSTOM_DNS_SERVERS/.env yang
        # sama dipakai main.py (socket.getaddrinfo global) supaya satu sumber
        # kebenaran, bukan hardcode terpisah. Kalau CUSTOM_DNS_ENABLED=0,
        # dnspython balik pakai resolver sistem default (configure=True).
        import dns.resolver
        _dns_enabled = os.environ.get("CUSTOM_DNS_ENABLED", "1").strip().lower() in ("1", "true", "yes")
        if _dns_enabled:
            _dns_servers = [s.strip() for s in os.environ.get("CUSTOM_DNS_SERVERS", "1.1.1.1,1.0.0.1").split(",") if s.strip()]
            dns.resolver.default_resolver = dns.resolver.Resolver(configure=False)
            dns.resolver.default_resolver.nameservers = _dns_servers
        else:
            dns.resolver.default_resolver = dns.resolver.Resolver(configure=True)
        from pymongo import AsyncMongoClient  # type: ignore
        client = AsyncMongoClient(
            url,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=5000,
        )
        # Ping untuk memastikan koneksi benar-benar berhasil
        await client.admin.command("ping")
        return client[db_name]
    except ImportError:
        print("[DB] pymongo (async) tidak terinstall / versi terlalu lama — skip MongoDB")
        return None
    except Exception as e:
        print(f"[DB] MongoDB gagal: {e}")
        return None


async def _init_backend():
    """
    Tentukan backend aktif dan inisialisasi koneksi.
    Urutan: MongoDB (multi-shard jika MONGO_URL_2, MONGO_URL_3, ... diisi) → SQLite.

    MULTI-SHARD:
      Setiap MONGO_URL_n di .env dianggap 1 cluster Mongo independen.
      Semua cluster dicoba konek secara paralel saat startup. Cluster yang
      gagal konek TIDAK menggagalkan keseluruhan startup — collection yang
      ter-assign ke shard itu (lihat core/mongo_shard.py) otomatis fallback
      ke SQLite lokal khusus shard tersebut (_get_sqlite_shard), sementara
      shard lain yang sehat tetap berjalan normal di MongoDB.

      Jika hanya MONGO_URL terisi (tidak ada MONGO_URL_2 dst) → perilaku
      identik dengan versi sebelumnya: 1 cluster, tidak ada perubahan.
    """
    global _BACKEND, _mongo_db, _sqlite_conn

    urls = _shard.MONGO_URLS
    if urls:
        print(f"[DB] 🔍 Mencoba koneksi {len(urls)} shard MongoDB...")
        results = await asyncio.gather(*[_try_mongo(u, MONGO_DB_NAME) for u in urls])
        any_ok = False
        for idx, mongo in enumerate(results):
            _shard.set_shard_db(idx, mongo)
            if mongo is not None:
                any_ok = True
                tag = f"shard{idx}" if len(urls) > 1 else "MongoDB"
                print(f"[DB] ✅ {tag} aktif (db={MONGO_DB_NAME})")
            else:
                print(f"[DB] ⚠️  shard{idx} gagal konek → fallback SQLite khusus shard ini")
        if any_ok:
            _BACKEND  = "mongo"
            _mongo_db = _shard.get_shard_db(0)   # compat lama: kode yang akses _mongo_db langsung tetap dapat shard utama
            print(f"[DB] ✅ BACKEND AKTIF: MongoDB  ({_shard.shard_summary()})")
            return
        print("[DB] ⚠️  Semua shard MongoDB gagal → fallback total ke SQLite")
    else:
        print("[DB] ℹ️  MONGO_URL tidak ditemukan di .env → pakai SQLite")

    # ── Fallback SQLite (penuh, tidak ada shard Mongo yang hidup) ────────────
    _BACKEND = "sqlite"
    _sqlite_conn = await aiosqlite.connect(SQLITE_PATH, check_same_thread=False)
    await _sqlite_conn.execute("PRAGMA journal_mode=WAL")
    await _sqlite_conn.execute("PRAGMA synchronous=NORMAL")
    _sqlite_conn.row_factory = aiosqlite.Row
    abs_path = os.path.abspath(SQLITE_PATH)
    print(f"[DB] ✅ BACKEND AKTIF: SQLite     (file={abs_path})")


def get_active_backend() -> str:
    """Kembalikan nama backend aktif: 'mongo' atau 'sqlite'."""
    return _BACKEND


# ══════════════════════════════════════════════════════════════════════════════
# JSON ENCODER — handle datetime & bytes (untuk SQLite backend)
# ══════════════════════════════════════════════════════════════════════════════

class _Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return {"__dt__": obj.isoformat()}
        if isinstance(obj, bytes):
            return {"__bytes__": obj.hex()}
        return super().default(obj)


def _object_hook(obj: dict):
    if "__dt__" in obj:
        try:
            return datetime.fromisoformat(obj["__dt__"])
        except Exception:
            return obj
    if "__bytes__" in obj:
        try:
            return bytes.fromhex(obj["__bytes__"])
        except Exception:
            return obj
    return obj


def _dumps(obj) -> str:
    return json.dumps(obj, cls=_Encoder, ensure_ascii=False)


def _loads(s: str) -> dict:
    return json.loads(s, object_hook=_object_hook)


# ══════════════════════════════════════════════════════════════════════════════
# SQLITE HELPERS (internal)
# ══════════════════════════════════════════════════════════════════════════════

async def _get_sqlite() -> aiosqlite.Connection:
    global _sqlite_conn
    if _sqlite_conn is None:
        _sqlite_conn = await aiosqlite.connect(SQLITE_PATH, check_same_thread=False)
        await _sqlite_conn.execute("PRAGMA journal_mode=WAL")
        await _sqlite_conn.execute("PRAGMA synchronous=NORMAL")
        _sqlite_conn.row_factory = aiosqlite.Row
    return _sqlite_conn


# ── SQLite per-shard (fallback granular saat 1 cluster Mongo down) ───────────
# Dipakai HANYA oleh collection yang masuk SHARDED_COLLECTIONS saat shard
# Mongo yang dituju sedang tidak sehat. File terpisah per shard index agar
# tidak rebutan lock dengan _sqlite_conn (shard 0 / fallback total).
_shard_sqlite_conns: dict[int, aiosqlite.Connection] = {}


async def _get_sqlite_for_shard(idx: int) -> aiosqlite.Connection:
    if idx == 0:
        # Shard 0 berbagi file yang sama dengan fallback SQLite lama —
        # tidak perlu file baru, menjaga kompatibilitas data lama.
        return await _get_sqlite()
    global _shard_sqlite_conns
    conn = _shard_sqlite_conns.get(idx)
    if conn is None:
        path = SQLITE_PATH.replace(".db", f".shard{idx}.db") if SQLITE_PATH.endswith(".db") else f"{SQLITE_PATH}.shard{idx}"
        conn = await aiosqlite.connect(path, check_same_thread=False)
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=NORMAL")
        conn.row_factory = aiosqlite.Row
        _shard_sqlite_conns[idx] = conn
    return conn


def _tbl(name: str) -> str:
    return "col_" + name.replace("-", "_").replace(" ", "_")


async def _ensure_table(conn: aiosqlite.Connection, name: str):
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {_tbl(name)} (
            id     INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id TEXT    UNIQUE,
            data   TEXT    NOT NULL
        )
    """)
    await conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# QUERY MATCHING — MongoDB-style (untuk SQLite backend)
# ══════════════════════════════════════════════════════════════════════════════

def _match(doc: dict, query: dict) -> bool:
    if not query:
        return True
    for key, val in query.items():
        doc_val = doc.get(key)
        if isinstance(val, dict):
            for op, op_val in val.items():
                if op == "$exists":
                    if bool(op_val) != (key in doc):
                        return False
                elif op == "$ne":
                    if doc_val == op_val:
                        return False
                elif op == "$gt":
                    if not (doc_val is not None and doc_val > op_val):
                        return False
                elif op == "$lt":
                    if not (doc_val is not None and doc_val < op_val):
                        return False
                elif op == "$in":
                    if doc_val not in op_val:
                        return False
        else:
            if doc_val != val:
                return False
    return True


def _apply_update(doc: dict, update: dict, is_insert: bool = False) -> dict:
    result = dict(doc)
    if "$set" in update:
        result.update(update["$set"])
    if "$setOnInsert" in update and is_insert:
        result.update(update["$setOnInsert"])
    if "$inc" in update:
        for k, v in update["$inc"].items():
            result[k] = (result.get(k) or 0) + v
    if "$unset" in update:
        for k in update["$unset"]:
            result.pop(k, None)
    if "$push" in update:
        for k, v in update["$push"].items():
            if k not in result or not isinstance(result[k], list):
                result[k] = []
            result[k].append(v)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# RESULT OBJECTS
# ══════════════════════════════════════════════════════════════════════════════

class DeleteResult:
    def __init__(self, count: int = 0):
        self.deleted_count = count


class UpdateResult:
    def __init__(self, matched: int = 0, modified: int = 0, upserted_id=None):
        self.matched_count  = matched
        self.modified_count = modified
        self.upserted_id    = upserted_id


# ══════════════════════════════════════════════════════════════════════════════
# ASYNC CURSOR — mimic pymongo async cursor API
# ══════════════════════════════════════════════════════════════════════════════

def _apply_projection(doc: dict, projection: dict | None) -> dict:
    """Terapkan projection ala MongoDB (inclusion-only atau exclusion-only,
    _id boleh dicampur dengan mode manapun)."""
    if not projection:
        return doc
    is_inclusion = any(v for k, v in projection.items() if k != "_id")
    if is_inclusion:
        result = {k: doc[k] for k in projection if k != "_id" and k in doc}
        if projection.get("_id", 1) and "_id" in doc:
            result["_id"] = doc["_id"]
        return result
    result = dict(doc)
    for k, v in projection.items():
        if not v and k in result:
            del result[k]
    return result


class AsyncCursor:
    """
    Unified cursor untuk SQLite dan MongoDB.
    SQLite: load semua data lalu filter in-memory.
    MongoDB: delegasi ke pymongo async cursor dengan sort/skip/limit native.
    """

    def __init__(self, col_name: str, query: dict, projection: dict | None = None):
        self._col        = col_name
        self._query      = query
        self._projection = projection
        self._sort_key: str | None = None
        self._sort_dir: int        = 1
        self._skip_n:   int        = 0
        self._limit_n:  int | None = None
        self._docs:     list[dict] | None = None
        self._pos:      int        = 0
        # MongoDB async cursor (lazy)
        self._mongo_cur = None

    def sort(self, key: str, direction: int = 1) -> "AsyncCursor":
        self._sort_key = key
        self._sort_dir = direction
        return self

    def skip(self, n: int) -> "AsyncCursor":
        self._skip_n = n
        return self

    def limit(self, n: int) -> "AsyncCursor":
        self._limit_n = n
        return self

    # ── SQLite path ───────────────────────────────────────────────────────────
    async def _load_sqlite(self, shard_idx: int = 0):
        conn = await _get_sqlite_for_shard(shard_idx) if _BACKEND == "mongo" else await _get_sqlite()
        tbl  = _tbl(self._col)
        await _ensure_table(conn, self._col)
        async with conn.execute(f"SELECT id, data FROM {tbl} ORDER BY id") as cur:
            rows = await cur.fetchall()
        docs = []
        for row in rows:
            try:
                d = _loads(row["data"])
                if "_id" not in d:
                    d["_id"] = str(row["id"])
                if _match(d, self._query):
                    docs.append(_apply_projection(d, self._projection))
            except Exception:
                pass
        return docs

    def _apply_sort_skip_limit(self, docs: list[dict]) -> list[dict]:
        if self._sort_key:
            docs = sorted(
                docs,
                key=lambda d: (d.get(self._sort_key) or ""),
                reverse=(self._sort_dir == -1),
            )
        docs = docs[self._skip_n:]
        if self._limit_n is not None:
            docs = docs[:self._limit_n]
        return docs

    # ── MongoDB path ──────────────────────────────────────────────────────────
    async def _load_mongo(self):
        bare = _bare_collection_name(self._col)
        cid  = _shard.extract_chat_id(self._query)

        if bare in _shard.HOT_PATH_COLLECTIONS and _shard.SHARD_COUNT > 1 and cid is None:
            # Query tanpa chat_id spesifik → harus baca SEMUA cluster hot-path
            # dan gabung (sort/skip/limit diterapkan in-memory setelah
            # gabung, karena makna "skip 10 limit 20" lintas cluster tidak
            # bisa dipush secara native ke masing-masing cluster tanpa hasil
            # salah). Hanya perlu scan hotpath_pool() — collection ini TIDAK
            # PERNAH mendarat di cluster fungsi lain (config/global/log/
            # analytics), jadi tidak perlu scan semua 6 cluster.
            all_docs: list[dict] = []
            for idx in _shard.hotpath_pool():
                col = _mongo_col_for(self._col, idx)
                if col is not None:
                    try:
                        async for doc in col.find(self._query, self._projection):
                            doc["_id"] = str(doc["_id"])
                            all_docs.append(_apply_projection(doc, self._projection))
                        continue
                    except Exception as e:
                        print(f"[DB:mongo] find error {self._col} (shard{idx}): {e}")
                        _shard.mark_shard_down(idx)
                all_docs.extend(await self._load_sqlite(idx))
            self._docs = self._apply_sort_skip_limit(all_docs)
            return

        shard_idx = _resolve_shard_idx(self._col, self._query)
        col = _mongo_col_for(self._col, shard_idx)
        if col is not None:
            try:
                cur = col.find(self._query, self._projection)
                if self._sort_key:
                    cur = cur.sort(self._sort_key, self._sort_dir)
                if self._skip_n:
                    cur = cur.skip(self._skip_n)
                if self._limit_n is not None:
                    cur = cur.limit(self._limit_n)
                docs = []
                async for doc in cur:
                    doc["_id"] = str(doc["_id"])
                    docs.append(doc)
                self._docs = docs
                return
            except Exception as e:
                print(f"[DB:mongo] find error {self._col} (shard{shard_idx}): {e}")
                _shard.mark_shard_down(shard_idx)
        # fallback sqlite shard ini
        docs = await self._load_sqlite(shard_idx)
        self._docs = self._apply_sort_skip_limit(docs)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._docs is None:
            if _BACKEND == "mongo":
                await self._load_mongo()
            else:
                self._docs = self._apply_sort_skip_limit(await self._load_sqlite())
        if self._pos >= len(self._docs):
            raise StopAsyncIteration
        doc       = self._docs[self._pos]
        self._pos += 1
        return doc

    async def to_list(self, length: int | None = None) -> list[dict]:
        if self._docs is None:
            if _BACKEND == "mongo":
                await self._load_mongo()
            else:
                self._docs = self._apply_sort_skip_limit(await self._load_sqlite())
        if length is not None:
            return self._docs[:length]
        return list(self._docs)


# ══════════════════════════════════════════════════════════════════════════════
# SHARD RESOLUTION — pilih cluster Mongo / SQLite yang tepat untuk operasi
# ══════════════════════════════════════════════════════════════════════════════
#
# Skema functional partitioning (lihat core/mongo_shard.py untuk detail):
#   - Collection di _shard.HOT_PATH_COLLECTIONS → di-hash by chat_id, HANYA
#     berputar di antara _shard.HOTPATH_CLUSTERS (2 cluster khusus deteksi
#     pesan real-time).
#   - Collection lain → cluster TETAP sesuai _shard.FUNCTIONAL_MAP (config,
#     global-lintas-grup, log, atau analytics). Nama yang tidak terdaftar di
#     FUNCTIONAL_MAP otomatis jatuh ke CONFIG_CLUSTER (index 0).
#
# bare_name: nama collection SEBELUM di-prefix _ns(), karena FUNCTIONAL_MAP /
# HOT_PATH_COLLECTIONS didefinisikan dengan nama generik (mis. "bio_profiles"),
# bukan "mybot_bio_profiles". Collection.name yang disimpan di __init__ SUDAH
# di-_ns()-kan oleh DB.__getitem__, jadi kita strip prefix CODE_BOT dulu
# untuk pencocokan, dengan fallback ke endswith() agar tetap aman.

def _bare_collection_name(ns_name: str) -> str:
    if _CODE_BOT and ns_name.startswith(_CODE_BOT + "_"):
        return ns_name[len(_CODE_BOT) + 1:]
    return ns_name


def _resolve_shard_idx(ns_name: str, *dicts: dict) -> int:
    """Tentukan shard index untuk operasi pada collection ns_name.

    - Kalau ns_name adalah collection hot-path → cari chat_id di salah satu
      dict (query dan/atau doc/update), lalu hash ke salah satu
      HOTPATH_CLUSTERS (dengan overflow otomatis kalau home-nya down).
    - Kalau bukan → langsung pakai cluster tetap sesuai fungsinya
      (FUNCTIONAL_MAP), juga dengan overflow otomatis kalau home-nya down.
      SQLite lokal baru jadi upaya terakhir kalau SEMUA cluster kandidat
      down bersamaan — lihat penjelasan lengkap di
      core/mongo_shard.py bagian "OVERFLOW ROUTING".
    """
    bare = _bare_collection_name(ns_name)
    if _shard.SHARD_COUNT <= 1:
        return 0

    if bare in _shard.HOT_PATH_COLLECTIONS:
        for d in dicts:
            if not isinstance(d, dict):
                continue
            # cek langsung, lalu cek di dalam $set (update_one biasa berbentuk {"$set": {...}})
            cid = _shard.extract_chat_id(d)
            if cid is None and "$set" in d:
                cid = _shard.extract_chat_id(d["$set"])
            if cid is None and "$setOnInsert" in d:
                cid = _shard.extract_chat_id(d["$setOnInsert"])
            if cid is not None:
                return _shard.resolve_effective_shard(cid)
        # tidak ketemu chat_id (mis. query kosong/admin listing) → cluster
        # hot-path pertama yang tersedia, konsisten dengan AsyncCursor
        # multi-scan di bawah untuk kasus yang sama.
        return _shard.hotpath_pool()[0]

    return _shard.resolve_effective_fixed(bare)


def _mongo_col_for(ns_name: str, shard_idx: int):
    """Return pymongo async collection object di shard tertentu, atau None jika
    shard itu sedang tidak sehat (caller harus fallback SQLite shard ini)."""
    shard_db = _shard.get_shard_db(shard_idx)
    if shard_db is None or not _shard.is_shard_healthy(shard_idx):
        return None
    return shard_db[ns_name]


# ═══════════════════════════════════════════════════════════════════════════
#  SHARD HEALTH SUPERVISOR — deteksi shard down/pulih + migrasi balik data
# ═══════════════════════════════════════════════════════════════════════════
#
# Loop ini yang membuat overflow routing (core/mongo_shard.py) benar2
# "self-healing" tanpa perlu restart bot:
#   1. Ping tiap shard Mongo secara periodik.
#   2. Kalau shard yang tadinya sehat gagal ping BERUNTUN sejumlah
#      DOWN_STREAK_NEEDED kali → resmi ditandai down, grup2-nya otomatis
#      overflow ke shard sehat lain (lewat resolve_effective_shard, sudah
#      otomatis dipakai _resolve_shard_idx di atas — tidak perlu kode lain
#      yang diubah).
#   3. Kalau shard yang tadinya down berhasil ping BERUNTUN sejumlah
#      UP_STREAK_NEEDED kali → resmi ditandai pulih, DAN memicu migrasi
#      balik data yang sempat "numpang" di shard overflow selama down.
#
# MIGRASI BALIK (_migrate_shard_back) — 2 JALUR INDEPENDEN:
#
#   JALUR 1 — FIXED-CLUSTER collections (FUNCTIONAL_MAP):
#     Untuk tiap collection yang rumahnya PERSIS shard idx ini (mis. idx=4
#     → group_action_log, nexus_actlog, dst), scan SEMUA shard lain, ambil
#     SEMUA dokumen (tidak ada filter chat_id sama sekali — collection ini
#     memang 1 lokasi untuk semua data) dan pindahkan balik ke idx:
#       - Collection log/append-only (group_action_log, nexus_actlog,
#         ai_debug_log, sticker_report): insert apa adanya (pakai _id asli
#         — ObjectId unik lintas cluster, jadi aman; DuplicateKeyError
#         berarti sudah pernah dipindah, cukup dianggap sukses lalu hapus
#         sisi overflow).
#       - Collection lain (config/global/analytics): upsert keyed by _id
#         asli — generik, aman untuk struktur dokumen apapun.
#
#   JALUR 2 — HOT_PATH collections (hanya kalau idx anggota HOTPATH_CLUSTERS):
#     Karena tidak ada registry eksplisit "grup mana lagi overflow ke mana",
#     daftar grup yang home-nya shard idx diambil dari config_db (collection
#     "status" — fixed di CONFIG_CLUSTER, 1 dokumen per grup) — jadi bisa
#     dipetakan cid → shard_index_for_chat(cid) tanpa perlu scan penuh semua
#     collection hot-path di semua cluster.
#     Untuk tiap grup yang home-nya idx, dicek SESAMA anggota HOTPATH_CLUSTERS
#     lain (bukan seluruh SHARD_COUNT — cluster fungsi lain tidak pernah
#     kedatangan data hot-path) untuk tiap collection di HOT_PATH_COLLECTIONS:
#     upsert keyed by chat_id+user_id/username (overflow MENANG tanpa perlu
#     banding timestamp — karena shard rumah down sepanjang periode overflow
#     terjadi, data di overflow PASTI lebih baru).
#
#   Kedua jalur: dokumen HANYA dihapus dari cluster overflow SETELAH sukses
#   tertulis ke rumah — kalau gagal di tengah, dibiarkan untuk dicoba lagi
#   siklus supervisor berikutnya (idempotent, tidak ada window kehilangan
#   data).
# ═══════════════════════════════════════════════════════════════════════════


_LOG_LIKE_COLLECTIONS = {"group_action_log", "nexus_actlog", "ai_debug_log", "sticker_report"}

# Timeout khusus operasi migrasi (find/insert/update/delete per shard) — lebih
# longgar daripada connectTimeoutMS/socketTimeoutMS koneksi normal (5s), karena
# migrasi bisa scan banyak dokumen tanpa filter dan boleh nunggu lebih lama
# tanpa mengganggu jalur hot-path (loop ini jalan di background, bukan
# ditunggu user). Pakai pymongo CSOT (client-side operation timeout) — override
# durasi PER OPERASI tanpa perlu bikin koneksi/client baru.
_MIGRATION_OP_TIMEOUT_SECS = 20


async def _migrate_shard_back(idx: int) -> None:
    """Dipanggil sekali oleh shard_health_supervisor tiap kali shard idx
    baru saja resmi dianggap pulih. Lihat penjelasan lengkap di atas.

    Dua skema migrasi berjalan independen untuk shard idx yang baru pulih:

    1) FIXED-CLUSTER collections (FUNCTIONAL_MAP) — migrasi SEMUA dokumen
       collection yang rumahnya idx ini, dari SEMUA cluster lain (tempat dia
       "numpang" selama idx down lewat resolve_effective_fixed) balik ke idx.
       Tidak perlu filter chat_id sama sekali — collection ini memang selalu
       1 lokasi untuk SEMUA data, bukan per-grup.

    2) HOT_PATH collections — migrasi per-chat_id seperti skema lama, HANYA
       kalau idx adalah anggota HOTPATH_CLUSTERS, dan HANYA dari sesama
       anggota HOTPATH_CLUSTERS lain (bukan seluruh SHARD_COUNT — cluster
       fungsi lain tidak pernah kedatangan data hot-path).
    """
    print(f"[DB:shard] 🔄 shard{idx} pulih — mulai migrasi balik data overflow...")
    moved_total  = 0
    error_total  = 0

    # ── 1) Fixed-cluster collections yang rumahnya persis idx ini ───────────
    fixed_bare_names = [b for b, c in _shard.FUNCTIONAL_MAP.items() if c == idx]
    for bare in fixed_bare_names:
        ns_name  = _ns(bare)
        home_col = _mongo_col_for(ns_name, idx)
        if home_col is None:
            print(f"[DB:shard] ⚠️  shard{idx} belum benar-benar sehat saat migrasi {bare} — dibatalkan, coba lagi siklus berikutnya.")
            continue
        for j in range(_shard.SHARD_COUNT):
            if j == idx:
                continue
            src_col = _mongo_col_for(ns_name, j)
            if src_col is None:
                continue
            try:
                with _mongo_op_timeout(_MIGRATION_OP_TIMEOUT_SECS):
                    async for doc in src_col.find({}):
                        doc_id = doc.pop("_id", None)
                        try:
                            if bare in _LOG_LIKE_COLLECTIONS:
                                if doc_id is not None:
                                    doc["_id"] = doc_id
                                await home_col.insert_one(doc)
                            elif bare == "status" and doc.get("chat_id") is not None:
                                # "status" (config_db) = 1 dokumen per grup, keyed
                                # by chat_id — BUKAN oleh _id. Selama home down,
                                # tulisan overflow membuat _id BARU utk chat_id
                                # yang aslinya SUDAH punya dokumen (tak terhapus)
                                # di home. Upsert by _id lama di sini akan bikin
                                # dokumen KEDUA utk chat_id yang sama (bug: grup
                                # dobel di daftar "Kelola Grup"). Upsert by
                                # chat_id supaya menimpa dokumen home yang lama,
                                # bukan menambah baris baru.
                                await home_col.update_one(
                                    {"chat_id": doc["chat_id"]}, {"$set": doc}, upsert=True
                                )
                            elif doc_id is not None:
                                # Collection fixed generik (config/global/analytics)
                                # tidak selalu punya chat_id — upsert keyed by _id
                                # asli supaya aman untuk collection apapun.
                                await home_col.update_one({"_id": doc_id}, {"$set": doc}, upsert=True)
                            if doc_id is not None:
                                await src_col.delete_one({"_id": doc_id})
                            moved_total += 1
                        except DuplicateKeyError:
                            if doc_id is not None:
                                await src_col.delete_one({"_id": doc_id})
                        except Exception as e:
                            error_total += 1
                            print(f"[DB:shard] ⚠️  gagal pindah 1 dok {bare} shard{j}→shard{idx}: {e}")
            except Exception as e:
                print(f"[DB:shard] ⚠️  migrasi {bare} shard{j}→shard{idx} error: {e}")

    # ── 2) Hot-path collections (per-chat_id), hanya jika idx anggota pool ──
    if idx in _shard.HOTPATH_CLUSTERS:
        try:
            target_cids: list[int] = []
            with _mongo_op_timeout(_MIGRATION_OP_TIMEOUT_SECS):
                async for doc in config_db.find({}):
                    cid = doc.get("chat_id")
                    if cid is not None and _shard.shard_index_for_chat(cid) == idx:
                        target_cids.append(cid)
        except Exception as e:
            print(f"[DB:shard] ⚠️  shard{idx}: gagal ambil daftar grup dari config_db: {e}")
            target_cids = []

        if target_cids:
            pool = _shard.hotpath_pool()
            for bare in _shard.HOT_PATH_COLLECTIONS:
                ns_name  = _ns(bare)
                home_col = _mongo_col_for(ns_name, idx)
                if home_col is None:
                    print(f"[DB:shard] ⚠️  shard{idx} belum benar-benar sehat saat migrasi {bare} — dibatalkan, coba lagi siklus berikutnya.")
                    continue
                for j in pool:
                    if j == idx:
                        continue
                    src_col = _mongo_col_for(ns_name, j)
                    if src_col is None:
                        continue
                    try:
                        with _mongo_op_timeout(_MIGRATION_OP_TIMEOUT_SECS):
                            async for doc in src_col.find({"chat_id": {"$in": target_cids}}):
                                doc_id = doc.pop("_id", None)
                                try:
                                    key: dict = {"chat_id": doc.get("chat_id")}
                                    if "user_id" in doc:
                                        key["user_id"] = doc["user_id"]
                                    elif "username" in doc:
                                        key["username"] = doc["username"]
                                    elif doc_id is not None:
                                        key = {"_id": doc_id}
                                    await home_col.update_one(key, {"$set": doc}, upsert=True)
                                    if doc_id is not None:
                                        await src_col.delete_one({"_id": doc_id})
                                    moved_total += 1
                                except DuplicateKeyError:
                                    if doc_id is not None:
                                        await src_col.delete_one({"_id": doc_id})
                                except Exception as e:
                                    error_total += 1
                                    print(f"[DB:shard] ⚠️  gagal pindah 1 dok {bare} shard{j}→shard{idx}: {e}")
                    except Exception as e:
                        print(f"[DB:shard] ⚠️  migrasi {bare} shard{j}→shard{idx} error: {e}")
        else:
            print(f"[DB:shard] shard{idx}: tidak ada grup hot-path ter-assign, skip bagian ini.")

    print(f"[DB:shard] ✅ Migrasi balik shard{idx} selesai — {moved_total} dokumen dipindahkan, {error_total} gagal (akan dicoba lagi).")


async def shard_health_supervisor() -> None:
    """
    Loop background — ping periodik tiap shard Mongo untuk deteksi shard
    down (agar overflow ke shard lain aktif) MAUPUN shard yang sudah PULIH
    (agar dipindah balik + migrasi data overflow-nya). Tidak berjalan sama
    sekali kalau cuma ada 1 shard (tidak ada kandidat overflow/failover).

    ANTISIPASI STARTUP RACE: baru mulai ping PERTAMA setelah jeda
    MONGO_SHARD_HEALTH_GRACE_START detik SESUDAH setup_db() selesai (yang
    sendiri sudah menunggu semua shard mencoba konek paralel, masing2 jatah
    5 detik yang SAMA). Jeda tambahan ini jaring pengaman untuk cluster M0
    yang kadang butuh waktu "bangun" dari status paused lebih dari 5 detik
    — supaya shard yang cuma LAMBAT connect di awal (bukan benar2 down)
    tidak keburu memicu grup2-nya overflow. Lihat detail lengkap di
    core/mongo_shard.py bagian "ANTISIPASI STARTUP RACE".

    ENV:
      MONGO_SHARD_HEALTH_INTERVAL    default 20  — jeda antar siklus ping (detik)
      MONGO_SHARD_HEALTH_GRACE_START default 30  — jeda sebelum siklus ping pertama (detik)
    """
    if _shard.SHARD_COUNT <= 1:
        return

    _INTERVAL     = float(os.environ.get("MONGO_SHARD_HEALTH_INTERVAL", 20.0))
    _GRACE_START  = float(os.environ.get("MONGO_SHARD_HEALTH_GRACE_START", 30.0))

    await asyncio.sleep(_GRACE_START)
    print(f"[DB:shard] 🩺 Shard health supervisor aktif ({_shard.SHARD_COUNT} shard, interval {_INTERVAL}s).")

    while True:
        for idx in range(_shard.SHARD_COUNT):
            shard_db = _shard.get_shard_db(idx)
            if shard_db is None:
                continue  # shard ini gagal total saat startup, tidak punya client sama sekali

            ok = False
            try:
                await asyncio.wait_for(shard_db.client.admin.command("ping"), timeout=5.0)
                ok = True
            except Exception:
                ok = False

            event = _shard.note_ping_result(idx, ok)
            if event == "down":
                print(f"[DB:shard] ⚠️  shard{idx} terdeteksi DOWN (ping gagal beruntun) — grup terkait mulai overflow ke shard sehat lain.")
            elif event == "recovered":
                print(f"[DB:shard] ✅ shard{idx} terdeteksi PULIH (ping sukses beruntun).")
                asyncio.create_task(_migrate_shard_back(idx))

        await asyncio.sleep(_INTERVAL)


# ══════════════════════════════════════════════════════════════════════════════
# COLLECTION — unified API untuk MongoDB dan SQLite
# ══════════════════════════════════════════════════════════════════════════════

class Collection:
    def __init__(self, name: str):
        self.name = name

    # ── find_one ──────────────────────────────────────────────────────────────

    async def find_one(self, query: dict = {}, projection: dict | None = None) -> dict | None:
        if _BACKEND == "mongo":
            shard_idx = _resolve_shard_idx(self.name, query)
            col = _mongo_col_for(self.name, shard_idx)
            if col is not None:
                try:
                    doc = await col.find_one(query, projection)
                    if doc:
                        doc["_id"] = str(doc["_id"])
                        doc = _apply_projection(doc, projection)
                    return doc
                except Exception as e:
                    print(f"[DB:mongo] find_one error {self.name} (shard{shard_idx}): {e}")
                    _shard.mark_shard_down(shard_idx)
                    # lanjut ke fallback SQLite shard ini di bawah
            # Shard down / error → fallback SQLite khusus shard ini
            conn = await _get_sqlite_for_shard(shard_idx)
        else:
            conn = await _get_sqlite()
        tbl  = _tbl(self.name)
        await _ensure_table(conn, self.name)
        async with conn.execute(f"SELECT id, data FROM {tbl} ORDER BY id") as cur:
            rows = await cur.fetchall()
        for row in rows:
            try:
                d = _loads(row["data"])
                if "_id" not in d:
                    d["_id"] = str(row["id"])
                if _match(d, query):
                    return _apply_projection(d, projection)
            except Exception:
                pass
        return None

    # ── find ──────────────────────────────────────────────────────────────────

    def find(self, query: dict = {}, projection: dict | None = None) -> AsyncCursor:
        return AsyncCursor(self.name, query, projection)

    # ── update_one ────────────────────────────────────────────────────────────

    async def update_one(
        self, filter_q: dict, update: dict, upsert: bool = False
    ) -> UpdateResult:
        if _BACKEND == "mongo":
            shard_idx = _resolve_shard_idx(self.name, filter_q, update)
            col = _mongo_col_for(self.name, shard_idx)
            if col is not None:
                try:
                    r = await col.update_one(filter_q, update, upsert=upsert)
                    return UpdateResult(r.matched_count, r.modified_count, str(r.upserted_id) if r.upserted_id else None)
                except Exception as e:
                    print(f"[DB:mongo] update_one error {self.name} (shard{shard_idx}): {e}")
                    _shard.mark_shard_down(shard_idx)
            conn = await _get_sqlite_for_shard(shard_idx)
        else:
            conn = await _get_sqlite()
        tbl  = _tbl(self.name)
        await _ensure_table(conn, self.name)
        found_id, found_doc = None, None
        async with conn.execute(f"SELECT id, data FROM {tbl} ORDER BY id") as cur:
            rows = await cur.fetchall()
        for row in rows:
            try:
                d = _loads(row["data"])
                if "_id" not in d:
                    d["_id"] = str(row["id"])
                if _match(d, filter_q):
                    found_id  = row["id"]
                    found_doc = d
                    break
            except Exception:
                pass
        if found_doc is not None:
            new_doc = _apply_update(found_doc, update, is_insert=False)
            await conn.execute(f"UPDATE {tbl} SET data=? WHERE id=?", (_dumps(new_doc), found_id))
            await conn.commit()
            return UpdateResult(matched=1, modified=1)
        if upsert:
            new_doc = {}
            new_doc.update(filter_q)
            new_doc = _apply_update(new_doc, update, is_insert=True)
            doc_id  = str(new_doc.get("_id") or uuid.uuid4().hex)
            new_doc["_id"] = doc_id
            await conn.execute(
                f"INSERT OR REPLACE INTO {tbl} (doc_id, data) VALUES (?, ?)",
                (doc_id, _dumps(new_doc))
            )
            await conn.commit()
            return UpdateResult(matched=0, modified=0, upserted_id=doc_id)
        return UpdateResult()

    # ── find_one_and_update ───────────────────────────────────────────────────

    async def find_one_and_update(
        self,
        filter_q: dict,
        update: dict,
        upsert: bool = False,
        return_document: bool = False,
    ) -> dict | None:
        return_after = bool(return_document)  # ReturnDocument.AFTER == True, BEFORE == False
        if _BACKEND == "mongo":
            shard_idx = _resolve_shard_idx(self.name, filter_q, update)
            col = _mongo_col_for(self.name, shard_idx)
            if col is not None:
                try:
                    doc = await col.find_one_and_update(
                        filter_q, update, upsert=upsert,
                        return_document=ReturnDocument.AFTER if return_after else ReturnDocument.BEFORE,
                    )
                    if doc:
                        doc["_id"] = str(doc["_id"])
                    return doc
                except Exception as e:
                    print(f"[DB:mongo] find_one_and_update error {self.name} (shard{shard_idx}): {e}")
                    _shard.mark_shard_down(shard_idx)
            conn = await _get_sqlite_for_shard(shard_idx)
        else:
            conn = await _get_sqlite()
        tbl  = _tbl(self.name)
        await _ensure_table(conn, self.name)
        found_id, found_doc = None, None
        async with conn.execute(f"SELECT id, data FROM {tbl} ORDER BY id") as cur:
            rows = await cur.fetchall()
        for row in rows:
            try:
                d = _loads(row["data"])
                if "_id" not in d:
                    d["_id"] = str(row["id"])
                if _match(d, filter_q):
                    found_id  = row["id"]
                    found_doc = d
                    break
            except Exception:
                pass
        if found_doc is not None:
            new_doc = _apply_update(found_doc, update, is_insert=False)
            await conn.execute(f"UPDATE {tbl} SET data=? WHERE id=?", (_dumps(new_doc), found_id))
            await conn.commit()
            return new_doc if return_after else found_doc
        if upsert:
            new_doc = {}
            new_doc.update(filter_q)
            new_doc = _apply_update(new_doc, update, is_insert=True)
            doc_id  = str(new_doc.get("_id") or uuid.uuid4().hex)
            new_doc["_id"] = doc_id
            await conn.execute(
                f"INSERT OR REPLACE INTO {tbl} (doc_id, data) VALUES (?, ?)",
                (doc_id, _dumps(new_doc))
            )
            await conn.commit()
            return new_doc if return_after else None
        return None

    # ── update_many ───────────────────────────────────────────────────────────

    async def update_many(self, filter_q: dict, update: dict) -> UpdateResult:
        if _BACKEND == "mongo":
            bare = _bare_collection_name(self.name)
            cid  = _shard.extract_chat_id(filter_q)
            if bare in _shard.SHARDED_COLLECTIONS and _shard.SHARD_COUNT > 1 and cid is None:
                # Filter tidak menyebut chat_id spesifik (mis. cleanup TTL massal)
                # → harus menjangkau SEMUA cluster hot-path, bukan hanya 1.
                total_matched, total_modified = 0, 0
                for idx in _shard.hotpath_pool():
                    col = _mongo_col_for(self.name, idx)
                    if col is None:
                        continue
                    try:
                        r = await col.update_many(filter_q, update)
                        total_matched  += r.matched_count
                        total_modified += r.modified_count
                    except Exception as e:
                        print(f"[DB:mongo] update_many error {self.name} (shard{idx}): {e}")
                        _shard.mark_shard_down(idx)
                return UpdateResult(total_matched, total_modified)
            shard_idx = _resolve_shard_idx(self.name, filter_q, update)
            col = _mongo_col_for(self.name, shard_idx)
            if col is not None:
                try:
                    r = await col.update_many(filter_q, update)
                    return UpdateResult(r.matched_count, r.modified_count)
                except Exception as e:
                    print(f"[DB:mongo] update_many error {self.name} (shard{shard_idx}): {e}")
                    _shard.mark_shard_down(shard_idx)
            conn = await _get_sqlite_for_shard(shard_idx)
        else:
            conn = await _get_sqlite()
        # SQLite
        tbl  = _tbl(self.name)
        await _ensure_table(conn, self.name)
        async with conn.execute(f"SELECT id, data FROM {tbl}") as cur:
            rows = await cur.fetchall()
        modified = 0
        for row in rows:
            try:
                d = _loads(row["data"])
                if "_id" not in d:
                    d["_id"] = str(row["id"])
                if not filter_q or _match(d, filter_q):
                    new_doc = _apply_update(d, update, is_insert=False)
                    await conn.execute(f"UPDATE {tbl} SET data=? WHERE id=?", (_dumps(new_doc), row["id"]))
                    modified += 1
            except Exception:
                pass
        if modified:
            await conn.commit()
        return UpdateResult(matched=modified, modified=modified)

    # ── insert_one ────────────────────────────────────────────────────────────

    async def insert_one(self, doc: dict) -> UpdateResult:
        if _BACKEND == "mongo":
            shard_idx = _resolve_shard_idx(self.name, doc)
            col = _mongo_col_for(self.name, shard_idx)
            if col is not None:
                try:
                    d = dict(doc)
                    d.pop("_id", None)
                    r = await col.insert_one(d)
                    return UpdateResult(upserted_id=str(r.inserted_id))
                except Exception as e:
                    print(f"[DB:mongo] insert_one error {self.name} (shard{shard_idx}): {e}")
                    _shard.mark_shard_down(shard_idx)
            conn = await _get_sqlite_for_shard(shard_idx)
        else:
            conn = await _get_sqlite()
        tbl    = _tbl(self.name)
        await _ensure_table(conn, self.name)
        doc_id = str(doc.get("_id") or uuid.uuid4().hex)
        d      = dict(doc)
        d["_id"] = doc_id
        try:
            await conn.execute(
                f"INSERT OR IGNORE INTO {tbl} (doc_id, data) VALUES (?, ?)",
                (doc_id, _dumps(d))
            )
            await conn.commit()
        except Exception:
            pass
        return UpdateResult(upserted_id=doc_id)

    # ── delete_one ────────────────────────────────────────────────────────────

    async def delete_one(self, query: dict) -> DeleteResult:
        if _BACKEND == "mongo":
            shard_idx = _resolve_shard_idx(self.name, query)
            col = _mongo_col_for(self.name, shard_idx)
            if col is not None:
                try:
                    r = await col.delete_one(query)
                    return DeleteResult(r.deleted_count)
                except Exception as e:
                    print(f"[DB:mongo] delete_one error {self.name} (shard{shard_idx}): {e}")
                    _shard.mark_shard_down(shard_idx)
            conn = await _get_sqlite_for_shard(shard_idx)
        else:
            conn = await _get_sqlite()
        tbl  = _tbl(self.name)
        await _ensure_table(conn, self.name)
        async with conn.execute(f"SELECT id, data FROM {tbl} ORDER BY id") as cur:
            rows = await cur.fetchall()
        for row in rows:
            try:
                d = _loads(row["data"])
                if "_id" not in d:
                    d["_id"] = str(row["id"])
                if _match(d, query):
                    await conn.execute(f"DELETE FROM {tbl} WHERE id=?", (row["id"],))
                    await conn.commit()
                    return DeleteResult(1)
            except Exception:
                pass
        return DeleteResult(0)

    # ── delete_many ───────────────────────────────────────────────────────────

    async def delete_many(self, query: dict = {}) -> DeleteResult:
        if _BACKEND == "mongo":
            bare = _bare_collection_name(self.name)
            cid  = _shard.extract_chat_id(query)
            if bare in _shard.SHARDED_COLLECTIONS and _shard.SHARD_COUNT > 1 and cid is None:
                total = 0
                for idx in _shard.hotpath_pool():
                    col = _mongo_col_for(self.name, idx)
                    if col is None:
                        continue
                    try:
                        r = await col.delete_many(query)
                        total += r.deleted_count
                    except Exception as e:
                        print(f"[DB:mongo] delete_many error {self.name} (shard{idx}): {e}")
                        _shard.mark_shard_down(idx)
                return DeleteResult(total)
            shard_idx = _resolve_shard_idx(self.name, query)
            col = _mongo_col_for(self.name, shard_idx)
            if col is not None:
                try:
                    r = await col.delete_many(query)
                    return DeleteResult(r.deleted_count)
                except Exception as e:
                    print(f"[DB:mongo] delete_many error {self.name} (shard{shard_idx}): {e}")
                    _shard.mark_shard_down(shard_idx)
            conn = await _get_sqlite_for_shard(shard_idx)
        else:
            conn = await _get_sqlite()
        tbl  = _tbl(self.name)
        await _ensure_table(conn, self.name)
        async with conn.execute(f"SELECT id, data FROM {tbl}") as cur:
            rows = await cur.fetchall()
        to_del = []
        for row in rows:
            try:
                d = _loads(row["data"])
                if "_id" not in d:
                    d["_id"] = str(row["id"])
                if not query or _match(d, query):
                    to_del.append(row["id"])
            except Exception:
                to_del.append(row["id"])
        for rid in to_del:
            await conn.execute(f"DELETE FROM {tbl} WHERE id=?", (rid,))
        if to_del:
            await conn.commit()
        return DeleteResult(len(to_del))

    # ── insert_many ───────────────────────────────────────────────────────────

    async def insert_many(self, docs: list[dict]) -> None:
        if not docs:
            return
        if _BACKEND == "mongo":
            bare = _bare_collection_name(self.name)
            if bare in _shard.SHARDED_COLLECTIONS and _shard.SHARD_COUNT > 1:
                # Dokumen bisa milik chat_id berbeda-beda → kelompokkan per shard
                groups: dict[int, list[dict]] = {}
                for d in docs:
                    idx = _resolve_shard_idx(self.name, d)
                    groups.setdefault(idx, []).append(d)
                for idx, group_docs in groups.items():
                    col = _mongo_col_for(self.name, idx)
                    clean = [{k: v for k, v in d.items() if k != "_id"} for d in group_docs]
                    if col is not None:
                        try:
                            await col.insert_many(clean, ordered=False)
                            continue
                        except Exception as e:
                            print(f"[DB:mongo] insert_many error {self.name} (shard{idx}): {e}")
                            _shard.mark_shard_down(idx)
                    # fallback sqlite shard ini untuk grup dokumen ini
                    conn = await _get_sqlite_for_shard(idx)
                    tbl  = _tbl(self.name)
                    await _ensure_table(conn, self.name)
                    for doc in group_docs:
                        doc_id = str(doc.get("_id") or uuid.uuid4().hex)
                        dd = dict(doc)
                        dd["_id"] = doc_id
                        try:
                            await conn.execute(
                                f"INSERT OR IGNORE INTO {tbl} (doc_id, data) VALUES (?, ?)",
                                (doc_id, _dumps(dd))
                            )
                        except Exception:
                            pass
                    await conn.commit()
                return
            col = _mongo_col_for(self.name, _shard.resolve_effective_fixed(bare))
            if col is not None:
                try:
                    clean = [{k: v for k, v in d.items() if k != "_id"} for d in docs]
                    await col.insert_many(clean, ordered=False)
                    return
                except Exception as e:
                    _fixed_idx = _shard.resolve_effective_fixed(bare)
                    print(f"[DB:mongo] insert_many error {self.name} (shard{_fixed_idx}): {e}")
                    _shard.mark_shard_down(_fixed_idx)
            conn = await _get_sqlite_for_shard(_shard.fixed_cluster_for(bare))
        else:
            conn = await _get_sqlite()
        tbl  = _tbl(self.name)
        await _ensure_table(conn, self.name)
        for doc in docs:
            doc_id = str(doc.get("_id") or uuid.uuid4().hex)
            d      = dict(doc)
            d["_id"] = doc_id
            try:
                await conn.execute(
                    f"INSERT OR IGNORE INTO {tbl} (doc_id, data) VALUES (?, ?)",
                    (doc_id, _dumps(d))
                )
            except Exception:
                pass
        await conn.commit()

    # ── bulk_write ────────────────────────────────────────────────────────────
    # NOTE: hanya mendukung pymongo.UpdateOne (cukup untuk kebutuhan saat ini —
    # ns_flush_score_buffer). Operasi lain (InsertOne/DeleteOne/dst) bisa
    # ditambahkan kalau ada pemanggil baru yang butuh.

    async def bulk_write(self, ops: list, ordered: bool = False) -> "UpdateResult":
        """
        Versi unified dari pymongo async `collection.bulk_write()` — shard-aware untuk
        MongoDB (operasi dikelompokkan per shard berdasarkan chat_id di
        filter masing-masing UpdateOne, lalu dieksekusi 1x bulk_write per
        shard — TETAP 1 round-trip per shard, bukan 1 round-trip per dokumen),
        dan fallback ke update_one satu-satu untuk SQLite (tidak ada operasi
        bulk asli di SQLite, tapi volume kasus pakai ini kecil).

        Tanpa method ini, caller yang mengandalkan `collection._col.bulk_write()`
        langsung (mengasumsikan `Collection` adalah pymongo async collection asli)
        akan selalu gagal dengan AttributeError, lalu (kalau caller punya
        fallback naive) jatuh balik ke N kali update_one per flush — yang
        justru meniadakan tujuan batching ini sama sekali.
        """
        from pymongo import UpdateOne as _UpdateOne

        def _op_parts(op):
            """Ambil (filter, update_doc, upsert) dari UpdateOne secara aman.
            Pymongo menyimpan ini sebagai atribut privat (_filter/_doc/_upsert,
            stabil sejak versi 3.x s/d 4.x yang dipakai project ini — lihat
            requirements.txt), tapi tetap pakai getattr dengan default aman
            agar tidak crash kalau suatu saat nama atributnya berubah."""
            f = getattr(op, "_filter", None) or {}
            d = getattr(op, "_doc", None) or {}
            u = bool(getattr(op, "_upsert", False))
            return f, d, u

        total_matched, total_modified, total_upserted = 0, 0, 0

        if _BACKEND == "mongo":
            bare = _bare_collection_name(self.name)

            groups: dict[int, list] = {}
            for op in ops:
                if not isinstance(op, _UpdateOne):
                    continue  # tipe lain belum didukung — lihat catatan di atas
                f, d, _u = _op_parts(op)
                # PENTING: selalu lewat _resolve_shard_idx (bukan hardcode 0
                # untuk collection non-hot-path) — kalau tidak, collection
                # seperti newscore_stats (ANALYTICS_CLUSTER) akan salah
                # mendarat di CONFIG_CLUSTER setiap kali di-bulk_write.
                shard_idx = _resolve_shard_idx(self.name, f, d)
                groups.setdefault(shard_idx, []).append(op)

            for shard_idx, shard_ops in groups.items():
                col = _mongo_col_for(self.name, shard_idx)
                if col is not None:
                    try:
                        r = await col.bulk_write(shard_ops, ordered=ordered)
                        total_matched  += r.matched_count
                        total_modified += r.modified_count
                        total_upserted += len(r.upserted_ids or {})
                        continue
                    except Exception as e:
                        print(f"[DB:mongo] bulk_write error {self.name} (shard{shard_idx}): {e}")
                        _shard.mark_shard_down(shard_idx)
                # Fallback per-op kalau shard ini down — tetap lebih baik
                # daripada gagal total untuk seluruh batch.
                for op in shard_ops:
                    f, d, u = _op_parts(op)
                    try:
                        r = await self.update_one(f, d, upsert=u)
                        total_matched  += r.matched_count
                        total_modified += r.modified_count
                    except Exception as e:
                        print(f"[DB] bulk_write fallback op error {self.name}: {e}")
            return UpdateResult(total_matched, total_modified)

        # SQLite: tidak ada operasi bulk asli — terapkan satu-satu lewat
        # update_one (sudah konsisten dengan jalur SQLite collection lain).
        for op in ops:
            if not isinstance(op, _UpdateOne):
                continue
            f, d, u = _op_parts(op)
            try:
                r = await self.update_one(f, d, upsert=u)
                total_matched  += r.matched_count
                total_modified += r.modified_count
            except Exception as e:
                print(f"[DB] bulk_write sqlite op error {self.name}: {e}")
        return UpdateResult(total_matched, total_modified)

    # ── count_documents ───────────────────────────────────────────────────────

    async def count_documents(self, query: dict = {}) -> int:
        if _BACKEND == "mongo":
            bare = _bare_collection_name(self.name)
            cid  = _shard.extract_chat_id(query)
            if bare in _shard.SHARDED_COLLECTIONS and _shard.SHARD_COUNT > 1 and cid is None:
                total = 0
                for idx in _shard.hotpath_pool():
                    col = _mongo_col_for(self.name, idx)
                    if col is None:
                        continue
                    try:
                        if query:
                            total += await col.count_documents(query)
                        else:
                            total += await col.estimated_document_count()
                    except Exception as e:
                        print(f"[DB:mongo] count_documents error {self.name} (shard{idx}): {e}")
                        _shard.mark_shard_down(idx)
                return total
            shard_idx = _resolve_shard_idx(self.name, query)
            col = _mongo_col_for(self.name, shard_idx)
            if col is not None:
                try:
                    if query:
                        return await col.count_documents(query)
                    return await col.estimated_document_count()
                except Exception as e:
                    print(f"[DB:mongo] count_documents error {self.name} (shard{shard_idx}): {e}")
                    _shard.mark_shard_down(shard_idx)
            conn = await _get_sqlite_for_shard(shard_idx)
        else:
            conn = await _get_sqlite()
        tbl  = _tbl(self.name)
        await _ensure_table(conn, self.name)
        if not query:
            async with conn.execute(f"SELECT COUNT(*) FROM {tbl}") as cur:
                row = await cur.fetchone()
                return row[0] if row else 0
        async with conn.execute(f"SELECT data FROM {tbl}") as cur:
            rows = await cur.fetchall()
        return sum(
            1 for row in rows
            if _match(_loads(row["data"]), query)
        )

    # ── create_index ──────────────────────────────────────────────────────────

    async def create_index(
        self,
        keys,
        unique: bool = False,
        sparse: bool = False,
        expireAfterSeconds: int | None = None,
    ):
        """
        SQLite: no-op (tidak perlu index eksplisit).
        MongoDB: buat index asli via pymongo async — di SEMUA shard yang relevan untuk
        collection ini (penting untuk TTL index seperti bio_profiles.expires_at,
        agar auto-expire bekerja konsisten di tiap cluster, bukan cuma shard 0).
        """
        if _BACKEND == "mongo":
            try:
                from pymongo import ASCENDING, DESCENDING  # type: ignore
                if isinstance(keys, str):
                    keys = [(keys, ASCENDING)]
                bare = _bare_collection_name(self.name)
                if bare in _shard.SHARDED_COLLECTIONS and _shard.SHARD_COUNT > 1:
                    shard_range = _shard.hotpath_pool()
                else:
                    shard_range = [_shard.fixed_cluster_for(bare)]
                for idx in shard_range:
                    col = _mongo_col_for(self.name, idx)
                    if col is None:
                        continue
                    try:
                        await col.create_index(
                            keys,
                            unique=unique,
                            sparse=sparse,
                            expireAfterSeconds=expireAfterSeconds,
                        )
                    except Exception:
                        pass
            except Exception:
                pass


# ══════════════════════════════════════════════════════════════════════════════
# DB — dict-like container, mimic pymongo async client["db"]["collection"]
# ══════════════════════════════════════════════════════════════════════════════

class DB:
    def __getitem__(self, name: str) -> Collection:
        return Collection(_ns(name))


db = DB()

# ── Named collections (backward compat) ───────────────────────────────────────
config_db          = db["status"]
messages_db        = db["seen_messages"]
# Anti-gcast global (lintas grup) — SENGAJA dipisah dari messages_db/
# seen_messages. Dokumennya (_id: "glob_<uid>_<hash>") tidak punya field
# chat_id di level atas (data lintas grup ada di dalam sub-field
# "locations.<chat_id>"), jadi tidak pernah bisa di-hash by chat_id — kalau
# dibiarkan nebeng di seen_messages (collection hot-path), dia akan selalu
# jatuh ke cluster hot-path pertama padahal isinya bukan data per-grup.
# Dipetakan permanen ke GLOBAL_CLUSTER lewat FUNCTIONAL_MAP (lihat
# core/mongo_shard.py) supaya jelas terisolasi dari deteksi real-time.
gcast_global_db    = db["gcast_global"]
regex_db           = db["regex_list"]
regex_helper_db    = db["regex_helper"]  # Gerbang penerjemah kata ambigu — jalan SEBELUM AI Manual (lihat core/regex_helper.py)
nexus_kalimat_db   = db["nexus_kalimat"]  # v8.0 — Record Data: raw teks TAHAP 1 (spam_claim) yg diperiksa Groq TAHAP 2
nexus_kalimat_variants_db = db["nexus_kalimat_variants"]  # varian hasil generate TAHAP 2 yg sudah dinilai Groq (link ke raw via claim_key)
nexus_regex_db     = db["nexus_regex"]
banned_groups_db   = db["banned_groups"]  # /bangrup — grup yang bot dipaksa keluar & dilarang add ulang
nexus_grup_db      = db["nexus_grup"]
nexus_whitelist_db = db["nexus_whitelist"]
nexus_actlog_db    = db["nexus_actlog"]
group_action_log_db = db["group_action_log"]
bot_config_db      = db["bot_config"]
mention_cache_db   = db["mention_member_cache"]
mention_global_db  = db["mention_global_cache"]   # non-akun & channel/grup — lintas semua grup
mention_wl_db      = db["mention_whitelist"]       # whitelist per grup (username raw)
# "Database khusus" — username @mention yang cache-miss tapi RESOLUSI-nya
# ditunda (bukan cache-miss biasa yang langsung ditembak API real-time).
# Diisi oleh 2 kondisi di _is_external_mention (plugins/filters/antispam.py):
#   1. Username lagi di-resolve lorong lain (single-flight in-flight) —
#      daripada ikut menunggu (blocking Gate E), langsung dicatat di sini.
#   2. Pesan multi-mention — hanya mention PERTAMA yang cache-miss yang
#      ditembak API real-time per pesan; sisanya dicatat di sini.
# Dibersihkan pelan-pelan oleh mention_pending_resolve_loop saat antrian
# antispam lagi sepi (lihat core/antispam_queue.py::get_detection_queue_stats).
mention_pending_db = db["mention_pending_resolve"]
welcome_delete_db  = db["welcome_pending_delete"]   # jadwal hapus pesan welcome (resilien restart)
# Kata kustom RAW per kategori CategoryDetector (nexus/ai_core/category_detector.py)
# — panel "Kategori Kata" (Owner). BUKAN regex generator: cuma simpan teks
# apa adanya, dicek via substring match sederhana di category_detector.py.
category_custom_words_db = db["category_custom_words"]


# ══════════════════════════════════════════════════════════════════════════════
# HANDLED MESSAGES TRACKER
# ══════════════════════════════════════════════════════════════════════════════

def mark_message_handled(chat_id: int, msg_id: int) -> None:
    _handled_msgs[(chat_id, msg_id)] = time.time()
    if len(_handled_msgs) > 2000:
        cutoff = time.time() - _HANDLED_TTL
        stale  = [k for k, ts in _handled_msgs.items() if ts < cutoff]
        for k in stale:
            _handled_msgs.pop(k, None)


def is_message_handled(chat_id: int, msg_id: int) -> bool:
    key = (chat_id, msg_id)
    ts  = _handled_msgs.get(key)
    if ts is None:
        return False
    if time.time() - ts > _HANDLED_TTL:
        _handled_msgs.pop(key, None)
        return False
    return True


# ══════════════════════════════════════════════════════════════════════════════
# QUEUED-FOR-ASYNC-DETECTION TRACKER
# ══════════════════════════════════════════════════════════════════════════════
# BUG FIX (race consec_spam vs pesan bersih): plugins/filters/antispam.py
# group=2 hanya me-enqueue pesan ke antrian deteksi async
# (core/antispam_queue.py::enqueue_for_detection) — deteksi SEBENARNYA (gate
# regex/link/dup/gcast/mention/ubot) baru dikerjakan BELAKANGAN oleh worker
# terpisah, lewat banyak `await` (get_config, query Mongo, dst). Sementara
# itu, group=10 (_clean_message_tracker) jalan HAMPIR INSTAN persis setelah
# group=2 selesai — jauh lebih cepat dari worker tsb. Sebelum fix ini,
# group=10 cuma cek is_message_handled() (yang baru di-set kalau salah satu
# gate SUDAH mengklaim pesan sebagai spam) — hampir selalu KALAH race,
# sehingga pesan spam yang belum sempat diproses dianggap "bersih" dan
# reset_local_mute() dipanggil sebelum gate sempat menghitungnya. Akibatnya
# SPAM_MUTE_THRESHOLD custom (mis. 5) nyaris tidak pernah tercapai secara
# konsisten.
#
# FIX: begitu sebuah pesan di-enqueue (group=2, SINKRON, pasti selesai
# sebelum group=10 jalan), ditandai "queued" di sini. group=10 SKIP total
# untuk pesan yang masih/baru saja masuk status ini — keputusan bersih-atau-
# tidak diserahkan SEPENUHNYA ke _process_detection() (core/antispam_queue.py)
# sendiri, di SATU titik final setelah semua gate selesai dicek — itu
# satu-satunya tempat yang benar-benar tahu hasil akhirnya, tanpa race.
_queued_msgs: dict[tuple[int, int], float] = {}
_QUEUED_TTL = 30.0


def mark_message_queued(chat_id: int, msg_id: int) -> None:
    _queued_msgs[(chat_id, msg_id)] = time.time()
    if len(_queued_msgs) > 2000:
        cutoff = time.time() - _QUEUED_TTL
        stale  = [k for k, ts in _queued_msgs.items() if ts < cutoff]
        for k in stale:
            _queued_msgs.pop(k, None)


def is_message_queued(chat_id: int, msg_id: int) -> bool:
    key = (chat_id, msg_id)
    ts  = _queued_msgs.get(key)
    if ts is None:
        return False
    if time.time() - ts > _QUEUED_TTL:
        _queued_msgs.pop(key, None)
        return False
    return True


def clear_message_queued(chat_id: int, msg_id: int) -> None:
    """Dipanggil di SEMUA jalur keluar _process_detection() (finally-block)
    supaya status "queued" tidak nyangkut sampai TTL habis — mempersempit
    jendela race untuk pesan berikutnya di (chat_id, msg_id) yang sama
    (jarang tapi bisa terjadi kalau id pesan dipakai ulang lintas migrasi/
    testing)."""
    _queued_msgs.pop((chat_id, msg_id), None)


def mark_log_channel_sent(chat_id: int, msg_id: int) -> None:
    """Tandai (chat_id, msg_id) sudah dapat log LOG_CHANNEL langsung dari
    gate-nya sendiri (lihat core/antispam_queue.py::_log_gate_to_channel).
    Dipanggil SYNCHRONOUS saat gate memutuskan kirim log — bukan setelah
    _send_log() selesai — supaya secepat mungkin terlihat oleh
    log_deletion_trigger (plugins/commands/log.py) yang jalan independen."""
    _log_channel_sent[(chat_id, msg_id)] = time.time()
    if len(_log_channel_sent) > 2000:
        cutoff = time.time() - _LOG_CHANNEL_SENT_TTL
        stale  = [k for k, ts in _log_channel_sent.items() if ts < cutoff]
        for k in stale:
            _log_channel_sent.pop(k, None)


def is_log_channel_sent(chat_id: int, msg_id: int) -> bool:
    key = (chat_id, msg_id)
    ts  = _log_channel_sent.get(key)
    if ts is None:
        return False
    if time.time() - ts > _LOG_CHANNEL_SENT_TTL:
        _log_channel_sent.pop(key, None)
        return False
    return True


# ══════════════════════════════════════════════════════════════════════════════
# SETUP — init backend + tabel + background cleanup
# ══════════════════════════════════════════════════════════════════════════════

async def _cleanup_seen_messages():
    """Background task: hapus seen_messages lebih dari 24 jam, jalan setiap 1 jam."""
    while True:
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            break
        try:
            cutoff = time.time() - 86400
            if _BACKEND == "mongo":
                # PENTING: pakai Collection (bukan _mongo_db langsung) supaya
                # cleanup menjangkau SEMUA shard tempat seen_messages tersebar,
                # bukan hanya shard 0. _ns() tetap diterapkan oleh db[...].
                await db["seen_messages"].delete_many({"time": {"$lt": cutoff}})
            else:
                conn = await _get_sqlite()
                # _ns() sudah diterapkan saat tabel dibuat di setup_db(); pakai nama yang sama
                ns_col = _ns("seen_messages")
                tbl    = _tbl(ns_col)
                await _ensure_table(conn, ns_col)
                async with conn.execute(f"SELECT id, data FROM {tbl}") as cur:
                    rows = await cur.fetchall()
                deleted = 0
                for row in rows:
                    try:
                        d  = _loads(row["data"])
                        ts = d.get("time", 0)
                        if isinstance(ts, (int, float)) and ts < cutoff:
                            await conn.execute(f"DELETE FROM {tbl} WHERE id=?", (row["id"],))
                            deleted += 1
                    except Exception:
                        pass
                if deleted:
                    await conn.commit()
                    prefix = f"[{_CODE_BOT}] " if _CODE_BOT else ""
                    print(f"[DB] {prefix}cleanup: {deleted} seen_messages expired dihapus")
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[DB] cleanup error: {e}")



async def _migrate_legacy_data():
    """
    Migrasi otomatis data lama (tanpa CODE_BOT prefix) ke namespace aktif.
    Dijalankan sekali saat startup jika CODE_BOT aktif.
    Hanya menyalin dokumen yang BELUM ada di namespace baru — tidak menimpa.
    Aman dijalankan berulang kali.
    """
    if not _CODE_BOT:
        return

    # Daftar semua collection yang perlu dicek
    _COLLECTIONS = [
        "status", "seen_messages", "regex_list",
        "regex_per_group", "whitelist_per_group", "free_per_group",
        "nexus_kalimat", "nexus_regex", "nexus_grup", "nexus_whitelist",
        "nexus_actlog", "local_mute", "group_action_log",
        "ai_debug_log", "dm_users", "nexus_ai_model", "bot_config", "group_admin_roster",
    ]

    migrated_total = 0

    if _BACKEND == "mongo":
        # Catatan: migrasi legacy ini SENGAJA hanya menyentuh shard 0 (_mongo_db
        # variable lama = shard utama). Data lama (sebelum sharding ada) pasti
        # semua berada di shard 0, jadi tidak perlu fan-out ke shard lain.
        for col_name in _COLLECTIONS:
            old_col = _mongo_db[col_name]          # collection lama tanpa prefix
            new_col = _mongo_db[_ns(col_name)]     # collection baru dengan prefix

            # Skip jika nama sama (tidak ada prefix)
            if col_name == _ns(col_name):
                continue

            try:
                old_count = await old_col.count_documents({})
                if old_count == 0:
                    continue

                new_count = await new_col.count_documents({})
                if new_count > 0:
                    # Sudah ada data di namespace baru, skip
                    continue

                # Copy semua dokumen dari old ke new
                docs = []
                async for doc in old_col.find({}):
                    docs.append(doc)

                if docs:
                    try:
                        await new_col.insert_many(docs, ordered=False)
                        migrated_total += len(docs)
                        print(f"[Migrasi] {col_name} → {_ns(col_name)}: {len(docs)} dokumen dipindah")
                    except Exception as e:
                        print(f"[Migrasi] {col_name}: sebagian gagal ({e})")

            except Exception as e:
                print(f"[Migrasi] Error cek {col_name}: {e}")

    elif _BACKEND == "sqlite":
        conn = await _get_sqlite()
        for col_name in _COLLECTIONS:
            new_col = _ns(col_name)
            if col_name == new_col:
                continue

            try:
                # Cek apakah tabel lama ada
                async with conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (_tbl(col_name),)
                ) as cur:
                    row = await cur.fetchone()
                if not row:
                    continue

                # Cek jumlah data di tabel lama
                async with conn.execute(f"SELECT COUNT(*) FROM {_tbl(col_name)}") as cur:
                    old_count = (await cur.fetchone())[0]
                if old_count == 0:
                    continue

                # Cek tabel baru sudah ada data?
                await _ensure_table(conn, new_col)
                async with conn.execute(f"SELECT COUNT(*) FROM {_tbl(new_col)}") as cur:
                    new_count = (await cur.fetchone())[0]
                if new_count > 0:
                    continue

                # Copy
                async with conn.execute(f"SELECT data FROM {_tbl(col_name)}") as cur:
                    rows = await cur.fetchall()

                for row in rows:
                    try:
                        await conn.execute(
                            f"INSERT OR IGNORE INTO {_tbl(new_col)} (id, data) VALUES (?, ?)",
                            (str(uuid.uuid4()), row["data"] if isinstance(row, dict) else row[0])
                        )
                    except Exception:
                        pass

                await conn.commit()
                migrated_total += old_count
                print(f"[Migrasi] SQLite {col_name} → {new_col}: {old_count} baris dipindah")

            except Exception as e:
                print(f"[Migrasi] SQLite error {col_name}: {e}")

    if migrated_total > 0:
        print(f"[Migrasi] ✅ Total {migrated_total} dokumen berhasil dimigrasikan ke namespace [{_CODE_BOT}]")
    else:
        print(f"[Migrasi] ✅ Namespace [{_CODE_BOT}] sudah up-to-date, tidak ada migrasi diperlukan")


async def _migrate_sqlite_to_mongo():
    """
    Migrasi data dari SQLite lokal ke MongoDB saat backend aktif adalah MongoDB
    dan file SQLite lokal masih ada dan berisi data.

    Alur:
      1. Cek apakah file SQLite ada dan tidak kosong.
      2. Untuk setiap collection, ambil semua dokumen dari SQLite.
      3. Untuk setiap dokumen, cek apakah sudah ada di MongoDB (berdasarkan _id atau doc_id).
         - Jika belum ada → insert ke MongoDB.
         - Jika sudah ada (duplikat) → skip (data MongoDB diutamakan).
      4. Setelah semua collection selesai dan SQLite sudah kosong total → log selesai.
    """
    import os as _os
    import json as _json

    if _BACKEND != "mongo" or _mongo_db is None:
        return  # Hanya jalan jika backend aktif adalah MongoDB

    sqlite_path = SQLITE_PATH
    if not _os.path.exists(sqlite_path):
        return  # Tidak ada file SQLite, skip

    # Cek apakah file SQLite punya data sama sekali
    try:
        import aiosqlite as _aiosqlite
        async with _aiosqlite.connect(sqlite_path) as sq:
            sq.row_factory = _aiosqlite.Row
            async with sq.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ) as cur:
                tables = [r[0] for r in await cur.fetchall()]
        if not tables:
            return  # SQLite kosong, skip
    except Exception as e:
        print(f"[Migrasi SQLite→Mongo] Gagal buka SQLite: {e}")
        return

    _COLLECTIONS = [
        "status", "seen_messages", "regex_list",
        "regex_per_group", "whitelist_per_group", "free_per_group",
        "nexus_kalimat", "nexus_regex", "nexus_grup", "nexus_whitelist",
        "local_mute", "group_action_log",
        "nexus_actlog", "ai_debug_log", "dm_users", "nexus_ai_model", "bot_config", "group_admin_roster",
    ]

    total_migrated = 0
    total_skipped  = 0

    print("[Migrasi SQLite→Mongo] 🔄 Ditemukan data SQLite lokal, mulai migrasi...")

    try:
        async with _aiosqlite.connect(sqlite_path) as sq:
            sq.row_factory = _aiosqlite.Row

            for col_name in _COLLECTIONS:
                # Coba kedua kemungkinan nama tabel: dengan prefix dan tanpa prefix
                candidates = list({_tbl(_ns(col_name)), _tbl(col_name)})
                for tbl in candidates:
                    # Cek apakah tabel ini ada di SQLite
                    async with sq.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,)
                    ) as cur:
                        exists = await cur.fetchone()
                    if not exists:
                        continue

                    async with sq.execute(f"SELECT id, doc_id, data FROM {tbl}") as cur:
                        rows = await cur.fetchall()

                    if not rows:
                        continue

                    mongo_col = _mongo_db[_ns(col_name)]
                    inserted  = 0
                    skipped   = 0

                    for row in rows:
                        try:
                            raw = row["data"] if "data" in row.keys() else None
                            if not raw:
                                continue
                            doc = _json.loads(raw) if isinstance(raw, str) else raw

                            # Tentukan _id untuk cek duplikat
                            doc_id = row["doc_id"] if "doc_id" in row.keys() else None
                            if doc_id:
                                doc.setdefault("_id", doc_id)

                            filter_q = {"_id": doc["_id"]} if "_id" in doc else None

                            if filter_q:
                                existing = await mongo_col.find_one(filter_q)
                                if existing:
                                    skipped += 1
                                    continue

                            await mongo_col.insert_one(doc)
                            inserted += 1

                        except Exception:
                            skipped += 1
                            continue

                    if inserted:
                        print(f"[Migrasi SQLite→Mongo] ✅ {tbl} → {_ns(col_name)}: {inserted} dokumen dipindah, {skipped} duplikat dilewati")
                    total_migrated += inserted
                    total_skipped  += skipped

    except Exception as e:
        print(f"[Migrasi SQLite→Mongo] ❌ Error: {e}")
        return

    if total_migrated > 0:
        print(f"[Migrasi SQLite→Mongo] ✅ Selesai. Total {total_migrated} dokumen dipindah, {total_skipped} duplikat dilewati.")
        print(f"[Migrasi SQLite→Mongo] ℹ️  File SQLite ({sqlite_path}) tetap ada sebagai backup.")
        print(f"[Migrasi SQLite→Mongo] ℹ️  Hapus manual jika sudah yakin data aman di MongoDB.")
    else:
        print(f"[Migrasi SQLite→Mongo] ✅ Semua data SQLite sudah ada di MongoDB ({total_skipped} duplikat). Tidak ada yang perlu dipindah.")


async def _dedupe_config_db() -> None:
    """
    Migrasi sekali-jalan (tapi aman dipanggil tiap startup, idempotent,
    no-op kalau datanya sudah bersih): gabungkan semua dokumen config_db
    yang punya chat_id sama menjadi 1 dokumen, sebelum unique index
    dibuat di atasnya.

    Kenapa perlu: create_index(unique=True) akan GAGAL (dan di Collection.
    create_index() di atas, gagalnya DIAM-DIAM lewat except/pass) kalau
    koleksi masih punya dokumen kembar chat_id — index unique tidak akan
    pernah benar-benar terpasang, dan bug grup dobel akan terus muncul
    lagi di masa depan tiap kali race registrasi kejadian lagi.

    Strategi merge per grup yang dobel:
      • Dokumen dengan field terisi paling banyak (non-None) dijadikan
        dasar (paling mungkin adalah dokumen yang paling sering ter-update
        via panel/refresh_group_public_info).
      • Field yang kosong di dasar diisi dari dokumen lain kalau ada.
      • Dokumen selain dasar dihapus.
    """
    if _BACKEND != "mongo":
        return
    try:
        groups: dict[int, list[dict]] = {}
        async for doc in config_db.find({}):
            chat_id = doc.get("chat_id")
            if not chat_id:
                continue
            groups.setdefault(chat_id, []).append(doc)

        for chat_id, docs in groups.items():
            if len(docs) < 2:
                continue  # tidak dobel, tidak perlu apa-apa

            # Pilih dasar: dokumen dengan jumlah field non-None terbanyak.
            docs_sorted = sorted(
                docs,
                key=lambda d: sum(1 for v in d.values() if v is not None),
                reverse=True,
            )
            base = dict(docs_sorted[0])
            for other in docs_sorted[1:]:
                for k, v in other.items():
                    if v is not None and base.get(k) is None:
                        base[k] = v

            base.pop("_id", None)

            # Hapus SEMUA dokumen chat_id ini lalu tulis ulang 1 dokumen
            # bersih — lebih aman daripada delete_many parsial berdasar
            # _id (backend Collection di sini tidak selalu expose _id
            # mentah secara konsisten lintas mongo/sqlite).
            await config_db.delete_many({"chat_id": chat_id})
            await config_db.update_one(
                {"chat_id": chat_id},
                {"$set": base},
                upsert=True,
            )
            print(f"[Dedupe] config_db: {len(docs)} dokumen dobel untuk "
                  f"chat_id={chat_id} digabung jadi 1.")
    except Exception as e:
        print(f"[Dedupe] Gagal bersihkan duplikat config_db: {e}")


async def _create_panel_indexes() -> None:
    """
    Buat index untuk koleksi yang dipakai berulang dari tombol-tombol panel
    grup (chat_id sebagai filter utama, beberapa juga user_id).

    Idempotent — aman dipanggil tiap startup. No-op total di SQLite
    (lihat implementasi Collection.create_index). TIDAK mengubah query,
    hasil, maupun urutan logika apapun di kode lain — index hanya
    membuat MongoDB menemukan dokumen yang sama jauh lebih cepat,
    tanpa full collection scan lintas semua grup setiap kali tombol
    panel grup diklik.
    """
    if _BACKEND != "mongo":
        return
    try:
        from pymongo import ASCENDING  # type: ignore

        # status (config_db) — BUG DUPLIKAT GRUP DI /list & PANEL:
        # index chat_id sebelumnya TIDAK unique, sementara ADA 2 jalur
        # registrasi yang upsert ke chat_id yang sama (bootstrap_groups_
        # from_dialogs saat start bot, DAN ensure_group_registered per
        # pesan grup masuk). Kalau keduanya jalan bersamaan untuk grup
        # yang sama (mis. tepat setelah restart bot), race condition pada
        # update_one(upsert=True) TANPA unique index bisa membuat MongoDB
        # insert 2 dokumen terpisah dengan chat_id identik — grup itu lalu
        # tampil dobel di mana pun daftar grup dibaca dari config_db.
        #
        # Fix 2 lapis:
        #  1) Bersihkan duplikat yang SUDAH ada sekarang (_dedupe_config_db).
        #  2) Baru buat index chat_id sebagai UNIQUE, supaya upsert
        #     berikutnya tidak bisa lagi menghasilkan dokumen kembar
        #     (MongoDB akan menegakkan constraint ini secara atomik).
        await _dedupe_config_db()
        await config_db.create_index([("chat_id", ASCENDING)], unique=True)

        # regex_per_group — panel utama (hitung jumlah filter) & daftar regex
        await db["regex_per_group"].create_index([("chat_id", ASCENDING)])

        # free_per_group — panel utama (hitung VIP) & daftar Member VIP,
        # juga dicek per (chat_id, user_id) di banyak filter pesan.
        await db["free_per_group"].create_index([("chat_id", ASCENDING), ("user_id", ASCENDING)])

        # whitelist_per_group — panel CAS & daftar whitelist,
        # juga dicek per (chat_id, user_id) di filter CAS.
        await db["whitelist_per_group"].create_index([("chat_id", ASCENDING), ("user_id", ASCENDING)])

        # security_os — dibaca _sec_os_get setiap render/toggle panel Security OS
        await db["security_os"].create_index([("chat_id", ASCENDING)])

        # vc_muted_by_ub — dicek tiap /unmutemic dan tiap siklus scan VC
        await db["vc_muted_by_ub"].create_index([("chat_id", ASCENDING), ("user_id", ASCENDING)])

        # vc_onkem_punishment — riwayat strike hukuman onkem berjenjang
        # (Custom Userbot & userbot utama mode eksperimen MAIN_UB_MULTI_VC=1),
        # dicek tiap siklus scan VC untuk auto-unmute & penentuan durasi mute.
        await db["vc_onkem_punishment"].create_index([("chat_id", ASCENDING), ("user_id", ASCENDING)])

        # group_admin_roster — dibaca userbot Security OS tiap siklus scan VC
        # (gantikan panggilan Telegram API langsung — lihat database.py bagian
        # "ADMIN ROSTER" dan security_os/video_call.py _get_group_admin_ids).
        await db["group_admin_roster"].create_index([("chat_id", ASCENDING)], unique=True)

        # seen_messages (messages_db) — dicek SETIAP pesan yang lolos sampai
        # tahap Anti Duplikasi Lokal (core/antispam_queue.py), baik untuk
        # cari pesan lama yang mirip (find().sort("time",-1).limit(N)) maupun
        # untuk hitung total histori user guna pembersihan (all_docs).
        # TANPA index ini, query itu full collection scan — makin lambat
        # makin besar koleksinya (koleksi ini diisi TIAP pesan bersih juga,
        # bukan cuma duplikat, jadi tumbuh terus menerus).
        # Compound index (chat_id, user_id, type, time) mencakup filter
        # SEKALIGUS urutan sort by time tanpa sort tambahan di memori.
        await db["seen_messages"].create_index([
            ("chat_id", ASCENDING), ("user_id", ASCENDING),
            ("type", ASCENDING), ("time", ASCENDING),
        ])
    except Exception as e:
        print(f"[DB] Gagal buat index panel: {e}")


async def setup_db():
    """
    Inisialisasi backend (MongoDB atau SQLite) dan mulai background cleanup.
    Wajib dipanggil sekali di main.py saat startup.
    """
    await _init_backend()

    if _BACKEND == "sqlite":
        conn = await _get_sqlite()
        for col_name in [
            "status", "seen_messages", "regex_list",
            "regex_per_group", "whitelist_per_group", "free_per_group",
            "nexus_kalimat", "nexus_regex", "nexus_grup", "nexus_whitelist",
            "local_mute", "group_action_log",
            "nexus_actlog", "ai_debug_log", "dm_users", "nexus_ai_model", "bot_config", "group_admin_roster",
            "security_os", "security_os_monitors",
            "mention_member_cache",
        ]:
            await _ensure_table(conn, _ns(col_name))

    # ── Migrasi SQLite lokal → MongoDB (jika backend aktif Mongo & SQLite ada) ─
    await _migrate_sqlite_to_mongo()

    asyncio.create_task(_cleanup_seen_messages())
    asyncio.create_task(shard_health_supervisor())

    # ── Migrasi data lama (tanpa CODE_BOT prefix) ke namespace aktif ─────────
    if _CODE_BOT:
        await _migrate_legacy_data()

    # ── Index performa: koleksi yang dipakai berulang dari tombol panel ─────
    # FIX (tombol panel terasa berat): koleksi-koleksi ini di-query dengan
    # filter chat_id (dan/atau user_id) setiap kali tombol grup terkait
    # diklik (panel utama, regex, whitelist, free/VIP, Security OS, dll),
    # tapi tidak punya index sama sekali — di MongoDB artinya full collection
    # scan lintas SEMUA grup setiap klik. Penambahan index ini TIDAK
    # mengubah hasil/logika apapun, hanya membuat query yang SAMA jadi
    # lebih cepat dicari oleh database. Idempotent & no-op di SQLite
    # (lihat Collection.create_index).
    await _create_panel_indexes()

    # ── Banner detail startup ─────────────────────────────────────────────────
    sep = "─" * 52
    print(f"\n╔{sep}╗")
    print(f"║{'  DATABASE INFO':^52}║")
    print(f"╠{sep}╣")

    if _BACKEND == "mongo":
        url_display = MONGO_URL[:45] + "…" if len(MONGO_URL) > 45 else MONGO_URL
        print(f"║  Backend   : MongoDB (cloud/server)              ║")
        print(f"║  URL       : {url_display:<39}║")
        print(f"║  DB Name   : {MONGO_DB_NAME:<39}║")
    else:
        abs_path = os.path.abspath(SQLITE_PATH)
        path_display = abs_path[-45:] if len(abs_path) > 45 else abs_path
        print(f"║  Backend   : SQLite (lokal / Termux)             ║")
        print(f"║  File      : {path_display:<39}║")

    print(f"╠{sep}╣")

    if _CODE_BOT:
        print(f"║  CODE_BOT  : [{_CODE_BOT}]")
        print(f"║  Namespace : semua koleksi pakai prefix [{_CODE_BOT}_…]")
        print(f"║  Akses DB  : bot lain dengan CODE_BOT sama")
        print(f"║              → berbagi data yang sama ✅")
        print(f"║              bot lain dengan CODE_BOT beda")
        print(f"║              → data terpisah, tidak campur ✅")
    else:
        print(f"║  CODE_BOT  : (tidak diset / kosong)")
        print(f"║  ⚠️  PERINGATAN: tanpa CODE_BOT, semua bot yang")
        print(f"║     pakai DB yang sama akan BERBAGI data!")
        print(f"║     Isi CODE_BOT di .env untuk isolasi data.")

    print(f"╚{sep}╝\n")


async def save_bot_config(key: str, value) -> None:
    """
    Simpan setting bot ke DB secara persisten.
    Dipakai untuk cache info channel (title, username) agar dikenal lintas sesi.
    """
    try:
        await bot_config_db.update_one(
            {"_id": key},
            {"$set": {"_id": key, "value": value}},
            upsert=True,
        )
    except Exception as e:
        print(f"[DB] save_bot_config error ({key}): {e}")


async def get_bot_config(key: str, default=None):
    """Ambil setting bot dari DB. Return default jika tidak ada."""
    try:
        doc = await bot_config_db.find_one({"_id": key})
        if doc is not None:
            return doc.get("value", default)
    except Exception as e:
        print(f"[DB] get_bot_config error ({key}): {e}")
    return default


async def reset_code_bot_data(code_bot: str) -> tuple[int, list[str]]:
    """
    Hapus semua data dari namespace CODE_BOT yang diberikan.
    Cocok untuk perintah /reset — membersihkan SEMUA data satu bot.

    Return: (total_dokumen_dihapus, daftar_koleksi_yang_dibersihkan)
    """
    import re as _re2
    safe   = _re2.sub(r"[^a-zA-Z0-9]", "_", code_bot.strip()).lower().strip("_")
    prefix = f"{safe}_" if safe else ""

    _ALL_COLS = [
        "status", "seen_messages", "regex_list",
        "regex_per_group", "whitelist_per_group", "free_per_group",
        "nexus_kalimat", "nexus_regex", "nexus_grup", "nexus_whitelist",
        "nexus_actlog", "local_mute", "group_action_log",
        "ai_debug_log", "dm_users", "nexus_ai_model", "bot_config", "group_admin_roster",
    ]

    cleared: list[str] = []
    total   = 0

    if _BACKEND == "mongo":
        for col_name in _ALL_COLS:
            ns = f"{prefix}{col_name}" if prefix else col_name
            try:
                # Pakai Collection (lewat DB.__getitem__ tanpa _ns ganda — ns
                # di sini SUDAH final) supaya delete_many fan-out ke SEMUA
                # shard untuk collection yang sharded (bio_profiles dkk),
                # bukan hanya shard 0. _resolve_shard_idx menerima query={}
                # yang berarti "tanpa chat_id" → otomatis fan-out semua shard.
                r = await Collection(ns).delete_many({})
                if r.deleted_count > 0:
                    total += r.deleted_count
                    cleared.append(f"{ns} ({r.deleted_count})")
            except Exception as e:
                print(f"[reset] MongoDB error {ns}: {e}")

    elif _BACKEND == "sqlite":
        conn = await _get_sqlite()
        for col_name in _ALL_COLS:
            ns  = f"{prefix}{col_name}" if prefix else col_name
            tbl = _tbl(ns)
            try:
                async with conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,)
                ) as cur:
                    row = await cur.fetchone()
                if not row:
                    continue
                async with conn.execute(f"SELECT COUNT(*) FROM {tbl}") as cur:
                    cnt = (await cur.fetchone())[0]
                await conn.execute(f"DELETE FROM {tbl}")
                if cnt > 0:
                    total += cnt
                    cleared.append(f"{ns} ({cnt})")
            except Exception as e:
                print(f"[reset] SQLite error {ns}: {e}")
        await conn.commit()

    # Bersihkan cache in-memory jika namespace aktif yang direset
    if safe == _CODE_BOT:
        _config_cache.clear()
        _admin_cache.clear()

    return total, cleared


async def close_db():
    """Tutup koneksi database dengan bersih saat shutdown."""
    global _sqlite_conn, _mongo_db
    if _BACKEND == "sqlite" and _sqlite_conn is not None:
        try:
            await _sqlite_conn.close()
            _sqlite_conn = None
            print("[DB] SQLite connection ditutup.")
        except Exception as e:
            print(f"[DB] Error tutup SQLite: {e}")
    elif _BACKEND == "mongo" and _mongo_db is not None:
        try:
            _mongo_db.client.close()
            _mongo_db = None
            print("[DB] MongoDB connection ditutup.")
        except Exception as e:
            print(f"[DB] Error tutup MongoDB: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# CONFIG HELPERS
# ══════════════════════════════════════════════════════════════════════════════

async def get_config(chat_id: int) -> dict:
    now = time.monotonic()
    hit = _config_cache.get(chat_id)
    if hit and (now - hit[1]) < CONFIG_TTL:
        return hit[0]
    doc = await config_db.find_one({"chat_id": chat_id})
    cfg = dict(DEFAULT_CONFIG)
    if doc:
        for k in DEFAULT_CONFIG:
            if k in doc:
                cfg[k] = doc[k]
    _config_cache[chat_id] = (cfg, now)
    return cfg


async def update_config(chat_id: int, key: str, value) -> None:
    await config_db.update_one(
        {"chat_id": chat_id},
        {"$set": {"chat_id": chat_id, key: value}},
        upsert=True,
    )
    _config_cache.pop(chat_id, None)


def update_config_optimistic(
    chat_id: int, key: str, value,
    dm_chat_id: int | None = None, dm_msg_id: int | None = None,
) -> dict:
    """
    Versi "ringan" dari update_config — dipakai oleh tombol panel DM.

    1. Cache di-update LANGSUNG (synchronous) → panggilan get_config()
       berikutnya (dipakai untuk render ulang panel) langsung melihat
       nilai baru tanpa menunggu DB.
    2. Penulisan sesungguhnya ke DB diantrikan via panel_write_queue dan
       dieksekusi belakangan oleh panel_write_worker.
    3. Jika dm_chat_id + dm_msg_id diisi (lokasi pesan panel di DM admin)
       dan penulisan ternyata GAGAL PERMANEN setelah di-retry, worker akan
       mengoreksi tampilan panel itu kembali ke nilai DB yang sebenarnya.

    Return dict config terbaru (hasil optimistic) agar pemanggil bisa
    langsung pakai untuk render tanpa query ulang.
    """
    now = time.monotonic()
    hit = _config_cache.get(chat_id)
    cfg = dict(hit[0]) if hit else dict(DEFAULT_CONFIG)
    cfg[key] = value
    _config_cache[chat_id] = (cfg, now)
    enqueue_config_write(chat_id, key, value, dm_chat_id, dm_msg_id)
    return cfg


# ── Cached count helpers (dipakai oleh page_manage di panel DM) ───────────────

async def get_regex_count(chat_id: int) -> int:
    """Count regex rules untuk grup, dengan cache COUNT_TTL detik."""
    now = time.monotonic()
    hit = _regex_count_cache.get(chat_id)
    if hit and (now - hit[1]) < COUNT_TTL:
        return hit[0]
    n = await db["regex_per_group"].count_documents({"chat_id": chat_id})
    _regex_count_cache[chat_id] = (n, now)
    return n


async def get_free_count(chat_id: int) -> int:
    """Count VIP members untuk grup, dengan cache COUNT_TTL detik."""
    now = time.monotonic()
    hit = _free_count_cache.get(chat_id)
    if hit and (now - hit[1]) < COUNT_TTL:
        return hit[0]
    n = await db["free_per_group"].count_documents({"chat_id": chat_id})
    _free_count_cache[chat_id] = (n, now)
    return n


def invalidate_count_cache(chat_id: int) -> None:
    """Hapus cache count untuk grup ini (panggil saat regex/VIP ditambah/hapus)."""
    _regex_count_cache.pop(chat_id, None)
    _free_count_cache.pop(chat_id, None)


def invalidate_admin_groups_cache(user_id: int) -> None:
    """Paksa refresh daftar grup admin (panggil saat tombol Refresh ditekan)."""
    _admin_groups_cache.pop(user_id, None)


def invalidate_nexus_counts() -> None:
    """Hapus semua cache count nexus — panggil setelah operasi tulis ke nexus collections."""
    global _nexus_kalimat_count_cache, _nexus_regex_count_cache
    global _nexus_wl_count_cache, _nexus_owner_regex_count_cache, _nexus_grup_cache
    _nexus_kalimat_count_cache      = None
    _nexus_regex_count_cache        = None
    _nexus_wl_count_cache           = None
    _nexus_owner_regex_count_cache  = None
    _nexus_grup_cache               = None


# ══════════════════════════════════════════════════════════════════════════════
# BOT PERMISSIONS CACHE
# Cek apakah bot punya can_delete_messages DAN can_restrict_members di sebuah
# grup. Cache 5 menit per grup agar tidak terus-terus hit Telegram API.
# Dipakai oleh antispam.py, antispam_queue.py, dan panel DM (handlers_dm.py).
# ══════════════════════════════════════════════════════════════════════════════

_bot_perm_cache: dict[int, tuple[bool, float]] = {}
_BOT_PERM_CACHE_TTL = 300  # 5 menit — cukup sering refresh, tidak terlalu boros API


async def check_bot_permissions(client, chat_id: int) -> bool:
    """
    Kembalikan True jika bot punya KEDUA izin di grup chat_id:
      • can_delete_messages  — wajib untuk hapus pesan spam
      • can_restrict_members — wajib untuk ban/mute/restrict user

    Jika salah satu tidak ada → False → bot harus skip grup ini sepenuhnya.

    Cache 5 menit per grup. Saat gagal query ke Telegram (error jaringan, dll.)
    → fail-open (return True) agar bot tidak tiba-tiba berhenti di semua grup
    hanya karena Telegram sedang lambat.
    """
    now = _time_module.monotonic()
    cached = _bot_perm_cache.get(chat_id)
    if cached:
        has_perms, ts = cached
        if now - ts < _BOT_PERM_CACHE_TTL:
            return has_perms

    try:
        me     = client.me
        member = await client.get_chat_member(chat_id, me.id)
        privs  = getattr(member, "privileges", None)
        if privs is None:
            # Bot tidak punya privileges objek → bukan admin
            has_perms = False
        else:
            can_del      = getattr(privs, "can_delete_messages",  False) or False
            can_restrict = getattr(privs, "can_restrict_members", False) or False
            has_perms    = bool(can_del and can_restrict)
    except Exception as e:
        print(f"[BotPerm] Gagal cek izin chat={chat_id}: {e} — anggap OK (fail-open)")
        has_perms = True  # fail-open: jangan block bot saat Telegram error
    else:
        # CEK BERHASIL (bukan fail-open, bukan hasil cache) — sinkronkan
        # perm_forced_off di config_db SEKARANG JUGA, jangan tunggu event
        # ChatMemberUpdated (nexus_react_bot_perm_change) atau siklus
        # perm_watchdog (jam-an). Ini gate yang paling sering ke-trigger
        # (tiap pesan masuk di grup ber-cache-habis), jadi paling cepat
        # menutup celah kalau event demote ke-skip (mis. bot lagi restart
        # pas admin cabut izin) — /list & panel jadi tidak pernah lebih
        # basi dari cache 5 menit ini. Fire-and-forget (create_task) supaya
        # tidak menambah latensi ke jalur gating pesan yang lagi nunggu
        # nilai has_perms ini.
        asyncio.create_task(_sync_perm_forced_off(chat_id, has_perms))

    _bot_perm_cache[chat_id] = (has_perms, now)
    return has_perms


async def _sync_perm_forced_off(chat_id: int, has_perms: bool) -> None:
    """
    Helper kecil dipanggil dari check_bot_permissions() begitu live check
    (bukan fail-open) selesai — sinkronkan perm_forced_off ke arah yang
    sesuai, idempotent dan aman dipanggil berkali-kali (kedua fungsi di
    bawah sudah no-op kalau kondisinya memang tidak perlu diubah).
    """
    try:
        if not has_perms:
            await force_disable_group_moderation(chat_id)
        else:
            await restore_group_moderation_if_forced(chat_id)
    except Exception as e:
        print(f"[BotPerm] ⚠️  Gagal sinkron perm_forced_off chat={chat_id}: {e}")


def invalidate_bot_perm_cache(chat_id: int) -> None:
    """Hapus cache izin bot untuk grup ini (misalnya setelah bot di-promote ulang)."""
    _bot_perm_cache.pop(chat_id, None)


# ══════════════════════════════════════════════════════════════════════════════
# PERM WATCHDOG — pengecekan berkala izin ban/mute per grup (lihat core/perm_watchdog.py)
#
# Berbeda dari check_bot_permissions() (reaktif, dipicu pesan masuk, cache 5 menit,
# fail-open saat error API): watchdog ini PROAKTIF, jalan terus berkala bahkan
# untuk grup yang sepi pesan, dan saat izin ban/mute hilang ia menulis status
# OFF ke DATABASE (bukan cache sesaat) — sehingga toggle panel ikut berubah dan
# kondisinya bertahan walau cache check_bot_permissions kosong/fail-open.
# ══════════════════════════════════════════════════════════════════════════════

blocked_groups_db = db["blocked_groups"]

# Toggle yang menjalankan EKSEKUSI hapus/ban — dipaksa OFF saat kuasa ban/mute
# hilang. TIDAK termasuk: ubot_detect (rekam selalu jalan, independen),
# anti_mention/mention record, dan Nexus AI passive learning — ketiganya
# sudah didesain untuk tetap jalan tanpa izin admin (lihat masing-masing filter).
# PENTING (fix bug "moderasi dipaksa off tapi bot masih hapus pesan"):
# anti_link & anti_mention adalah toggle TERPISAH dari local/global/cas,
# default True, dan gate-nya di core/antispam_queue.py (Gate B link
# detector, gate mention) HANYA mengecek cfg.get("anti_link"/"anti_mention")
# — tidak pernah mengecek perm_forced_off sama sekali. Kalau keduanya tidak
# ikut dipaksa off di sini, satu-satunya penjaga yang mencegah keduanya
# tetap menghapus pesan adalah check_bot_permissions() (database.py) —
# yang melakukan live API call SENDIRI dengan cache 5 menit dan FAIL-OPEN
# saat API error. Kalau live check itu sesaat salah (cache belum invalidate,
# atau get_chat_member() error lalu fail-open), link/mention detector tetap
# jalan HAPUS PESAN walau perm_forced_off=True & panel sudah benar
# menunjukkan "moderasi dipaksa off". Makanya sekarang ikut dipaksa off di
# sini juga — belt-and-suspenders, bukan cuma andalkan 1 lapis pertahanan.
_PERM_GATED_TOGGLES = ("local", "global", "cas", "anti_link", "anti_mention", "anti_flood")


async def force_disable_group_moderation(chat_id: int) -> bool:
    """
    Paksa matikan toggle yang berujung pada eksekusi hapus pesan/ban
    (local, global, cas) untuk chat_id, dan tandai grup ini sebagai
    "dimatikan oleh watchdog" (perm_forced_off=True) — bukan oleh admin grup.

    Return True jika ada perubahan (sebelumnya minimal satu toggle ON),
    False jika grup sudah dalam keadaan OFF semua (tidak ada yang berubah).
    """
    cfg = await get_config(chat_id)
    already_off = not any(cfg.get(k) for k in _PERM_GATED_TOGGLES)

    if already_off:
        # PENTING: JANGAN tandai perm_forced_off=True di sini. Kalau semua
        # toggle memang sudah OFF (termasuk kasus grup BARU — bot ditambah
        # dulu sebagai member biasa sebelum dipromosikan admin, jadi handler
        # ini kepanggil duluan saat belum ada apa-apa untuk "dipaksa mati"),
        # menandai perm_forced_off=True di sini membuat
        # restore_group_moderation_if_forced() nanti (begitu bot dapat admin
        # beberapa detik kemudian) mengira ini grup yang PERNAH aktif lalu
        # dimatikan watchdog → otomatis menyalakan local/global=True padahal
        # admin grup belum pernah menekan tombol apapun. Cukup pastikan cache
        # bot-perm ter-invalidate; tidak perlu tulis apapun ke DB.
        invalidate_bot_perm_cache(chat_id)
        return False

    updates = {k: False for k in _PERM_GATED_TOGGLES}
    updates["perm_forced_off"] = True
    await config_db.update_one(
        {"chat_id": chat_id},
        {"$set": {"chat_id": chat_id, **updates}},
        upsert=True,
    )
    _config_cache.pop(chat_id, None)
    invalidate_bot_perm_cache(chat_id)

    print(f"[PermWatchdog] ⛔ Grup {chat_id}: kuasa ban/mute hilang — "
          f"MODERASI OTOMATIS DIPAKSA OFF SELURUHNYA (toggle "
          f"{'/'.join(_PERM_GATED_TOGGLES)} di-nonaktifkan di DB; "
          f"deteksi ubot & Nexus AI ikut berhenti eksekusi hapus/ban lewat "
          f"cek perm_forced_off, walau toggle ubot_detect/anti_spam_ai "
          f"sendiri tidak ikut di-set False).")
    return True


async def restore_group_moderation_if_forced(chat_id: int) -> bool:
    """
    Nyalakan kembali local/global ke True HANYA jika grup ini sebelumnya
    dimatikan oleh watchdog (perm_forced_off=True) — tidak menimpa pilihan
    admin grup yang mematikan fitur secara manual.

    `cas` SENGAJA TIDAK di-restore otomatis — CAS adalah auto-ban global
    yang lebih sensitif, biar admin grup yang menyalakan ulang secara sadar
    lewat panel setelah memverifikasi izin bot benar-benar pulih.

    Return True jika grup ini di-restore, False jika tidak perlu (tidak
    pernah di-force-off oleh watchdog).
    """
    # PENTING: baca config_db LANGSUNG di sini, bukan get_config(). Sejak
    # perm_forced_off dimasukkan ke DEFAULT_CONFIG (dipakai _process_detection
    # sebagai lapis pertahanan kedua), get_config() SUDAH ikut mengembalikan
    # field ini — tapi lewat _config_cache yang punya TTL (CONFIG_TTL), jadi
    # bisa telat sesaat. Fungsi restore ini butuh nilai TERBARU saat itu juga
    # (dipanggil begitu perm_watchdog/nexus_react_bot_perm_change mendeteksi
    # izin pulih), makanya tetap query config_db apa adanya, tidak lewat cache.
    doc = await config_db.find_one({"chat_id": chat_id})
    if not doc or not doc.get("perm_forced_off"):
        return False

    await config_db.update_one(
        {"chat_id": chat_id},
        {"$set": {
            "local": True, "global": True,
            "anti_link": True, "anti_mention": True,
            "anti_flood": True,
            "perm_forced_off": False,
        }},
    )
    _config_cache.pop(chat_id, None)
    print(f"[PermWatchdog] ✅ Grup {chat_id}: kuasa ban/mute pulih — "
          f"MODERASI OTOMATIS PULIH SELURUHNYA (toggle local/global/"
          f"anti_link/anti_mention/anti_flood diaktifkan lagi di DB; deteksi ubot & "
          f"Nexus AI otomatis ikut eksekusi hapus/ban lagi karena "
          f"perm_forced_off=False; cas TETAP OFF, perlu manual).")
    return True


async def confirm_perm_ok(chat_id: int) -> None:
    """
    Dipanggil perm_watchdog begitu chat_id kedeteksi PUNYA kuasa penuh
    (can_delete_messages + can_restrict_members) di siklus ini.

    - Tandai ever_had_ban_perm=True (idempotent, aman dipanggil tiap
      siklus) — supaya kalau nanti izin dicabut, watchdog tahu ini bukan
      grup baru yang belum pernah di-admin-in.
    - Reset perm_lost_strikes ke 0 — hitungan "berapa siklus berturut
      kehilangan izin" jadi basi begitu izin terbukti utuh lagi.
    """
    await config_db.update_one(
        {"chat_id": chat_id},
        {"$set": {
            "chat_id": chat_id,
            "ever_had_ban_perm": True,
            "perm_lost_strikes": 0,
        }},
        upsert=True,
    )
    _config_cache.pop(chat_id, None)


async def register_perm_lost(chat_id: int) -> dict:
    """
    Dipanggil perm_watchdog begitu chat_id kedeteksi TIDAK punya kuasa
    penuh di siklus ini. Increment perm_lost_strikes +1, lalu kembalikan
    state relevan dalam SATU pembacaan supaya watchdog bisa memutuskan
    langkah selanjutnya tanpa round-trip tambahan:

    Return: {
        "ever_had_ban_perm": bool,  # pernah confirmed admin sebelumnya?
        "strikes":           int,   # jumlah siklus berturut kehilangan izin (setelah increment ini)
        "invited_by":        int | None,  # pelaku add bot ke grup ini
        "title":             str,
    }
    """
    await config_db.update_one(
        {"chat_id": chat_id},
        {"$inc": {"perm_lost_strikes": 1}, "$set": {"chat_id": chat_id}},
        upsert=True,
    )
    doc = await config_db.find_one({"chat_id": chat_id}) or {}
    _config_cache.pop(chat_id, None)
    return {
        "ever_had_ban_perm": bool(doc.get("ever_had_ban_perm", False)),
        "strikes":           int(doc.get("perm_lost_strikes", 1)),
        "invited_by":        doc.get("invited_by"),
        "title":             doc.get("title") or "",
    }


async def save_group_invited_by(chat_id: int, user_id: int | None) -> None:
    """
    Simpan siapa yang menambahkan bot ke grup ini (pelaku add di service
    message new_chat_members, dipanggil dari handle_bot_join). Dipakai
    perm_watchdog untuk DM notifikasi kalau bot auto-leave karena izin
    ban dicabut. Tidak menimpa apapun kalau user_id kosong (join tanpa
    pelaku add yang jelas) — biarkan fallback ke OWNER_ID nanti.
    """
    if not user_id:
        return
    await config_db.update_one(
        {"chat_id": chat_id},
        {"$set": {"chat_id": chat_id, "invited_by": user_id}},
        upsert=True,
    )
    _config_cache.pop(chat_id, None)


# ══════════════════════════════════════════════════════════════════════════════
# MONITOR WATCHDOG — pengecekan berkala status bot pemantau per grup
# (lihat core/monitor_watchdog.py).
#
# Beda dari _monitor_privacy_block() (plugins/ui/pages.py) yang HANYA menyamarkan
# TAMPILAN panel jadi merah tanpa mengubah nilai DB: watchdog ini menulis
# status OFF ke DATABASE sungguhan begitu bot pemantau terdeteksi
# offline/dikick/Privacy Mode ON DI TENGAH JALAN — supaya seluruh pipeline
# yang membaca cfg["bio_check"]/cfg["mention_batasi_*"] langsung dari config_db
# (mis. database.py mention_pending_resolve_loop, core/mention_bio_scan.py,
# plugins/filters/bio.py) ikut berhenti, bukan cuma tampilan panel yang
# kelihatan merah padahal DB-nya masih True.
# ══════════════════════════════════════════════════════════════════════════════

# Toggle panel "Bio Cek & Mention" — SEMUANYA sama-sama mensyaratkan bot
# pemantau grup terpasang & siap (lihat _gated_keys di
# plugins/ui/handlers_dm.py::cb_toggle), jadi semuanya dipaksa OFF bareng
# begitu bot pemantau terdeteksi tidak siap lagi.
_MONITOR_GATED_TOGGLES = (
    "bio_check", "mention_batasi_channel", "mention_batasi_grup", "mention_batasi_akun",
)


async def force_disable_bio_mention_features(chat_id: int) -> bool:
    """
    Paksa matikan seluruh toggle "Bio Cek & Mention" (bio_check,
    mention_batasi_channel/grup/akun) untuk chat_id, dan tandai grup ini
    sebagai "dimatikan oleh monitor watchdog" (monitor_forced_off=True).

    Dipanggil watchdog begitu bot pemantau grup ini terdeteksi:
      - tidak pernah dipasang (monitor_bot_id == 0), ATAU
      - sudah dipasang tapi keluar/dikick dari grup, ATAU
      - masih member tapi Privacy Mode kemungkinan masih ON (belum pernah
        terbukti terima pesan biasa — monitor_privacy_ok masih False).

    SENGAJA TIDAK ada fungsi restore otomatis (beda dari
    restore_group_moderation_if_forced) — begitu dipaksa OFF, fitur ini
    HANYA bisa dinyalakan lagi lewat admin menekan tombol ON secara manual
    di panel, dan cb_toggle akan re-validasi monitor_ready + privacy OK
    saat itu juga sebelum benar-benar mengizinkan nyala.

    Return True jika ada perubahan (sebelumnya minimal satu toggle ON),
    False jika grup sudah OFF semua (tidak ada yang berubah).
    """
    cfg = await get_config(chat_id)
    already_off = not any(cfg.get(k) for k in _MONITOR_GATED_TOGGLES)

    if already_off:
        return False

    updates = {k: False for k in _MONITOR_GATED_TOGGLES}
    # anti_mention master toggle mengikuti OR sub-toggle mention (sama
    # seperti logika sinkronisasi di cb_toggle) — semua sub sudah False →
    # master ikut False.
    updates["anti_mention"]      = False
    updates["monitor_forced_off"] = True
    await config_db.update_one(
        {"chat_id": chat_id},
        {"$set": {"chat_id": chat_id, **updates}},
        upsert=True,
    )
    _config_cache.pop(chat_id, None)

    print(f"[MonitorWatchdog] ⛔ Grup {chat_id}: bot pemantau offline/belum "
          f"siap — PAKSA OFF seluruh fitur 'Bio Cek & Mention' "
          f"({'/'.join(_MONITOR_GATED_TOGGLES)}) di DB. Admin harus menekan "
          f"ON manual lagi setelah bot pemantau terpasang & Privacy Mode "
          f"disabled.")
    return True


async def block_and_remove_group(chat_id: int, reason: str) -> None:
    """
    Grup tidak ditemukan lagi (bot dikick / grup mati / dihapus). Hapus data
    operasionalnya dari config_db (lewat remove_group_data) DAN catat
    permanen ke blocked_groups agar grup ini tidak diam-diam terdaftar lagi
    kalau suatu saat bot diundang balik tanpa sepengetahuan owner.
    """
    await blocked_groups_db.update_one(
        {"chat_id": chat_id},
        {"$set": {
            "chat_id":    chat_id,
            "reason":     reason,
            "blocked_at": datetime.now(TZ_WIB),
        }},
        upsert=True,
    )
    await remove_group_data(chat_id)
    print(f"[PermWatchdog] 🚫 Grup {chat_id} diblokir & dihapus dari DB ({reason}).")


async def is_group_blocked(chat_id: int) -> bool:
    """True jika chat_id ada di daftar blocked_groups."""
    doc = await blocked_groups_db.find_one({"chat_id": chat_id})
    return doc is not None


async def get_all_known_group_ids() -> list[int]:
    """
    Kembalikan semua chat_id grup yang dikenal di config_db (bukan channel),
    dipakai watchdog untuk iterasi bergilir. Channel (chat_type=CHANNEL
    tersimpan) dikecualikan karena tidak relevan dengan moderasi grup.
    """
    ids: list[int] = []
    async for doc in config_db.find({}):
        chat_id = doc.get("chat_id")
        if not chat_id:
            continue
        if (doc.get("chat_type") or "").upper() == "CHANNEL":
            continue
        ids.append(chat_id)
    return ids


async def get_all_groups_with_perm_status() -> list[dict]:
    """
    Kembalikan SEMUA grup yang memakai bot (sumber: config_db — bukan
    nexus_grup_db yang isinya hanya grup yang pernah trigger /spam atau
    event member-update), beserta status izin ban/mute TERKINI.

    Dipakai oleh panel "Grup Terdaftar" (Nexus AI > Owner Bot) supaya owner
    tidak melihat daftar yang menyesatkan — grup tanpa izin ban/mute (sudah
    di-force-off oleh perm_watchdog) HARUS terlihat statusnya, bukan tampil
    polos seolah normal.

    Grup yang sudah mati/bot-dikick TIDAK akan muncul di sini sama sekali,
    karena perm_watchdog menghapusnya dari config_db (lihat
    block_and_remove_group) — daftar ini otomatis hanya berisi grup yang
    masih aktif memakai bot saat ini.

    Channel dikecualikan (sama seperti get_all_known_group_ids).

    Setiap item:
      {
        "chat_id":         int,
        "title":           str,
        "username":        str | None,
        "invite_link":     str | None,  # hanya terisi untuk grup privat
        "has_ban_perm":    bool,  # status TERKINI (perm_forced_off==False)
        "forced_off":      bool,  # True jika watchdog yang mematikan, bukan admin
      }
    """
    # DEDUPE DEFENSIF: config_db seharusnya 1 dokumen per chat_id, tapi
    # race condition upsert (dua jalur registrasi — bootstrap_groups_from_dialogs
    # & ensure_group_registered — jalan bersamaan tanpa unique index) bisa
    # menghasilkan 2+ dokumen dengan chat_id sama. _create_panel_indexes()
    # sudah membersihkan & mencegah ini via unique index, tapi dedupe di sini
    # tetap dipasang sebagai lapis kedua supaya /list & panel TIDAK PERNAH
    # menampilkan grup dobel, apa pun kondisi datanya saat ini.
    by_chat_id: dict[int, dict] = {}
    async for doc in config_db.find({}):
        chat_id = doc.get("chat_id")
        if not chat_id:
            continue
        if (doc.get("chat_type") or "").upper() == "CHANNEL":
            continue

        existing = by_chat_id.get(chat_id)
        if existing is None:
            by_chat_id[chat_id] = doc
        else:
            # Gabungkan: field yang kosong di dokumen pertama diisi dari
            # dokumen berikutnya (mis. username/invite_link baru masuk
            # belakangan dari perm_watchdog), tanpa menimpa field yang
            # sudah terisi.
            for k, v in doc.items():
                if v is not None and existing.get(k) is None:
                    existing[k] = v

    result: list[dict] = []
    for chat_id, doc in by_chat_id.items():
        forced_off = bool(doc.get("perm_forced_off", False))
        result.append({
            "chat_id":      chat_id,
            "title":        doc.get("title") or str(chat_id),
            "username":     doc.get("username"),
            "invite_link":  doc.get("invite_link"),
            "has_ban_perm": not forced_off,
            "forced_off":   forced_off,
        })
    return result


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN CACHE
# ══════════════════════════════════════════════════════════════════════════════

async def is_admin(client, chat_id: int, user_id) -> bool:
    if not user_id:
        return False
    now = time.monotonic()
    key = (chat_id, user_id)
    hit = _admin_cache.get(key)
    if hit and (now - hit[1]) < ADMIN_TTL:
        return hit[0]
    try:
        member = await client.get_chat_member(chat_id, user_id)
        result = member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
    except Exception as e:
        result = False
    _admin_cache[key] = (result, now)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN ROSTER — daftar admin per grup, PERSISTEN ke DB (bukan in-memory saja).
# ══════════════════════════════════════════════════════════════════════════════
# Tujuan: userbot Security OS (security_os/video_call.py) butuh tahu siapa saja
# admin grup untuk skip mereka dari pengecekan VC — sebelumnya userbot query
# Telegram API sendiri (get_chat_members ADMINISTRATORS) tiap 5 menit per grup
# lewat akun MTProto-nya. Sekarang datanya dibaca dari sini (DB bersama),
# diisi/disinkronkan REAKTIF oleh bot utama lewat on_chat_member_updated
# (lihat plugins/nexus/nexus_group.py) — userbot jadi tidak perlu panggil API
# sama sekali untuk keperluan ini di kondisi normal.
#
# doc schema: {chat_id, admin_ids: [int, ...], updated_at: float}
#
# PENTING — beda None vs set kosong:
#   None       = roster BELUM PERNAH dibuat untuk grup ini (belum di-bootstrap)
#   set()      = roster SUDAH ada, dan memang grup ini terdeteksi 0 admin
# Caller (userbot) HARUS treat ini beda: None → boleh fallback ke API sekali
# lalu bootstrap; set kosong → percaya saja, JANGAN fallback ke API.

async def get_admin_roster(chat_id: int) -> "set[int] | None":
    """Baca roster admin grup dari DB. None jika belum pernah di-bootstrap."""
    try:
        doc = await db["group_admin_roster"].find_one({"chat_id": chat_id})
    except Exception as e:
        print(f"[AdminRoster] Gagal baca roster grup {chat_id}: {e}")
        return None
    if doc is None:
        return None
    return set(doc.get("admin_ids") or [])


async def set_admin_roster(chat_id: int, admin_ids: "set[int]") -> None:
    """Timpa penuh roster admin grup ini. Dipakai saat bootstrap/reconcile."""
    try:
        await db["group_admin_roster"].update_one(
            {"chat_id": chat_id},
            {"$set": {
                "chat_id":    chat_id,
                "admin_ids":  sorted(admin_ids),
                "updated_at": time.time(),
            }},
            upsert=True,
        )
    except Exception as e:
        print(f"[AdminRoster] Gagal simpan roster grup {chat_id}: {e}")

    # ── Force-flash berdasar admin ID tertentu (core/admin_flash_watch.py) ──
    # Jalur TERPISAH dari sistem boost donasi/trial — lihat docstring modul
    # itu. Dipanggil di sini karena set_admin_roster() adalah SATU-SATUNYA
    # titik tulis roster (dipakai bootstrap, reconciliation harian, DAN
    # update reaktif promote/demote) — jadi override ini otomatis ikut
    # tersinkron dari jalur manapun tanpa perlu hook tambahan di tempat lain.
    try:
        from core.admin_flash_watch import sync_flash_state
        await sync_flash_state(chat_id, admin_ids)
    except Exception as e:
        print(f"[AdminFlash] gagal sync override grup {chat_id}: {e}")


async def admin_roster_upsert_user(chat_id: int, user_id: int, is_admin_now: bool, client=None) -> None:
    """
    Update 1 user_id di roster (promote → tambah, demote → hapus) TANPA
    full rescan — dipanggil reaktif dari on_chat_member_updated.

    Jika roster grup ini belum pernah dibuat (None) DAN `client` disediakan,
    langsung bootstrap penuh SEKARANG JUGA (full scan admin grup ini) alih-
    alih diabaikan diam-diam — supaya fitur yang bergantung ke roster (mis.
    Admin-Flash, lihat core/admin_flash_watch.py) tidak nunggu sampai
    reconciliation harian (bisa sampai 24 jam) buat grup yang rosternya
    kebetulan belum pernah ke-bootstrap. Hasil bootstrap itu sendiri sudah
    mencerminkan event promote/demote yang baru saja terjadi (Telegram sudah
    menerapkan perubahannya duluan sebelum update ini terkirim), jadi tidak
    perlu apply user_id secara manual lagi setelah bootstrap.

    Jika `client` tidak disediakan (caller lama yang belum di-update) dan
    roster masih None, perilaku lama tetap dipakai: diabaikan diam-diam,
    nanti otomatis terisi lewat bootstrap_admin_roster() saat grup pertama
    kali dikenali, atau lewat reconciliation harian.
    """
    current = await get_admin_roster(chat_id)
    if current is None:
        if client is not None:
            await bootstrap_admin_roster(client, chat_id)
        return
    if is_admin_now:
        current.add(user_id)
    else:
        current.discard(user_id)
    await set_admin_roster(chat_id, current)


_bootstrap_admin_roster_in_progress: set[int] = set()


async def bootstrap_admin_roster(client, chat_id: int) -> "set[int]":
    """
    Full scan admin grup via bot utama (client), lalu simpan sebagai roster
    baru (menimpa yang lama). Dipanggil saat:
      - Grup baru pertama kali dikenali bot (nexus_tracking_grup).
      - Reconciliation harian (admin_roster_reconcile_loop) — jaring pengaman
        kalau ada event promote/demote yang ter-skip (mis. bot restart/down).
      - Fallback on-demand kalau roster ternyata masih kosong saat event
        promote/demote reaktif masuk (lihat admin_roster_upsert_user()).

    Guard `_bootstrap_admin_roster_in_progress` — cegah 2 scan API
    (get_chat_members) jalan BERSAMAAN untuk grup yang SAMA kalau kebetulan
    ada beberapa event promote/demote beruntun dalam waktu berdekatan untuk
    grup itu. Panggilan yang datang belakangan cukup menunggu sebentar lalu
    baca hasil yang barusan disimpan scan pertama — TIDAK ikut memanggil API
    lagi. Ini TIDAK mengubah kapan/kenapa bootstrap dipicu, cuma mencegah
    duplikasi kerja untuk grup yang sama di momen yang sama.

    Return set kosong (dan tidak menyimpan apapun) jika scan gagal total —
    lebih aman biarkan roster lama/absen daripada menimpanya dengan data
    kosong yang salah.
    """
    if chat_id in _bootstrap_admin_roster_in_progress:
        for _ in range(50):          # tunggu maks ~5 detik scan yang sudah jalan
            await asyncio.sleep(0.1)
            if chat_id not in _bootstrap_admin_roster_in_progress:
                break
        existing = await get_admin_roster(chat_id)
        return existing if existing is not None else set()

    _bootstrap_admin_roster_in_progress.add(chat_id)
    try:
        from pyrogram.enums import ChatMembersFilter
        admin_ids: set[int] = set()
        try:
            async for member in client.get_chat_members(chat_id, filter=ChatMembersFilter.ADMINISTRATORS):
                if member.user and member.user.id:
                    admin_ids.add(member.user.id)
        except Exception as e:
            print(f"[AdminRoster] Bootstrap gagal grup {chat_id}: {e}")
            return set()
        await set_admin_roster(chat_id, admin_ids)
        print(f"[AdminRoster] Bootstrap grup {chat_id}: {len(admin_ids)} admin disimpan ke roster.")
        return admin_ids
    finally:
        _bootstrap_admin_roster_in_progress.discard(chat_id)


async def admin_roster_reconcile_loop(client) -> None:
    """
    Jaring pengaman harian: re-scan penuh admin roster SEMUA grup dikenal.

    Kenapa masih perlu ini walau sudah ada update reaktif per-event:
    Telegram TIDAK backfill ChatMemberUpdated yang terlewat saat bot mati/
    restart — jadi kalau ada promote/demote yang terjadi persis di window
    downtime, roster bisa basi selamanya tanpa reconciliation ini.

    Jeda antar grup kecil (2 detik) supaya tidak membanjiri Telegram API
    sekaligus untuk banyak grup — ini SATU-SATUNYA sumber panggilan API di
    seluruh sistem admin-roster, dan cuma jalan 1x/hari.
    """
    while True:
        try:
            await asyncio.sleep(86400)  # 24 jam
            ids = await get_all_known_group_ids()
            print(f"[AdminRoster] Reconciliation harian dimulai — {len(ids)} grup.")
            for chat_id in ids:
                try:
                    await bootstrap_admin_roster(client, chat_id)
                except Exception as e:
                    print(f"[AdminRoster] Reconcile gagal grup {chat_id}: {e}")
                await asyncio.sleep(2)
            print("[AdminRoster] Reconciliation harian selesai.")
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[AdminRoster] Reconcile loop error: {e}")


# ══════════════════════════════════════════════════════════════════════════════

# AUTO DELETE / DELETE WORKER
# ══════════════════════════════════════════════════════════════════════════════

async def auto_delete_reply(msgs: list, delay: int = 5) -> None:
    """
    Hapus pesan setelah `delay` detik.

    FIXED: Sebelumnya memanggil m.delete() satu per satu di sini, yang
    artinya setiap coroutine yang 'await auto_delete_reply(...)' tidur
    selama delay detik lalu menembak N delete calls individual setelah
    bangun — jika banyak handler bangun bersamaan (raid CAS, settings
    beruntun), hasilnya burst ke API.

    Sekarang: setelah tidur, pesan dikelompokkan per chat_id dan dikirim
    sekaligus ke delete_queue (worker yang sudah ada). delete_worker akan
    menggabungkannya menjadi 1 call delete_messages per chat — aman dari flood.
    """
    await asyncio.sleep(delay)
    # Kelompokkan per chat agar bisa dimasukkan sebagai 1 item queue per chat
    grouped: dict[int, list[int]] = {}
    for m in msgs:
        try:
            cid = m.chat.id
            mid = m.id
            grouped.setdefault(cid, []).append(mid)
        except Exception:
            pass
    for cid, mids in grouped.items():
        try:
            await delete_queue.put((cid, mids))
        except Exception:
            pass


async def schedule_welcome_delete(chat_id: int, message_id: int, delay: int) -> None:
    """
    Jadwalkan hapus pesan welcome ke DB (bukan asyncio.sleep in-memory).

    KENAPA BEDA DARI auto_delete_reply/delete_queue:
    Pesan welcome dikirim oleh bot PEMBANTU (token/Client terpisah dari bot
    utama `app`) — bot pembantu hanya boleh hapus pesannya sendiri (tidak
    admin), dan delete_queue/delete_worker di atas terikat ke client bot
    utama. Kalau pakai asyncio.sleep(delay) murni di memori, redeploy
    Railway sebelum timer selesai = pesan welcome nyangkut permanen (pola
    bug yang sama seperti group_action_log_db tanpa cleanup worker).

    Disimpan ke DB supaya welcome_delete_sweep_loop (jalan di proses bot
    utama, tapi mengeksekusi lewat client bot pembantu yang sesuai
    chat_id) bisa lanjutkan jadwal ini walau proses sempat restart.
    """
    now = time.time()
    await welcome_delete_db.insert_one({
        "chat_id":    chat_id,
        "message_id": message_id,
        "delete_at":  now + max(1, int(delay)),
        "created_at": now,
    })


async def delete_worker(client) -> None:
    """
    Worker hapus pesan berbasis per-grup scheduler EVENT-DRIVEN.

    ARSITEKTUR:
      - 1 dispatcher: baca delete_queue, routing ke bucket per cid.
      - Tiap cid aktif punya 1 asyncio.Task scheduler independen.
      - Scheduler TIDAK polling periodik — dia menunggu event "ada pesan
        baru" (asyncio.Event per-cid, di-set oleh dispatcher). Begitu ada
        mid masuk bucket, scheduler langsung bangun, lalu menunggu PENUH
        interval kelompoknya (flash/slow) sebelum eksekusi delete_messages.
      - Jeda hapus SELALU konsisten sesuai interval kelompoknya (flash/slow)
        — baik untuk spam sporadis (jarang-jarang) MAUPUN burst beruntun,
        bukan cuma "rate-limit floor" yang cuma kerasa saat burst. Burst
        yang masuk SELAMA jeda tunggu otomatis ikut ke-batch ke delete yang
        sama (lihat _group_scheduler).
      - 2 KELOMPOK KECEPATAN (mode flash vs mode slow) — HANYA berlaku di
        sini (delete_worker/kecepatan HAPUS). Worker DETEKSI di
        core/antispam_queue.py (_effective_max_workers) SUDAH TIDAK ikut
        dibagi flash/slow lagi — deteksi selalu jalan di speed maksimal
        untuk semua grup. delete_worker membedakan rate-limit floor per
        grup berdasarkan status boost "Upgrade Speed" (core/speed_boost.py):
          • Grup FLASH (is_group_boosted(cid) True)  → DELETE_INTERVAL_FLASH
          • Grup SLOW  (tidak sedang boost)           → DELETE_INTERVAL_SLOW
        Status ini dicek ULANG setiap siklus scheduler (bukan sekali di
        awal), jadi begitu boost grup habis (atau baru dipasang), grup itu
        otomatis pindah kelompok kecepatan TANPA perlu respawn task.
        Kedua kelompok saling eksklusif — grup flash tidak pernah masuk
        hitungan slot slow, begitu juga sebaliknya (lihat _spawn_all).
      - Saat grup baru masuk atau grup di-drop → respawn semua task.
      - STANDBY BERBASIS AKTIVITAS GRUP (bukan cuma spam): setiap pesan
        yang lewat _process_detection — spam ATAUPUN bukan — mengirim item
        (cid, []) ke delete_queue sebagai sinyal "keepalive". Dispatcher
        memperlakukannya sama seperti item delete biasa (spawn scheduler
        kalau grup ini baru, reset idle timer), hanya saja bucket-nya tetap
        kosong (tidak ada apa-apa untuk dihapus). Efeknya: scheduler grup
        sudah standby/hidup SEBELUM ada spam yang perlu dihapus — begitu
        spam beneran ketemu, tidak perlu cold-start worker dulu.
      - Grup dianggap idle & di-drop HANYA kalau BENAR-BENAR nol pesan sama
        sekali (bukan cuma nol spam) selama DELETE_IDLE_SECS detik
        berturut-turut — karena keepalive di atas terus me-reset idle timer
        selama grup masih ada pesan masuk, spam atau tidak.
      - Antar grup tidak saling tunggu — jadwal tiap grup independen.

    ENV:
      DELETE_INTERVAL_FLASH  default 0.4  — jarak minimum antar delete_messages,
                                             grup yang SEDANG boost "Upgrade Speed"
      DELETE_INTERVAL_SLOW   default 1.0  — jarak minimum antar delete_messages,
                                             grup default/tidak sedang boost
      DELETE_CATEGORY_JITTER default 0.01 — jitter slot awal supaya kelompok
                                             flash & slow tidak fire di detik
                                             yang sama persis (lihat _spawn_all)
      DELETE_IDLE_SECS       default 300  — berapa detik TANPA PESAN SAMA SEKALI (bukan cuma tanpa spam) sebelum drop
      DELETE_BATCH_WINDOW    default 0.03 — window kumpul spam sebelum dispatch (detik)
      DELETE_IDLE_TIMEOUT    default 5.0  — poll dispatcher saat queue kosong (detik)

    CATATAN TUNING (latensi kirim→hapus):
      Rate-limit floor ini adalah rem LOKAL per-grup yang berlaku SELALU,
      terlepas dari kondisi API sebenarnya. Padahal sudah ada rem REAKTIF
      terpisah (set_global_flood_backoff / wait_global_flood_backoff di
      bawah) yang baru aktif saat Telegram BENAR-BENAR membalas FloodWait.
      Dua-duanya tetap jalan bareng — jadi grup rame (flash) tidak perlu
      nunggu rem lokal yang lebih ketat dari yang Telegram sendiri minta;
      kalau memang kena FloodWait beneran, rem reaktif itu yang ambil alih
      (mundur global, bukan cuma 1 grup/1 kelompok).

    JITTER ANTAR-KELOMPOK (flash vs slow):
      _spawn_all menghitung slot_offset AWAL secara terpisah per kelompok —
      kelompok slow mendapat tambahan DELETE_CATEGORY_JITTER di atas slot
      awalnya sendiri. ini murni jitter startup (hindari 2 kelompok yang
      punya interval beda tapi kebetulan align di detik yang sama saat baru
      spawn) — bukan rate-limit tambahan, dan tidak mempengaruhi rate-limit
      floor masing-masing grup sama sekali.
    """
    _DELETE_INTERVAL_FLASH  = float(os.environ.get("DELETE_INTERVAL_FLASH",  0.4))
    _DELETE_INTERVAL_SLOW   = float(os.environ.get("DELETE_INTERVAL_SLOW",   1.0))
    _CATEGORY_JITTER        = float(os.environ.get("DELETE_CATEGORY_JITTER", 0.01))
    _DELETE_IDLE_SECS    = float(os.environ.get("DELETE_IDLE_SECS",    300.0))
    _BATCH_WINDOW        = float(os.environ.get("DELETE_BATCH_WINDOW", 0.03))
    _IDLE_TIMEOUT        = float(os.environ.get("DELETE_IDLE_TIMEOUT", 5.0))
    # FIX: _MAX_PENDING_PER_GROUP (dulu 10) DIHAPUS — sebelumnya bucket
    # per-grup dipotong ke 10 mid terbaru, sisanya di-drop diam-diam kalau
    # bucket-nya penuh. Ini kontradiktif dengan proteksi flood RAM
    # (Proteksi A/B/C, plugins/filters/antispam.py) yang justru dirancang
    # supaya SEMUA pesan flood tersapu, sekalipun jumlahnya banyak sekaligus
    # (mis. serangan multi-akun puluhan pesan nyaris bersamaan) — kalau
    # dipotong di sini, sebagian pesan spam yang sudah "ditangkap" & masuk
    # antrian malah tidak pernah kehapus. client.delete_messages() sendiri
    # sudah otomatis memecah ke batch ≤100 (limit API Telegram) per
    # panggilan, jadi tidak ada risiko error walau bucket-nya besar.

    # Lazy import (bukan di top-level file) — core.antispam_queue mengimpor
    # database di top-level-nya sendiri (set_global_flood_backoff dkk), jadi
    # import balik di top-level sini akan circular. Aman diimpor di sini
    # karena delete_worker baru dipanggil setelah semua modul selesai load.
    from core.antispam_queue import is_group_boosted as _is_group_boosted

    def _current_interval(cid: int) -> float:
        """Rate-limit floor EFEKTIF grup ini — kelompok flash (sedang boost
        aktif) vs kelompok slow (default). Dicek live tiap panggilan, BUKAN
        di-fix sekali saat scheduler spawn — supaya begitu boost grup habis
        di tengah jalan, siklus berikutnya scheduler otomatis langsung pakai
        interval slow tanpa perlu drop/respawn task."""
        return _DELETE_INTERVAL_FLASH if _is_group_boosted(cid) else _DELETE_INTERVAL_SLOW

    # Tunggu sampai client benar-benar terkoneksi sebelum mulai memproses
    for _ in range(60):
        if getattr(client, "is_connected", False):
            break
        await asyncio.sleep(1.0)

    print("[delete_worker] ✅ Dispatcher siap.", flush=True)

    buckets:      dict[int, list[int]]     = {}  # cid → pending mids
    idle_since:   dict[int, float]         = {}  # cid → timestamp pertama kali bucket kosong
    group_tasks:  dict[int, asyncio.Task]  = {}  # cid → task scheduler aktif
    group_events: dict[int, asyncio.Event] = {}  # cid → sinyal "ada mid baru di bucket"

    def _add_to_bucket(cid: int, mids: list[int]) -> None:
        # mids bisa [] (keepalive/touch — lihat komentar delete_queue di atas).
        # bucket.extend([]) no-op, tapi idle_since.pop() & group_events.set()
        # TETAP jalan → itulah yang bikin scheduler grup ini stay-alive murni
        # dari aktivitas pesan, walau tidak ada satupun yang perlu dihapus.
        bucket = buckets.setdefault(cid, [])
        idle_since.pop(cid, None)  # ada aktivitas → reset idle timer
        bucket.extend(mids)
        group_events.setdefault(cid, asyncio.Event()).set()  # bangunkan scheduler cid ini

    async def _group_scheduler(cid: int, slot_offset: float, respawn_event: asyncio.Event) -> None:
        """
        Task independen per grup — EVENT-DRIVEN.
        - Tunggu slot_offset dulu (jitter kecil biar spawn awal tidak numpuk).
        - Tunggu event "ada pesan baru" (bukan sleep tetap interval tetap).
        - Kalau bucket kosong saat bangun (spurious/idle) → akumulasi
          idle_since seperti sebelumnya; idle DELETE_IDLE_SECS → drop.
        - Kalau bucket ada isi → SELALU tunggu penuh interval kelompoknya
          (flash/slow, dicek live via _current_interval) sebelum eksekusi
          delete_messages — lihat catatan FIX di bawah.

        FIX (jeda hapus tidak konsisten): versi sebelumnya menghitung
        wait_left = interval - (waktu sejak delete TERAKHIR di grup ini,
        disimpan di variabel lokal last_delete_mono). ini punya 2 masalah:
          1. last_delete_mono variabel LOKAL task ini — hilang tiap kali
             task-nya di-cancel & respawn (terjadi tiap ada grup baru
             join/di-drop, lihat _do_respawn). Begitu direspawn, hitungan
             mulai dari 0.0 lagi → delete berikutnya jadi instan lagi,
             padahal grup itu belum tentu benar-benar idle.
          2. Kalau grup itu MEMANG sudah lama tidak ada delete (spam
             sporadis, jarang-jarang) — wait_left otomatis negatif → delete
             instan juga. Efeknya: utk spam sporadis (kasus paling umum),
             grup flash & grup slow SAMA SAJA kecepatannya (sama-sama
             instan) — beda kelompok cuma kerasa pas ada burst beruntun.
             Ini bikin fitur "Upgrade Speed" nyaris tidak kerasa bedanya
             utk kasus sehari-hari.
        SEKARANG: setiap batch delete SELALU menunggu penuh interval
        kelompoknya (bukan "sisa" dari delete sebelumnya) — jadi jeda hapus
        konsisten sesuai .env di SEMUA kasus (sporadis maupun burst), dan
        tidak lagi butuh state "delete terakhir" apapun → otomatis kebal
        dari reset respawn. Efek samping yang disengaja: burst pesan yang
        masuk SELAMA jeda tunggu ini otomatis ikut ke-batch ke delete yang
        sama (mids adalah referensi list buckets[cid], lihat komentar di
        bawah) — tidak ada spam yang "lolos" gara-gara nunggu.
        """
        await asyncio.sleep(slot_offset)
        event = group_events.setdefault(cid, asyncio.Event())

        while not respawn_event.is_set():
            try:
                await asyncio.wait_for(event.wait(), timeout=_DELETE_IDLE_SECS)
            except asyncio.TimeoutError:
                pass  # tidak ada event masuk selama _DELETE_IDLE_SECS — cek bucket di bawah

            if respawn_event.is_set():
                break

            mids = buckets.get(cid)
            if not mids:
                event.clear()
                if cid not in idle_since:
                    idle_since[cid] = time.monotonic()
                elif time.monotonic() - idle_since[cid] >= _DELETE_IDLE_SECS:
                    print(f"[delete_worker] chat={cid}: idle {_DELETE_IDLE_SECS:.0f}s → drop & respawn")
                    respawn_event.set()
                    break
                continue

            idle_since.pop(cid, None)
            event.clear()

            # Jeda hapus WAJIB & konsisten — selalu penuh, bukan "sisa" dari
            # delete sebelumnya (lihat FIX di docstring atas).
            await asyncio.sleep(_current_interval(cid))
            if respawn_event.is_set():
                break

            if not getattr(client, "is_connected", False):
                continue

            await wait_global_flood_backoff()
            if respawn_event.is_set():
                break

            # mids adalah referensi list yang sama dengan buckets[cid] — kalau
            # ada mid baru masuk selama jeda di atas, otomatis ikut ke-copy.
            to_delete = mids.copy()
            mids.clear()
            try:
                await client.delete_messages(cid, to_delete)
            except asyncio.CancelledError:
                raise
            except Exception as _err:
                from pyrogram.errors import FloodWait as _FW
                if isinstance(_err, _FW):
                    set_global_flood_backoff(_err.value)
                mids[:0] = to_delete  # kembalikan untuk retry begitu event berikutnya
                group_events.setdefault(cid, asyncio.Event()).set()  # retry secepatnya, bukan nunggu idle timeout
                # Tidak perlu update state apapun di sini — retry berikutnya
                # tetap otomatis menunggu penuh _current_interval(cid) lagi
                # (lihat awal loop), jadi retry-storm tetap aman tercegah
                # tanpa butuh variabel last_delete_mono seperti sebelumnya.
                print(f"[delete_worker] chat={cid}: gagal delete ({_err}), retry berikutnya")

    def _spawn_all(cids_active: list[int], respawn_event: asyncio.Event) -> None:
        """
        Spawn task scheduler untuk semua cid aktif, slot terdistribusi merata
        TERPISAH per kelompok kecepatan (flash vs slow) — grup flash tidak
        pernah dihitung dalam slot slow, begitu juga sebaliknya (masing-masing
        kelompok slot_size = interval_kelompok / (n_kelompok + 1)).

        Kelompok slow dapat tambahan DELETE_CATEGORY_JITTER di atas slot
        awalnya — cuma jitter start-up, mencegah slot awal kelompok flash &
        slow kebetulan align di detik yang sama persis. Tidak mempengaruhi
        rate-limit floor masing-masing grup (itu dicek live via
        _current_interval, terpisah dari jitter ini).
        """
        flash_cids = [c for c in cids_active if _is_group_boosted(c)]
        slow_cids  = [c for c in cids_active if c not in set(flash_cids)]

        flash_slot_size = _DELETE_INTERVAL_FLASH / (len(flash_cids) + 1)
        slow_slot_size  = _DELETE_INTERVAL_SLOW  / (len(slow_cids) + 1)

        for i, cid in enumerate(flash_cids):
            slot_offset = flash_slot_size * i
            task = asyncio.create_task(
                _group_scheduler(cid, slot_offset, respawn_event),
                name=f"del_sched_{cid}",
            )
            group_tasks[cid] = task

        for i, cid in enumerate(slow_cids):
            slot_offset = _CATEGORY_JITTER + (slow_slot_size * i)
            task = asyncio.create_task(
                _group_scheduler(cid, slot_offset, respawn_event),
                name=f"del_sched_{cid}",
            )
            group_tasks[cid] = task

        print(
            f"[delete_worker] 🔄 Respawn {len(cids_active)} grup aktif "
            f"(flash={len(flash_cids)}, slow={len(slow_cids)}) | "
            f"flash_slot={flash_slot_size:.3f}s slow_slot={slow_slot_size:.3f}s "
            f"jitter={_CATEGORY_JITTER:.3f}s",
            flush=True,
        )

    async def _do_respawn(drop_cid: int | None = None) -> asyncio.Event:
        """
        Cancel semua task lama, copot grup idle jika ada, spawn ulang semua.
        Hanya 1 grup yang di-drop per respawn — yang lain menyusul di siklus berikutnya.
        Return: respawn_event baru untuk siklus berikutnya.
        """
        old_tasks = list(group_tasks.values())
        for t in old_tasks:
            t.cancel()
        if old_tasks:
            await asyncio.gather(*old_tasks, return_exceptions=True)
        group_tasks.clear()

        # Copot 1 grup idle (pemicu respawn ini)
        if drop_cid is not None:
            buckets.pop(drop_cid, None)
            idle_since.pop(drop_cid, None)
            group_events.pop(drop_cid, None)

        new_event = asyncio.Event()
        active_cids = list(buckets.keys())
        if active_cids:
            _spawn_all(active_cids, new_event)
        return new_event

    # ── Dispatcher loop ───────────────────────────────────────────────────────
    # respawn_event dummy — belum ada task, set agar tidak salah cek awal
    respawn_event: asyncio.Event = asyncio.Event()
    respawn_event.set()

    while True:
        try:
            wait_time = _BATCH_WINDOW if buckets else _IDLE_TIMEOUT
            cid, mids = await asyncio.wait_for(delete_queue.get(), timeout=wait_time)
            is_new_group = cid not in buckets
            _add_to_bucket(cid, mids)
            delete_queue.task_done()

            # Drain semua item yang sudah ada di queue saat ini (non-blocking)
            while not delete_queue.empty():
                try:
                    cid2, mids2 = delete_queue.get_nowait()
                    if cid2 not in buckets:
                        is_new_group = True
                    _add_to_bucket(cid2, mids2)
                    delete_queue.task_done()
                except asyncio.QueueEmpty:
                    break

            # Cek apakah ada task scheduler yang minta respawn (idle drop)
            needs_respawn = respawn_event.is_set()

            if is_new_group or needs_respawn:
                # Cari grup idle yang jadi pemicu respawn (jika ada)
                drop_cid = None
                if needs_respawn:
                    now_mono = time.monotonic()
                    for c, ts in idle_since.items():
                        if now_mono - ts >= _DELETE_IDLE_SECS:
                            drop_cid = c
                            break
                respawn_event = await _do_respawn(drop_cid)

        except asyncio.TimeoutError:
            # Tidak ada item baru — cek apakah ada idle drop yang pending
            if respawn_event.is_set() and buckets:
                drop_cid = None
                now_mono = time.monotonic()
                for c, ts in idle_since.items():
                    if now_mono - ts >= _DELETE_IDLE_SECS:
                        drop_cid = c
                        break
                respawn_event = await _do_respawn(drop_cid)

        except asyncio.CancelledError:
            # Shutdown — cancel semua task grup, flush sisa bucket
            for t in group_tasks.values():
                t.cancel()
            await asyncio.gather(*group_tasks.values(), return_exceptions=True)
            if getattr(client, "is_connected", False):
                for cid, mids in buckets.items():
                    if mids:
                        try:
                            await client.delete_messages(cid, mids)
                        except Exception:
                            pass
            break
        except Exception as _e:
            print(f"[delete_worker] ❌ Dispatcher error: {_e}")
            await asyncio.sleep(0.5)


async def _panel_write_attempt(kind: str, chat_id: int, key, value) -> None:
    """Satu percobaan penulisan ke DB. Lempar exception jika gagal."""
    if kind == "config":
        await config_db.update_one(
            {"chat_id": chat_id},
            {"$set": {"chat_id": chat_id, key: value}},
            upsert=True,
        )
    elif kind == "ns":
        await newscore_cfg_db.update_one(
            {"chat_id": chat_id},
            {"$set": {"chat_id": chat_id, **value}},
            upsert=True,
        )
    elif kind == "regex_delete":
        from bson import ObjectId
        # FIX: sebelumnya pakai nama variabel `group_regex_db` yang TIDAK
        # PERNAH didefinisikan di file ini (cuma ada sebagai variabel lokal
        # di beberapa file plugins/, beda scope sama sekali) — selalu
        # NameError begitu ada penghapusan regex per-grup lewat panel.
        # Fix: akses collection-nya langsung lewat db[...], sama persis
        # pola yang sudah dipakai kasus "free_delete" tepat di bawah ini.
        await db["regex_per_group"].delete_one({"_id": ObjectId(key), "chat_id": chat_id})
    elif kind == "free_delete":
        await db["free_per_group"].delete_one({"chat_id": chat_id, "user_id": int(key)})
    elif kind == "mention_wl_delete":
        await mention_wl_db.update_one(
            {"chat_id": chat_id},
            {"$pull": {"usernames": key}},
        )


async def panel_write_worker(client=None) -> None:
    """
    Worker tunggal untuk panel_write_queue.

    Tombol panel DM (toggle on/off, +/- durasi, dsb) sudah mengubah cache
    secara optimistic SEBELUM enqueue di sini — jadi worker ini hanya
    bertugas menulis nilai final ke DB di belakang layar, dengan jeda
    PANEL_WRITE_DELAY detik antar item agar tidak membanjiri DB/API saat
    banyak grup atau banyak klik beruntun terjadi bersamaan.

    Retry & rollback:
      - Tiap item dicoba hingga PANEL_WRITE_RETRIES kali (jeda singkat
        antar percobaan) sebelum dianggap GAGAL PERMANEN.
      - Sukses (kapan pun selama masih dalam batas retry) → selesai,
        tidak ada efek samping lain (silent), karena UI sudah benar.
      - Gagal permanen → cache untuk chat_id itu di-invalidate (paksa
        baca ulang dari DB di akses berikutnya), dan jika item membawa
        lokasi pesan panel (dm_chat_id + dm_msg_id), _panel_rollback_callback
        dipanggil untuk mengoreksi tampilan panel itu balik ke nilai DB
        yang sebenarnya + memberi tahu admin bahwa aksinya tidak tersimpan.

    Item diambil satu per satu (bukan batch) karena tiap toggle bisa
    menyasar koleksi/skema berbeda (config_db vs newscore_cfg_db).
    """
    while True:
        try:
            item = await panel_write_queue.get()
            kind       = item["kind"]
            chat_id    = item["chat_id"]
            key        = item.get("key")
            value      = item["value"]
            dm_chat_id = item.get("dm_chat_id")
            dm_msg_id  = item.get("dm_msg_id")

            ok = False
            last_err = None
            for attempt in range(1, PANEL_WRITE_RETRIES + 1):
                try:
                    await _panel_write_attempt(kind, chat_id, key, value)
                    ok = True
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    last_err = e
                    if attempt < PANEL_WRITE_RETRIES:
                        await asyncio.sleep(0.5 * attempt)  # backoff ringan

            if ok:
                # Item benar-benar sukses di DB → lepas status pending
                # (aman dilepas SETELAH sukses, bukan sebelum, supaya tidak
                # ada celah balapan dengan render panel di antaranya).
                if kind == "regex_delete":
                    unmark_pending_delete(chat_id, "regex", key)
                    try:
                        from plugins.filters.antispam import invalidate_local_regex_cache
                        invalidate_local_regex_cache(chat_id)
                    except Exception:
                        pass
                    invalidate_count_cache(chat_id)
                elif kind == "free_delete":
                    unmark_pending_delete(chat_id, "free", key)
                    target_user_id = int(key)
                    try:
                        from video_call import invalidate_vip_cache
                        invalidate_vip_cache(chat_id, target_user_id)
                    except Exception:
                        pass
                    try:
                        from core.antispam_queue import invalidate_antispam_vip_cache
                        invalidate_antispam_vip_cache(chat_id, target_user_id)
                    except Exception:
                        pass
                    try:
                        from core.vip_bio_guard import clear_vip_title
                        await clear_vip_title(chat_id, target_user_id)
                    except Exception:
                        pass
                    invalidate_count_cache(chat_id)
                elif kind == "mention_wl_delete":
                    unmark_pending_delete(chat_id, "mention_wl", key)
                    # Tidak perlu invalidate cache lain — mention_wl_get()
                    # selalu baca langsung dari DB tiap dipanggil.

            if not ok:
                print(f"[panel_write_worker] GAGAL PERMANEN tulis {kind} {chat_id} {key}: {last_err}")
                # Nilai optimistic yang sempat tersimpan di cache tidak pernah
                # benar-benar mendarat di DB — invalidate agar baca berikutnya
                # ambil nilai asli dari DB, bukan nilai optimistic yang salah.
                if kind == "config":
                    _config_cache.pop(chat_id, None)
                elif kind == "ns":
                    _ns_config_cache.pop(chat_id, None)
                elif kind == "regex_delete":
                    # Item TIDAK jadi terhapus di DB — lepas status pending
                    # supaya muncul kembali di render panel berikutnya.
                    unmark_pending_delete(chat_id, "regex", key)
                elif kind == "free_delete":
                    unmark_pending_delete(chat_id, "free", key)
                elif kind == "mention_wl_delete":
                    unmark_pending_delete(chat_id, "mention_wl", key)

                if dm_chat_id and dm_msg_id and _panel_rollback_callback is not None:
                    try:
                        await _panel_rollback_callback(client, kind, chat_id, key, dm_chat_id, dm_msg_id)
                    except Exception as cb_err:
                        print(f"[panel_write_worker] rollback callback gagal: {cb_err}")

            panel_write_queue.task_done()
            await asyncio.sleep(PANEL_WRITE_DELAY)
        except asyncio.CancelledError:
            break
        except Exception:
            # Cegah worker mati diam-diam akibat exception tak terduga
            await asyncio.sleep(0.5)


def enqueue_config_write(
    chat_id: int, key: str, value,
    dm_chat_id: int | None = None, dm_msg_id: int | None = None,
) -> None:
    """Antrikan penulisan satu field config grup ke DB (non-blocking)."""
    panel_write_queue.put_nowait({
        "kind": "config", "chat_id": chat_id, "key": key, "value": value,
        "dm_chat_id": dm_chat_id, "dm_msg_id": dm_msg_id,
    })


def enqueue_ns_write(
    chat_id: int, updates: dict,
    dm_chat_id: int | None = None, dm_msg_id: int | None = None,
) -> None:
    """Antrikan penulisan field(s) NewsCore config grup ke DB (non-blocking)."""
    panel_write_queue.put_nowait({
        "kind": "ns", "chat_id": chat_id, "key": None, "value": updates,
        "dm_chat_id": dm_chat_id, "dm_msg_id": dm_msg_id,
    })


# ══════════════════════════════════════════════════════════════════════════════
# GROUP HELPERS
# ══════════════════════════════════════════════════════════════════════════════

async def get_my_admin_groups(client, user_id: int, force_live: bool = False) -> list:
    """
    Kembalikan semua GRUP (bukan channel) dari config_db (berbagi via CODE_BOT).
    Menggunakan judul tersimpan jika bot token ini tidak ada di grup tersebut,
    sehingga dua bot dengan CODE_BOT yang sama melihat daftar grup yang sama.

    FIX: simpan chat_type saat bisa akses → filter channel saat tidak bisa akses.
    CACHE: hasil di-cache ADMIN_GROUPS_TTL detik — hindari looping Telegram API
           tiap kali admin menekan "Refresh" atau membuka daftar grup.

    FAST PATH (default, force_live=False): status admin dibaca dari
    `group_admin_roster` (DB, lihat get_admin_roster) — roster ini SUDAH
    disinkronkan reaktif oleh nexus_group.py tiap ada promote/demote, jadi
    valid dipercaya tanpa panggil Telegram API sama sekali per grup. Ini
    menghilangkan bottleneck utama: sebelumnya tiap buka "Kelola Grup" harus
    panggil get_chat() + get_chat_member() ke Telegram untuk SETIAP grup yang
    dikenal bot (2×N API call berurutan) — makin banyak grup, makin lama.

    Grup yang rosternya belum pernah di-bootstrap (None) tetap diverifikasi
    live via is_admin() seperti sebelumnya (kasus langka: grup baru banget),
    lalu roster-nya dibangun di background supaya load berikutnya sudah lewat
    fast path.

    force_live=True (dipakai tombol "Refresh Sinkronisasi"): lewati roster,
    verifikasi ulang semua grup langsung ke Telegram — dipakai saat admin
    mencurigai data belum sinkron (mis. baru saja dipromosikan admin baru).
    """
    now = time.monotonic()
    hit = _admin_groups_cache.get(user_id)
    if not force_live and hit and (now - hit[1]) < ADMIN_GROUPS_TTL:
        return hit[0]

    from pyrogram.enums import ChatType
    result = []
    _seen_chat_ids: set[int] = set()
    async for doc in config_db.find({}):
        chat_id = doc.get("chat_id")
        if not chat_id:
            continue

        # ── SELF-HEAL: bersihkan sisa dokumen dobel dari bug migrasi-balik ──
        # shard lama (1 chat_id sempat punya 2 dokumen "status" karena upsert
        # dulu keyed by _id, bukan chat_id — sudah diperbaiki di
        # _migrate_shard_back, tapi data dobel yang KADUNG ada perlu dibuang
        # supaya daftar "Kelola Grup" tidak dobel per grup).
        if chat_id in _seen_chat_ids:
            _dup_id = doc.get("_id")
            if _dup_id:
                async def _cleanup_dup(_id=_dup_id):
                    try:
                        from bson import ObjectId
                        await config_db.delete_one({"_id": ObjectId(_id)})
                    except Exception:
                        pass
                asyncio.create_task(_cleanup_dup())
            continue
        _seen_chat_ids.add(chat_id)

        stored_type = (doc.get("chat_type") or "").upper()
        if stored_type == "CHANNEL":
            continue  # skip channel yang tersimpan di DB

        title = doc.get("title")

        # ── FAST PATH — percaya roster admin dari DB, 0 panggilan Telegram ──
        if not force_live:
            roster = await get_admin_roster(chat_id)
            if roster is not None:
                if title and user_id in roster:
                    result.append({"id": chat_id, "title": title})
                continue  # roster valid → tidak perlu verifikasi live sama sekali

        # ── SLOW PATH — live verify (roster belum ada, atau force_live) ──
        title = title or str(chat_id)
        chat_accessible = False
        try:
            chat = await client.get_chat(chat_id)
            if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
                await config_db.update_one(
                    {"chat_id": chat_id},
                    {"$set": {"chat_type": chat.type.name}},
                )
                continue
            title = chat.title or title
            chat_accessible = True
            await config_db.update_one(
                {"chat_id": chat_id},
                {"$set": {"title": title, "chat_type": chat.type.name}},
            )
        except Exception as _e_getchat:
            if not doc.get("title"):
                continue  # belum ada judul tersimpan, lewati

            # FIX Bug 1: Verifikasi apakah bot masih anggota grup.
            try:
                from pyrogram.errors import (
                    UserNotParticipant, ChannelPrivate, ChatForbidden,
                    ChatIdInvalid, PeerIdInvalid,
                )
                me = client.me
                await client.get_chat_member(chat_id, me.id)
            except Exception as _ve:
                _err_cls = type(_ve).__name__
                if _err_cls in (
                    "UserNotParticipant", "ChannelPrivate", "ChatForbidden",
                    "ChatIdInvalid", "PeerIdInvalid",
                ):
                    await remove_group_data(chat_id)
                    continue
                # Error lain (jaringan, FloodWait, dsb.) → percayai data DB

        if chat_accessible:
            if await is_admin(client, chat_id, user_id):
                result.append({"id": chat_id, "title": title})
            # Bangun/perbarui roster di background agar load berikutnya lewat
            # fast path (tanpa perlu tunggu bootstrap selesai di sini).
            asyncio.create_task(bootstrap_admin_roster(client, chat_id))
        else:
            if await is_admin(client, chat_id, user_id):
                result.append({"id": chat_id, "title": title})
    _admin_groups_cache[user_id] = (result, time.monotonic())
    return result


async def save_group_title(chat_id: int, title: str) -> None:
    await config_db.update_one(
        {"chat_id": chat_id},
        {"$set": {"chat_id": chat_id, "title": title}},
        upsert=True,
    )
    _config_cache.pop(chat_id, None)


async def save_group_username(chat_id: int, username: str | None) -> None:
    """
    Simpan/update username grup ke config_db agar bisa di-resolve via
    @username saat rewarm.

    Username None DITULIS juga (bukan diabaikan) — kalau grup berubah dari
    publik→privat, field lama harus ikut hilang, supaya panel "Grup
    Terdaftar" tidak menampilkan link t.me/username basi yang sudah tidak
    berlaku (lihat refresh_group_public_info).
    """
    await config_db.update_one(
        {"chat_id": chat_id},
        {"$set": {"chat_id": chat_id, "username": username}},
        upsert=True,
    )
    _config_cache.pop(chat_id, None)


async def save_group_invite_link(chat_id: int, invite_link: str | None) -> None:
    """Simpan/update invite link grup privat ke config_db."""
    await config_db.update_one(
        {"chat_id": chat_id},
        {"$set": {"chat_id": chat_id, "invite_link": invite_link}},
        upsert=True,
    )
    _config_cache.pop(chat_id, None)


async def bootstrap_groups_from_dialogs() -> tuple[int, int]:
    """
    PEMULIHAN DARURAT — daftarkan ulang ke config_db grup/supergrup yang
    diketahui MASIH aktif memakai bot, sejauh yang bisa dilacak ulang dari
    Telegram.

    ⚠️ KOREKSI PENTING (versi sebelumnya salah asumsi):
    Percobaan pertama fungsi ini memakai `client.get_dialogs()` milik BOT
    UTAMA (login via bot_token) — dan itu SALAH: akun bot (bukan akun user
    biasa) TIDAK memiliki "daftar dialog" di sisi server Telegram sama
    sekali. `messages.getDialogs` untuk sesi bot selalu balik KOSONG,
    apa pun jumlah grup yang sebenarnya dipakai bot itu — ini keterbatasan
    resmi platform Telegram, bukan bug throttling/limit di kode. Itulah
    kenapa hasilnya "0 grup didaftarkan, 0 channel dilewati" padahal
    grupnya banyak — bukan gagal, tapi memang tidak ada apa pun yang bisa
    dibaca dari jalur itu.

    Telegram memang SENGAJA tidak menyediakan API bagi bot untuk
    menanyakan "aku ada di grup mana saja" — ini alasan keamanan/privasi
    (supaya bot pihak ketiga tidak bisa mengintai semua chat-nya sendiri
    sekaligus). Konsekuensinya, TIDAK ADA cara 100% lengkap untuk
    merekonstruksi ulang daftar grup bot murni dari sisi bot sendiri.

    STRATEGI PEMULIHAN (2 lapis, saling melengkapi):

    1. LAPIS INI (langsung/instan, tapi SEBAGIAN) — pakai USERBOT (akun
       Telegram biasa yang dipakai Security OS untuk join VC). Userbot
       ADALAH akun user asli, jadi get_dialogs()-nya benar-benar berisi
       semua grup yang dia ikuti. Untuk grup yang: (a) usernya adalah
       userbot ini, DAN (b) userbot pernah ditambahkan ke grup itu (biasa
       terjadi otomatis begitu Security OS/Inspeksi Onkem pernah aktif di
       grup tsb) → grup itu bisa langsung didaftarkan ulang ke config_db
       SEKARANG JUGA, instan, tanpa nunggu apa pun.
       Grup yang TIDAK PERNAH memakai Security OS (userbot tidak pernah
       diundang ke situ) TIDAK bisa dijangkau lapis ini — lanjut ke lapis 2.

    2. LAPIS PASIF (otomatis, mencakup SEMUA grup, tapi butuh sedikit
       waktu) — lihat ensure_group_registered() di bawah, dipanggil dari
       handler pesan grup paling awal (plugins/filters/antispam.py).
       Begitu ADA 1 pesan APA PUN (bebas dari member mana pun, bukan cuma
       admin) masuk dari grup manapun, grup itu OTOMATIS terdaftar lagi ke
       config_db — tanpa admin perlu melakukan apa pun secara sadar. Untuk
       grup yang aktif (ada obrolan rutin), ini biasanya pulih dalam
       hitungan menit setelah redeploy, bukan perlu ditunggu berhari-hari.

    Fungsi ini (lapis 1) dipanggil otomatis sekali saat bot start
    (main.py) DAN tersedia manual lewat /syncgrup — aman dipanggil
    berkali-kali (idempotent, upsert, tidak menimpa pengaturan yang sudah
    ada, cuma menyegarkan title/username/chat_type).

    Return (jumlah_grup_didaftarkan_lewat_userbot, jumlah_channel_dilewati).
    """
    from pyrogram.enums import ChatType
    from pyrogram.errors import FloodWait

    registered = 0
    skipped_channel = 0
    processed = 0

    try:
        from video_call import userbot, is_userbot_ready
    except Exception as e:
        print(f"[Bootstrap] Userbot tidak tersedia ({e}) — lapis 1 dilewati, "
              f"mengandalkan sepenuhnya pada pendaftaran pasif (ensure_group_registered).")
        return 0, 0

    if not is_userbot_ready() or userbot is None:
        print("[Bootstrap] Userbot belum online — lapis 1 dilewati untuk sekarang. "
              "Jalankan /syncgrup lagi setelah userbot online, atau tunggu "
              "pendaftaran pasif dari lalu-lintas pesan grup (lapis 2).")
        return 0, 0

    try:
        async for dialog in userbot.get_dialogs():
            chat = dialog.chat
            if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
                if chat.type == ChatType.CHANNEL:
                    skipped_channel += 1
                continue
            try:
                await config_db.update_one(
                    {"chat_id": chat.id},
                    {
                        "$set": {
                            "chat_id":   chat.id,
                            "title":     chat.title or str(chat.id),
                            "username":  chat.username,
                            "chat_type": chat.type.name,
                        },
                    },
                    upsert=True,
                )
                _config_cache.pop(chat.id, None)
                registered += 1
            except Exception as e:
                print(f"[Bootstrap] Gagal daftarkan grup {chat.id}: {e}")

            # ── Throttle ringan — TIDAK ada API Telegram per-grup di sini
            # (data title/username/type sudah didapat gratis dari dialog
            # yang sama, tulisannya cuma ke MongoDB/SQLite lokal, bukan ke
            # Telegram), jadi risiko FloodWait di titik INI sebenarnya nol.
            # Jeda kecil ini murni untuk tidak membanjiri get_dialogs()
            # sendiri (yang di baliknya tetap 1 request Telegram per ~100
            # dialog) dengan proses berat berturut-turut tanpa jeda sama
            # sekali kalau userbot ada di RIBUAN grup sekaligus.
            processed += 1
            if processed % 50 == 0:
                await asyncio.sleep(0.5)
    except FloodWait as fw:
        print(
            f"[Bootstrap] FloodWait {fw.value}s saat get_dialogs (pagination Telegram, "
            f"bukan per-grup) — pemulihan berhenti di tengah ({registered} grup sudah "
            f"tersimpan sejauh ini, TIDAK hilang). Jalankan /syncgrup lagi setelah "
            f"{fw.value}s untuk melanjutkan sisanya."
        )
    except Exception as e:
        print(f"[Bootstrap] Gagal ambil daftar dialog userbot: {e}")

    print(f"[Bootstrap] Selesai (lapis 1/userbot): {registered} grup didaftarkan/disegarkan "
          f"ke config_db, {skipped_channel} channel dilewati. Grup TANPA userbot (tidak "
          f"pernah pakai Security OS) akan pulih sendiri lewat lapis 2 (pesan masuk) — "
          f"lihat ensure_group_registered().")
    return registered, skipped_channel


# ── Registrasi pasif (LAPIS 2) — jaring pengaman utama, mencakup SEMUA grup ──
_registered_recently: dict[int, float] = {}   # {chat_id: monotonic_ts} — throttle per grup
_REGISTER_RECHECK_SECS = 3600   # cukup 1x/jam per grup — bukan tiap pesan


async def ensure_group_registered(chat_id: int, title: str, username: "str | None", chat_type: str) -> None:
    """
    Registrasi/segarkan config_db untuk grup ini — dipanggil dari handler
    pesan grup PALING AWAL (plugins/filters/antispam.py) untuk SETIAP pesan
    yang masuk dari grup manapun.

    Ini jaring pengaman UTAMA pemulihan config_db setelah reset/migrasi DB:
    tidak seperti bootstrap_groups_from_dialogs() (lapis 1, cuma menjangkau
    grup yang punya userbot Security OS), fungsi ini menjangkau SEMUA grup
    tanpa syarat apa pun — begitu ada 1 member mengirim 1 pesan apa pun,
    grup itu otomatis terdaftar lagi.

    THROTTLE: cek in-memory per grup (_REGISTER_RECHECK_SECS) SEBELUM
    sentuh DB — supaya grup yang trafiknya ramai tidak menulis config_db di
    SETIAP pesan (mahal &amp; tidak perlu, cukup di-refresh 1x/jam).
    Dipanggil sinkron di jalur utama tapi murah: cache miss hanya terjadi
    1x/jam per grup, sisanya cuma 1 dict lookup.
    """
    now = time.monotonic()
    last = _registered_recently.get(chat_id, 0.0)
    if now - last < _REGISTER_RECHECK_SECS:
        return
    _registered_recently[chat_id] = now
    try:
        await config_db.update_one(
            {"chat_id": chat_id},
            {
                "$set": {
                    "chat_id":   chat_id,
                    "title":     title,
                    "username":  username,
                    "chat_type": chat_type,
                },
            },
            upsert=True,
        )
        _config_cache.pop(chat_id, None)
    except Exception as e:
        print(f"[AutoRegister] Gagal registrasi pasif grup {chat_id}: {e}")





async def refresh_group_public_info(client, chat_id: int) -> None:
    """
    Sinkronkan info akses publik grup (username ATAU invite link) ke
    config_db supaya tidak basi — dipanggil berkala oleh perm_watchdog
    untuk setiap grup yang masih aktif.

    Aturan:
      • Grup PUBLIK (punya @username) → simpan username terbaru, HAPUS
        invite_link tersimpan (tidak relevan lagi, grup sudah publik).
      • Grup PRIVAT (tidak ada @username) → simpan username=None, lalu
        coba buat 1 invite link via export_chat_invite_link().
          - Telegram me-reuse link primer yang masih aktif jika ada, dan
            hanya generate baru jika link lama sudah di-revoke/dihapus
            owner — jadi aman dipanggil berkala, TIDAK membuat link baru
            setiap siklus selama link lama masih hidup.
          - Jika bot tidak punya izin invite (ChatAdminRequired) → SKIP
            diam-diam, tidak melempar error, invite_link lama (jika ada)
            dibiarkan apa adanya (lebih baik link basi yang masih bisa
            dicoba daripada tidak ada sama sekali).

    Dipanggil dengan try/except oleh caller — fungsi ini sendiri tidak
    melempar exception ke pemanggil untuk error apapun selain bug internal.
    """
    from pyrogram.errors import ChatAdminRequired, UserNotParticipant, FloodWait, BadRequest

    try:
        chat = await client.get_chat(chat_id)
    except Exception:
        return  # grup tidak terakses sekarang — perm_watchdog yang urus status grup hilang

    username = getattr(chat, "username", None)
    await save_group_username(chat_id, username)
    await save_group_title(chat_id, chat.title or str(chat_id))

    if username:
        # Grup publik — invite link tidak relevan lagi, bersihkan sisa lama.
        await save_group_invite_link(chat_id, None)
        return

    # Grup privat — coba pastikan ada 1 invite link valid.
    try:
        link = await client.export_chat_invite_link(chat_id)
        if link:
            await save_group_invite_link(chat_id, link)
    except (ChatAdminRequired, UserNotParticipant):
        pass  # tidak ada izin undang — skip, jangan error
    except FloodWait:
        pass  # rate limit sesaat — coba lagi siklus watchdog berikutnya
    except BadRequest as e:
        # Beberapa tipe chat tidak mendukung invite link sama sekali, mis.
        # CHANNEL_MONOFORUM_UNSUPPORTED (channel dengan mode forum satu-arah)
        # — ini bukan masalah izin/rate-limit, dan tidak akan pernah berhasil
        # walau di-retry. Skip diam-diam supaya log tidak penuh noise untuk
        # kondisi yang memang tidak bisa diperbaiki.
        if "MONOFORUM" not in str(e).upper():
            print(f"[GroupInfo] export invite link ditolak grup {chat_id}: {e}")
    except Exception as e:
        print(f"[GroupInfo] Gagal export invite link grup {chat_id}: {e}")


async def remove_group_data(chat_id: int) -> None:
    await config_db.delete_one({"chat_id": chat_id})
    _config_cache.pop(chat_id, None)
    keys_to_remove = [k for k in _admin_cache if k[0] == chat_id]
    for k in keys_to_remove:
        _admin_cache.pop(k, None)
    try:
        await db["group_admin_roster"].delete_one({"chat_id": chat_id})
    except Exception as e:
        print(f"[AdminRoster] Gagal hapus roster grup {chat_id}: {e}")

    # FIX: sebelumnya dokumen "security_os" (enabled, monitor_token,
    # monitor_bot_id, dst.) TIDAK ikut dihapus di sini. Akibatnya, begitu
    # grup mati/bot dikick, config_db bersih tapi security_os masih
    # "enabled": True selamanya — userbot (video_call.py, query langsung ke
    # security_os) dan bot pemantau (monitor_bot_reference.py, query
    # monitor_token) tetap menganggap grup ini aktif dan terus mencoba
    # naik VC / spawn instance, walau grupnya sudah lama mati. Dihapus di
    # sini supaya "ingatan" Security OS untuk grup ini benar-benar bersih
    # total, bukan cuma di-skip lewat gate baca config_db.
    try:
        result = await db["security_os"].delete_one({"chat_id": chat_id})
        if getattr(result, "deleted_count", 0):
            print(f"[DB] Data Security OS grup {chat_id} ikut dihapus (enabled/monitor_token dibersihkan total).")
    except Exception as e:
        print(f"[DB] Gagal hapus data Security OS grup {chat_id}: {e}")

    # Stop instance yang mungkin masih hidup di proses SAAT INI juga (bukan
    # cuma bersih di DB) — supaya reaksinya instan, tidak menunggu siklus
    # group_gate_watchdog_loop (60 detik) atau ronde scheduler VC (30 menit).
    # Lazy import + try/except: modul security_os belum tentu ter-load di
    # semua konteks yang memanggil remove_group_data, dan ini tidak boleh
    # jadi fatal kalau gagal.
    try:
        from monitor_bot_reference import stop_monitor_for_group
        await stop_monitor_for_group(chat_id)
    except Exception:
        pass
    try:
        from video_call import _leave_vc_for_group
        await _leave_vc_for_group(chat_id)
    except Exception:
        pass

    print(f"[DB] Data grup {chat_id} dihapus (bot dikeluarkan).")


# ══════════════════════════════════════════════════════════════════════════════
# NEXUS DATABASE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

async def nexus_insert_kalimat_raw(teks: str, claim_key: str) -> bool:
    """
    v8.0 — Simpan RAW teks TAHAP 1 (spam_claim_queue) ke Record Data
    ("nexus_kalimat"), supaya kalimat yang sedang/sudah diperiksa Groq
    TAHAP 2 kelihatan di panel Owner Bot → Record Data.

    claim_key = _id (hash) dokumen yang sama di spam_claim_queue_db —
    dipakai sebagai kunci link 1:1 untuk cascade delete (lihat
    nexus_delete_kalimat_by_id / nexus_delete_kalimat). Dedupe pada
    claim_key (bukan _id sendiri) supaya klaim yang sama tidak dobel
    dicatat di Record Data.
    """
    try:
        result = await nexus_kalimat_db.update_one(
            {"claim_key": claim_key},
            {
                "$setOnInsert": {
                    "teks":          teks,
                    "claim_key":     claim_key,
                    "status_proses": 0,   # 0 = belum digenerate TAHAP 2, 1 = sudah
                    "created_at":    datetime.now(TZ_WIB),
                }
            },
            upsert=True,
        )
        doc = await nexus_kalimat_db.find_one({"claim_key": claim_key})
        # v10: baru trigger cek batas kalau ini beneran INSERT baru (bukan
        # klaim duplikat yang cuma nge-match entri lama) — hindari query
        # count_documents sia-sia tiap kali ada klaim yang sebenarnya sudah
        # tercatat.
        if result.upserted_id is not None:
            try:
                await nexus_enforce_record_cap()
            except Exception as e:
                print(f"[DB] nexus_enforce_record_cap error (non-fatal): {e}")
        return doc is not None
    except Exception:
        return False


async def nexus_mark_kalimat_processed(claim_key: str) -> None:
    """Tandai raw Record Data sebagai sudah digenerate TAHAP 2 (dipanggil
    dari spam_claim_worker_loop setelah varian selesai di-enqueue ke Groq)."""
    try:
        await nexus_kalimat_db.update_one(
            {"claim_key": claim_key},
            {"$set": {"status_proses": 1}},
        )
    except Exception:
        pass


async def nexus_insert_kalimat_variant(
    raw_key: str, teks: str, is_spam: bool, confidence: float,
) -> None:
    """
    v8.0 — Simpan 1 varian kalimat (hasil generate TAHAP 2) yang SUDAH
    dinilai & dipakai melatih AI Manual oleh core/groq_queue.py. Ditaut
    ke raw asalnya lewat raw_key (= claim_key di nexus_kalimat_db /
    _id di spam_claim_queue_db) supaya bisa ikut dihapus kalau raw-nya
    dihapus dari Record Data (lihat nexus_delete_kalimat_by_id).
    """
    try:
        await nexus_kalimat_variants_db.insert_one({
            "raw_key":    raw_key,
            "teks":       teks,
            "is_spam":    is_spam,
            "confidence": confidence,
            "created_at": datetime.now(TZ_WIB),
        })
    except Exception as e:
        print(f"[DB] nexus_insert_kalimat_variant error (non-fatal): {e}")


async def _nexus_cascade_delete_by_claim_key(claim_key: str | None) -> None:
    """
    v8.0 — Dipanggil setiap kali 1 entri raw Record Data dihapus.
    Menghapus:
      1. Entri klaim TAHAP 1 di spam_claim_queue_db (supaya kalimat yang
         sama boleh diklaim ulang / dicek ulang di masa depan — bukan
         ditandai "sudah pernah dicek" selamanya).
      2. Semua varian TAHAP 2 di nexus_kalimat_variants_db yang berasal
         dari raw tersebut, DAN mencoba "untrain" tiap varian itu dari
         model Bayes (subtraction hitungan fitur — lihat
         NaiveBayesSpamClassifier.untrain di nexus/ai_core/bayes.py).

    CATATAN JUJUR: PatternMemory (nexus/ai_core/pattern_memory.py)
    menyimpan pola KONTEKS yang sudah di-merge (weighted average) antar
    banyak kalimat, bukan 1 dokumen per kalimat — jadi kontribusi 1
    varian ke PatternMemory TIDAK bisa dicabut secara presisi tanpa
    redesign total struktur itu. Hanya Bayes (berbasis hitungan aditif)
    yang di-untrain di sini; PatternMemory dibiarkan apa adanya.
    """
    if not claim_key:
        return
    try:
        await spam_claim_queue_db.delete_one({"_id": claim_key})
    except Exception as e:
        print(f"[DB] cascade: gagal hapus spam_claim_queue ({claim_key}): {e}")

    try:
        variants = [d async for d in nexus_kalimat_variants_db.find({"raw_key": claim_key})]
    except Exception as e:
        print(f"[DB] cascade: gagal ambil varian ({claim_key}): {e}")
        variants = []

    if variants:
        try:
            from nexus.ai_core.bridge import get_nexus_ai
            ai = get_nexus_ai()
            if not ai._loaded:
                await ai.load()
            for v in variants:
                try:
                    ai.bayes.untrain(v["teks"], is_spam=bool(v.get("is_spam")))
                except Exception:
                    pass
            await ai.save()
        except Exception as e:
            print(f"[DB] cascade: gagal untrain Bayes ({claim_key}): {e}")

    try:
        await nexus_kalimat_variants_db.delete_many({"raw_key": claim_key})
    except Exception as e:
        print(f"[DB] cascade: gagal hapus varian ({claim_key}): {e}")


async def nexus_enforce_record_cap() -> int:
    """
    v10: Jaga Record Data (nexus_kalimat) tidak melebihi NEXUS_RECORD_MAX
    entri. Kalau kelampaui, hapus entri TERLAMA (sort created_at ASC) satu
    per satu lewat nexus_delete_kalimat_by_id — supaya cascade delete yang
    SUDAH ADA otomatis ikut jalan: hapus entri di spam_claim_queue_db,
    hapus SEMUA varian turunannya di nexus_kalimat_variants_db, DAN
    untrain tiap varian itu dari model Bayes (nexus/ai_core/bayes.py).

    CATATAN JUJUR (sama seperti di _nexus_cascade_delete_by_claim_key):
    PatternMemory TIDAK bisa di-untrain presisi per-kalimat (pola
    kontekstualnya sudah ter-merge/weighted-average dengan kalimat lain),
    jadi bagian itu TETAP ADA walau raw & Bayes-nya sudah dihapus. Ini
    keterbatasan desain PatternMemory, bukan bug di fungsi ini.

    Return: jumlah entri yang dihapus (0 kalau belum melebihi batas).
    """
    if NEXUS_RECORD_MAX <= 0:
        return 0  # 0 = fitur cap dimatikan

    total = await nexus_kalimat_db.count_documents({})
    excess = total - NEXUS_RECORD_MAX
    if excess <= 0:
        return 0

    deleted = 0
    async for doc in nexus_kalimat_db.find({}).sort("created_at", 1).limit(excess):
        ok = await nexus_delete_kalimat_by_id(str(doc["_id"]))
        if ok:
            deleted += 1

    if deleted > 0:
        print(f"[DB] Record Data melebihi batas ({total}/{NEXUS_RECORD_MAX}) — {deleted} raw TERLAMA dihapus (cascade: varian + untrain Bayes).")
    return deleted


async def nexus_get_all_kalimat() -> list[str]:
    return [doc["teks"] async for doc in nexus_kalimat_db.find({})]


async def nexus_get_kalimat_count() -> tuple[int, int]:
    global _nexus_kalimat_count_cache
    now = time.monotonic()
    if _nexus_kalimat_count_cache and (now - _nexus_kalimat_count_cache[1]) < NEXUS_COUNT_TTL:
        return _nexus_kalimat_count_cache[0]
    total   = await nexus_kalimat_db.count_documents({})
    antrean = await nexus_kalimat_db.count_documents({"status_proses": 0})
    _nexus_kalimat_count_cache = ((total, antrean), now)
    return total, antrean


async def nexus_mark_all_processed():
    await nexus_kalimat_db.update_many({}, {"$set": {"status_proses": 1}})
    invalidate_nexus_counts()


async def nexus_delete_kalimat(teks: str) -> bool:
    """Hapus 1 raw dari Record Data berdasarkan teks asli — v8.0: cascade
    ke spam_claim_queue (raw permanen TAHAP 1) + semua varian TAHAP 2."""
    doc = await nexus_kalimat_db.find_one({"teks": teks})
    result = await nexus_kalimat_db.delete_one({"teks": teks})
    if result.deleted_count > 0:
        invalidate_nexus_counts()
        await _nexus_cascade_delete_by_claim_key(doc.get("claim_key") if doc else None)
    return result.deleted_count > 0


async def nexus_delete_kalimat_by_id(id_str: str) -> bool:
    """Hapus 1 raw dari Record Data berdasarkan _id — v8.0: cascade ke
    spam_claim_queue (raw permanen TAHAP 1) + semua varian TAHAP 2."""
    try:
        if _BACKEND == "mongo":
            from bson import ObjectId  # type: ignore
            _id = ObjectId(id_str)
        else:
            _id = str(id_str)
        doc = await nexus_kalimat_db.find_one({"_id": _id})
        result = await nexus_kalimat_db.delete_one({"_id": _id})
        if result.deleted_count > 0:
            invalidate_nexus_counts()
            await _nexus_cascade_delete_by_claim_key(doc.get("claim_key") if doc else None)
        return result.deleted_count > 0
    except Exception:
        return False


async def nexus_save_regex_bulk(pola_list: list[tuple[str, str]]):
    await nexus_regex_db.delete_many({})
    if pola_list:
        docs = [
            {
                "pola":       p,
                "kata_kunci": k,
                "created_at": datetime.now(TZ_WIB),
            }
            for p, k in pola_list
        ]
        await nexus_regex_db.insert_many(docs)
    invalidate_nexus_counts()


async def nexus_get_all_regex() -> list[dict]:
    return [
        {"pola": d["pola"], "kata_kunci": d["kata_kunci"]}
        async for d in nexus_regex_db.find({})
    ]


async def nexus_get_regex_count() -> int:
    global _nexus_regex_count_cache
    now = time.monotonic()
    if _nexus_regex_count_cache and (now - _nexus_regex_count_cache[1]) < NEXUS_COUNT_TTL:
        return _nexus_regex_count_cache[0]
    n = await nexus_regex_db.count_documents({})
    _nexus_regex_count_cache = (n, now)
    return n


async def nexus_delete_regex_by_pola(pola: str) -> bool:
    result = await nexus_regex_db.delete_one({"pola": pola})
    return result.deleted_count > 0


async def nexus_get_regex_page(page: int, limit: int = 5) -> tuple[list[dict], int]:
    total  = await nexus_regex_db.count_documents({})
    offset = (page - 1) * limit
    rows   = [
        {"pola": d["pola"], "kata_kunci": d["kata_kunci"]}
        async for d in nexus_regex_db.find({}).sort("_id", -1).skip(offset).limit(limit)
    ]
    return rows, total


async def nexus_get_kalimat_page(page: int, limit: int = 10) -> tuple[list[dict], int]:
    total  = await nexus_kalimat_db.count_documents({})
    offset = (page - 1) * limit
    rows   = [
        {
            "_id":           d["_id"],
            "teks":          d["teks"],
            "status_proses": d.get("status_proses", 0),
        }
        async for d in nexus_kalimat_db.find({}).sort("_id", -1).skip(offset).limit(limit)
    ]
    return rows, total


async def nexus_track_grup(chat_id: int, judul: str, username: str | None = None):
    set_fields = {"chat_id": chat_id, "judul": judul, "is_group": True}
    # Username grup publik (tanpa "@") — None/"" berarti grup privat, field
    # tetap ditulis supaya grup yang baru ganti dari publik→privat ikut
    # ter-update (tidak menyisakan username lama yang sudah tidak valid).
    set_fields["username"] = username or None
    await nexus_grup_db.update_one(
        {"chat_id": chat_id},
        {"$set": set_fields},
        upsert=True,
    )


async def nexus_remove_grup(chat_id: int):
    await nexus_grup_db.delete_one({"chat_id": chat_id})


async def nexus_get_all_grup() -> list[dict]:
    global _nexus_grup_cache
    now = time.monotonic()
    if _nexus_grup_cache and (now - _nexus_grup_cache[1]) < NEXUS_COUNT_TTL:
        return _nexus_grup_cache[0]
    result = [
        {
            "chat_id":  d["chat_id"],
            "judul":    d.get("judul", str(d["chat_id"])),
            "username": d.get("username"),
        }
        async for d in nexus_grup_db.find({"is_group": True})
    ]
    _nexus_grup_cache = (result, now)
    return result


async def nexus_clear_kalimat():
    """PURGE total Record Data — v8.0: ikut kosongkan spam_claim_queue
    (raw permanen TAHAP 1) + nexus_kalimat_variants (varian TAHAP 2).
    Ini wipe total ("mulai dari nol"), jadi TIDAK mencoba untrain Bayes
    per varian satu-satu (beda dengan hapus 1 kalimat lewat Record Data —
    lihat nexus_delete_kalimat/_by_id yang untrain per item)."""
    await nexus_kalimat_db.delete_many({})
    await nexus_regex_db.delete_many({})
    await spam_claim_queue_db.delete_many({})
    await nexus_kalimat_variants_db.delete_many({})
    invalidate_nexus_counts()


async def nexus_clear_regex():
    await nexus_regex_db.delete_many({})
    invalidate_nexus_counts()


# ══════════════════════════════════════════════════════════════════════════════
# NEXUS WHITELIST HELPERS
# ══════════════════════════════════════════════════════════════════════════════

async def nexus_whitelist_add(pola: str, raw: str, kata_list: list, mutasi: dict) -> bool:
    try:
        await nexus_whitelist_db.update_one(
            {"pola": pola},
            {
                "$set": {
                    "pola":       pola,
                    "raw":        raw,
                    "kata_list":  kata_list,
                    "mutasi":     mutasi,
                    "created_at": datetime.now(TZ_WIB),
                }
            },
            upsert=True,
        )
        invalidate_nexus_counts()
        return True
    except Exception:
        return False


async def nexus_whitelist_get_all() -> list[dict]:
    return [doc async for doc in nexus_whitelist_db.find({})]


async def nexus_whitelist_count() -> int:
    global _nexus_wl_count_cache
    now = time.monotonic()
    if _nexus_wl_count_cache and (now - _nexus_wl_count_cache[1]) < NEXUS_COUNT_TTL:
        return _nexus_wl_count_cache[0]
    n = await nexus_whitelist_db.count_documents({})
    _nexus_wl_count_cache = (n, now)
    return n


async def get_owner_regex_count() -> int:
    """Count Trigger AI / Owner Regex (regex_db) dengan cache NEXUS_COUNT_TTL detik.
    (Nama fungsi tetap get_owner_regex_count untuk kompatibilitas caller lama;
    fitur ini sekarang dibrand ulang sebagai "Trigger AI" di panel/UI.)
    """
    global _nexus_owner_regex_count_cache
    now = time.monotonic()
    if _nexus_owner_regex_count_cache and (now - _nexus_owner_regex_count_cache[1]) < NEXUS_COUNT_TTL:
        return _nexus_owner_regex_count_cache[0]
    n = await regex_db.count_documents({})
    _nexus_owner_regex_count_cache = (n, now)
    return n


async def nexus_regex_delete_by_id(object_id) -> bool:
    """Hapus satu pola dari regex_db (Trigger AI / Owner Regex) berdasarkan _id dokumen."""
    try:
        if _BACKEND == "mongo":
            from bson import ObjectId  # type: ignore
            result = await regex_db.delete_one({"_id": ObjectId(str(object_id))})
        else:
            result = await regex_db.delete_one({"_id": str(object_id)})
        if result.deleted_count > 0:
            invalidate_nexus_counts()
        return result.deleted_count > 0
    except Exception:
        return False


async def nexus_whitelist_delete_by_id(object_id) -> bool:
    try:
        if _BACKEND == "mongo":
            from bson import ObjectId  # type: ignore
            result = await nexus_whitelist_db.delete_one({"_id": ObjectId(str(object_id))})
        else:
            result = await nexus_whitelist_db.delete_one({"_id": str(object_id)})
        if result.deleted_count > 0:
            invalidate_nexus_counts()
        return result.deleted_count > 0
    except Exception:
        return False


# ══════════════════════════════════════════════════════════════════════════════
# KATEGORI KATA (Owner) — CategoryDetector custom raw words
# ══════════════════════════════════════════════════════════════════════════════
# Beda dari Trigger AI (regex_db): TIDAK ada generate pola/interlock sama
# sekali. Owner cuma menambah kata/frasa APA ADANYA per kategori
# (GROUP_INVITE/PORN/SCAM/PROMO_VIRAL/BIO_PROMO) — dicek via substring match
# sederhana di nexus/ai_core/category_detector.py::_check_custom(), bukan
# regex. Cocok buat owner yang mau nambah kata cepat tanpa paham sintaks regex.

CATEGORY_WORD_LIST = [
    "GROUP_INVITE", "PORN", "SCAM", "PROMO_VIRAL", "BIO_PROMO",
    "JUDI_SLOT", "INVESTASI_BODONG", "JUAL_AKUN", "GCAST_SPAM",
    "PINJOL_JUDOL", "SHORTLINK_SPAM",
]


async def get_category_word_count(category: str) -> int:
    return await category_custom_words_db.count_documents({"category": category})


async def get_category_words_page(category: str, page: int, limit: int = 8) -> tuple[list[dict], int]:
    offset = (page - 1) * limit
    total  = await get_category_word_count(category)
    docs   = [doc async for doc in category_custom_words_db.find({"category": category}).sort("_id", -1).skip(offset).limit(limit)]
    return docs, total


async def add_category_word(category: str, raw: str, added_by: int) -> bool:
    """Simpan 1 kata/frasa RAW baru untuk kategori. Dedupe per kategori
    (case-insensitive) — kata yang sama tidak disimpan dobel."""
    raw = raw.strip()
    if not raw:
        return False
    raw_lower = raw.lower()
    existing = await category_custom_words_db.find_one({"category": category, "raw_lower": raw_lower})
    if existing:
        return False
    await category_custom_words_db.insert_one({
        "category":  category,
        "raw":       raw[:200],
        "raw_lower": raw_lower[:200],
        "added_by":  added_by,
        "added_at":  time.time(),
    })
    return True


async def delete_category_word_by_id(object_id) -> str | None:
    """Hapus 1 kata kustom by id. Return raw text-nya kalau berhasil dihapus
    (dipakai caller buat untrain Bayes juga), atau None kalau gagal/tidak ada."""
    try:
        if _BACKEND == "mongo":
            from bson import ObjectId  # type: ignore
            oid = ObjectId(str(object_id))
        else:
            oid = str(object_id)
        doc = await category_custom_words_db.find_one({"_id": oid})
        if not doc:
            return None
        result = await category_custom_words_db.delete_one({"_id": oid})
        if result.deleted_count > 0:
            return doc.get("raw")
        return None
    except Exception:
        return None


async def get_all_category_words() -> dict[str, list[str]]:
    """Ambil SEMUA kata kustom (semua kategori) sekaligus — dipakai untuk
    mengisi ulang cache in-memory di category_detector.py (reload_custom_words),
    karena CategoryDetector.detect() sinkron/tidak bisa query Mongo langsung."""
    out: dict[str, list[str]] = {c: [] for c in CATEGORY_WORD_LIST}
    async for doc in category_custom_words_db.find({}):
        cat = doc.get("category")
        raw = doc.get("raw", "")
        if cat in out and raw:
            out[cat].append(raw)
    return out


async def nexus_whitelist_page(page: int, limit: int = 5) -> tuple[list[dict], int]:
    total  = await nexus_whitelist_db.count_documents({})
    offset = (page - 1) * limit
    rows   = [
        doc
        async for doc in nexus_whitelist_db.find({}).sort("_id", -1).skip(offset).limit(limit)
    ]
    return rows, total


async def nexus_whitelist_clear() -> int:
    result = await nexus_whitelist_db.delete_many({})
    invalidate_nexus_counts()
    return result.deleted_count


# ══════════════════════════════════════════════════════════════════════════════
# NEXUS ACTION LOG HELPERS
# ══════════════════════════════════════════════════════════════════════════════
# Menyimpan riwayat tindakan bot (hapus, whitelist spared, keroyokan)
# agar bisa dipantau langsung dari panel bot tanpa buka LOG_CHANNEL.
# Maksimum 500 entri — entri terlama otomatis dihapus saat melewati batas.
# ══════════════════════════════════════════════════════════════════════════════

_ACTLOG_MAX = 500


async def nexus_actlog_insert(
    aksi:        str,    # "HAPUS" | "WHITELIST" | "KEROYOKAN"
    user_id:     int,
    user_name:   str,
    chat_id:     int,
    chat_title:  str,
    alasan:      str,    # kata kunci / layer AI
    confidence:  float,  # 0.0 jika bukan AI
    content:     str,    # cuplikan pesan (maks 200 char)
) -> None:
    try:
        doc = {
            "aksi":       aksi,
            "user_id":    user_id,
            "user_name":  user_name,
            "chat_id":    chat_id,
            "chat_title": chat_title,
            "alasan":     alasan[:200],
            "confidence": round(confidence, 4),
            "content":    content[:200],
            "ts":         datetime.now(TZ_WIB),
        }
        await nexus_actlog_db.insert_one(doc)
        # Pangkas jika melebihi batas
        total = await nexus_actlog_db.count_documents({})
        if total > _ACTLOG_MAX:
            # Hapus 50 entri terlama sekaligus
            oldest = [
                d["_id"]
                async for d in nexus_actlog_db.find({}).sort("_id", 1).limit(50)
            ]
            if oldest:
                await nexus_actlog_db.delete_many({"_id": {"$in": oldest}})
    except Exception as e:
        print(f"[DB] actlog_insert error (non-fatal): {e}")


async def nexus_actlog_get_page(page: int, limit: int = 5) -> tuple[list[dict], int]:
    total  = await nexus_actlog_db.count_documents({})
    offset = (page - 1) * limit
    rows   = [
        doc
        async for doc in nexus_actlog_db.find({}).sort("_id", -1).skip(offset).limit(limit)
    ]
    return rows, total


async def nexus_actlog_count() -> int:
    return await nexus_actlog_db.count_documents({})


async def nexus_actlog_clear() -> int:
    result = await nexus_actlog_db.delete_many({})
    return result.deleted_count


# ══════════════════════════════════════════════════════════════════════════════
# AI DEBUG LOG — 24h TTL
# Log internal aktivitas nexus/ai_core/ untuk dipantau owner.
# Data lama otomatis dihapus saat fungsi get_page dipanggil.
# ══════════════════════════════════════════════════════════════════════════════

_ai_debug_db = db["ai_debug_log"]


async def ai_debug_log_insert(
    aksi:       str,
    label:      str   = "-",
    confidence: float = 0.0,
    ringkasan:  str   = "",
    chat_id:    int   = 0,
) -> None:
    """Simpan satu entri log debug AI. Non-blocking, gagal diam-diam."""
    try:
        ts_now = int(datetime.now(timezone.utc).timestamp())
        doc = {
            "ts":         ts_now,
            "aksi":       aksi[:40],
            "label":      label[:16],
            "confidence": round(float(confidence), 4),
            "ringkasan":  ringkasan[:180],
            "chat_id":    int(chat_id),
        }
        await _ai_debug_db.insert_one(doc)
    except Exception as e:
        print(f"[DB] ai_debug_log_insert error: {e}")


async def ai_debug_log_get_page(
    page:     int = 1,
    per_page: int = 5,
) -> tuple[list[dict], int]:
    """
    Ambil halaman log debug AI (24 jam terakhir), urut terbaru dulu.

    v2 — sebelumnya fungsi ini SELALU load SELURUH koleksi ke RAM
    (find({}).to_list(None)), filter+sort di Python, LALU hapus tiap entri
    basi satu-satu (delete_one dalam loop) — semua itu terjadi ulang SETIAP
    kali panel Debug AI dibuka ATAU digeser ke halaman lain. Jauh lebih berat
    dibanding fungsi pagination serupa (nexus_actlog_get_page, dkk) yang
    query+sort+skip+limit langsung di level DB. Sekarang disamakan polanya:
      - count_documents()/find().sort().skip().limit() di level DB (bukan
        load-semua-lalu-filter-di-Python).
      - Cleanup entri >24 jam jadi 1x delete_many bulk (bukan N delete_one),
        dan HANYA dijalankan di halaman 1 — geser ke halaman lain tidak ikut
        menanggung biaya cleanup.
      - Pakai operator $gt (cutoff-1), BUKAN $gte — shim SQLite fallback di
        file ini (lihat _match()) cuma dukung $gt/$lt/$ne/$exists/$in, tidak
        ada $gte, supaya perilaku Mongo & SQLite tetap identik.

    Returns: (docs_halaman_ini, total_dalam_24j)
    """
    try:
        cutoff = int((datetime.now(timezone.utc) - timedelta(hours=24)).timestamp())

        if page <= 1:
            try:
                await _ai_debug_db.delete_many({"ts": {"$lt": cutoff}})
            except Exception as e:
                print(f"[DB] ai_debug_log cleanup error (non-fatal): {e}")

        query  = {"ts": {"$gt": cutoff - 1}}
        total  = await _ai_debug_db.count_documents(query)
        offset = (page - 1) * per_page
        rows = [
            doc
            async for doc in _ai_debug_db.find(query).sort("ts", -1).skip(offset).limit(per_page)
        ]
        return rows, total

    except Exception as e:
        print(f"[DB] ai_debug_log_get_page error: {e}")
        return [], 0


# ══════════════════════════════════════════════════════════════════════════════
# DM USER REGISTRY — untuk broadcast notifikasi shutdown/maintenance
# ══════════════════════════════════════════════════════════════════════════════

_dm_users_db = db["dm_users"]


async def register_dm_user(user_id: int) -> None:
    """Catat user yang pernah berinteraksi dengan bot via DM."""
    try:
        ts_now = int(datetime.now(timezone.utc).timestamp())
        await _dm_users_db.update_one(
            {"user_id": user_id},
            {"$set": {"user_id": user_id, "ts": ts_now}},
            upsert=True,
        )
    except Exception as e:
        print(f"[DB] register_dm_user error: {e}")


async def get_all_dm_users() -> list[int]:
    """Ambil semua user_id yang terdaftar untuk broadcast shutdown."""
    try:
        docs = await _dm_users_db.find({}).to_list(None)
        return [d["user_id"] for d in docs if isinstance(d.get("user_id"), int)]
    except Exception as e:
        print(f"[DB] get_all_dm_users error: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
# GROUP TRIAL HISTORY — riwayat PERMANEN grup yang pernah dapat jatah gratis
# 3 hari Upgrade Speed ("mode flash") untuk grup baru.
#
# SENGAJA collection terpisah dari config_db / nexus_grup_db — TIDAK PERNAH
# dihapus oleh nexus_remove_grup() (saat bot di-kick/di-unadmin/keluar grup)
# ataupun oleh reset data lain. Tujuannya: kalau grup itu pernah tercatat di
# sini, dia TIDAK BOLEH dapat jatah trial gratis lagi — walau bot sempat di-
# kick lalu diundang masuk lagi, atau grup di-unadmin lalu di-admin ulang.
# ══════════════════════════════════════════════════════════════════════════════

group_trial_history_db = db["group_trial_history"]


async def try_register_new_group_trial(chat_id: int) -> bool:
    """
    Coba catat grup ini sebagai "sudah pernah pakai jatah trial gratis".

    Return True HANYA kalau ini kali PERTAMA grup ini tercatat di riwayat
    (artinya grup ini benar-benar baru & BERHAK dapat jatah 3 hari gratis).
    Return False kalau grup ini sudah pernah tercatat sebelumnya kapan pun
    (termasuk kalau bot sempat di-kick/di-unadmin lalu dipasang lagi) — jadi
    tidak dapat jatah trial kedua.

    Pakai $setOnInsert + upsert supaya atomic/race-safe — aman dipanggil
    berkali-kali untuk chat_id yang sama tanpa risiko dobel-grant kalau
    event ChatMemberUpdated datang beruntun cepat.
    """
    try:
        result = await group_trial_history_db.update_one(
            {"chat_id": chat_id},
            {"$setOnInsert": {
                "chat_id":       chat_id,
                "first_seen_at": time.time(),
            }},
            upsert=True,
        )
        return result.upserted_id is not None
    except Exception as e:
        print(f"[TrialHistory] gagal cek/catat grup={chat_id}: {e}")
        return False


async def has_group_used_trial(chat_id: int) -> bool:
    """Cek apakah grup ini sudah pernah tercatat di riwayat trial (kapan pun)."""
    try:
        doc = await group_trial_history_db.find_one({"chat_id": chat_id})
        return doc is not None
    except Exception as e:
        print(f"[TrialHistory] gagal cek grup={chat_id}: {e}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
# LOCAL MUTE TRACKER — anti-duplikasi eskalasi hukuman
# ══════════════════════════════════════════════════════════════════════════════
# Schema per dokumen:
#   _id         : "lmute_{chat_id}_{user_id}"
#   chat_id     : int
#   user_id     : int
#   consec_spam : int    — hitungan duplikat berturut-turut (reset jika pesan bersih)
#   mute_level  : int    — level eskalasi; 0 = 5 mnt, 1 = 10 mnt, 2 = 20 mnt, dst.
#   muted_until : float  — unix timestamp akhir mute; 0.0 jika tidak sedang mute
#   updated_at  : float  — unix timestamp terakhir update
# ══════════════════════════════════════════════════════════════════════════════

local_mute_db = db["local_mute"]
warn_once_db  = db["warn_once"]   # Riwayat pemberitahuan — seumur hidup, tidak ada TTL
spam_claim_queue_db = db["spam_claim_queue"]  # Antrean klaim spam (v7.0) — lihat core/spam_claim_queue.py


async def ensure_spam_claim_index() -> None:
    """
    Pastikan index spam_claim_queue ada — dipanggil sekali saat startup.
    TTL 30 hari (default) pada created_at + index pada `generated` untuk
    query polling worker (generated=False). Lihat core/spam_claim_queue.py.
    """
    if get_active_backend() != "mongo":
        return
    try:
        from core.spam_claim_queue import SPAM_CLAIM_TTL_SECONDS
        await spam_claim_queue_db.create_index(
            "created_at",
            expireAfterSeconds=SPAM_CLAIM_TTL_SECONDS,
        )
        await spam_claim_queue_db.create_index("generated")
    except Exception as e:
        print(f"[DB] Gagal buat index spam_claim_queue: {e}")


_BASE_MUTE_SECONDS = 5 * 60   # 5 menit


def _mute_duration_seconds(mute_level: int) -> int:
    """Durasi mute dalam detik berdasarkan level eskalasi (2^level × 5 menit)."""
    return _BASE_MUTE_SECONDS * (2 ** mute_level)


async def get_local_mute(chat_id: int, user_id: int) -> dict:
    """Ambil atau buat rekaman mute untuk user di grup tertentu.

    FIX (shard routing multi-cluster): query WAJIB menyertakan "chat_id"
    eksplisit di top-level dict — bukan cuma "_id" — karena "local_mute"
    ada di HOT_PATH_COLLECTIONS (di-hash by chat_id ke salah satu
    HOTPATH_CLUSTERS). _resolve_shard_idx() cuma bisa menemukan chat_id
    dari FIELD LITERAL "chat_id" di query/update, bukan dengan mengurai
    string _id — tanpa field ini, SEMUA baca selalu jatuh ke
    hotpath_pool()[0], padahal tulisannya (lewat _save_local_mute) di-hash
    ke shard lain kalau kebetulan chat_id ini hash-nya ke cluster hot-path
    kedua. Akibatnya rekaman mute "hilang" (selalu dianggap belum ada)
    setiap kali SHARD_COUNT membuat >1 cluster hot-path aktif.
    """
    key = f"lmute_{chat_id}_{user_id}"
    doc = await local_mute_db.find_one({"_id": key, "chat_id": chat_id})
    if doc is None:
        doc = {
            "_id":         key,
            "chat_id":     chat_id,
            "user_id":     user_id,
            "consec_spam": 0,
            "mute_level":  0,
            "muted_until": 0.0,
            "updated_at":  time.time(),
        }
    return doc


async def _save_local_mute(doc: dict) -> None:
    doc["updated_at"] = time.time()
    await local_mute_db.update_one(
        {"_id": doc["_id"]},
        {"$set": doc},
        upsert=True,
    )


async def increment_local_spam(chat_id: int, user_id: int) -> dict:
    """
    Tambah hitungan spam berturut-turut SECARA ATOMIK ($inc lewat
    find_one_and_update) — BUKAN lagi baca-lalu-tulis manual (get_local_mute
    lalu _save_local_mute terpisah, 2 round-trip DB dengan window kosong
    di antaranya).

    KENAPA DIUBAH (lost-update race): check_and_punish() (core/punishment.py)
    dipanggil via asyncio.create_task() dari SETIAP gate (regex, link,
    mention, AI Manual, dst) — tidak pernah di-await berurutan. Tiap grup
    juga punya sampai _GROUP_MAX_WORKERS (default 5, core/antispam_queue.py)
    worker yang memproses pesan BERSAMAAN. Kalau user spam yang SAMA kena
    beberapa deteksi nyaris bersamaan (khas serangan flood/userbot spam
    beruntun — persis skenario "AI Manual menghapus banyak pesan berturut
    tapi mute/ban eskalasi tidak pernah kepicu"), beberapa panggilan versi
    lama bisa overlap: masing-masing baca consec_spam LAMA sebelum yang
    lain sempat menulis balik nilai barunya → sebagian increment HILANG
    (classic lost update) → consec_spam tidak pernah benar-benar mencapai
    SPAM_MUTE_THRESHOLD walau jumlah deteksi sungguhan sudah jauh
    melewatinya.

    find_one_and_update dengan $inc dijamin atomik oleh MongoDB sendiri —
    tidak ada window baca-tulis yang bisa ditumpangi panggilan lain,
    berapa pun banyaknya yang berjalan bersamaan untuk (chat_id, user_id)
    yang sama.
    """
    key = f"lmute_{chat_id}_{user_id}"
    doc = await local_mute_db.find_one_and_update(
        {"_id": key},
        {
            "$inc": {"consec_spam": 1},
            "$setOnInsert": {
                "_id": key, "chat_id": chat_id, "user_id": user_id,
                "mute_level": 0, "muted_until": 0.0,
            },
            "$set": {"updated_at": time.time()},
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    return doc


async def apply_local_mute(chat_id: int, user_id: int) -> tuple[int, int]:
    """
    Terapkan mute berdasarkan level saat ini.
    Kembalikan (durasi_detik, level_yang_dipakai).
    Setelah dipanggil: consec_spam TIDAK direset ke 0 melainkan tetap di ambang
    (SPAM_MUTE_THRESHOLD) agar setelah mute habis, 1 pelanggaran berikutnya
    langsung memicu mute level berikutnya — hitungan punishment dilanjutkan,
    bukan dimulai dari awal. Restart bot tidak mempengaruhi ini karena
    consec_spam dan muted_until tersimpan persisten di database.

    PENTING: Fungsi ini menulis muted_until SEBELUM aksi mute API benar-benar
    dieksekusi (eksekusi terjadi async via core/moderation_queue.py). Jika
    eksekusi API gagal (bot bukan admin / kehilangan izin), pemanggil WAJIB
    memanggil revert_failed_local_mute(chat_id, user_id, level) — lihat
    fungsi tersebut — agar state "muted" tidak tertinggal palsu di DB.
    """
    doc      = await get_local_mute(chat_id, user_id)
    level    = doc.get("mute_level", 0)
    duration = _mute_duration_seconds(level)
    doc["muted_until"] = time.time() + duration
    # Pertahankan consec_spam di ambang (SPAM_MUTE_THRESHOLD, custom via .env)
    # agar setelah mute habis langsung mute lagi pada pelanggaran pertama
    # (bukan harus mengulang dari awal lagi).
    doc["consec_spam"] = SPAM_MUTE_THRESHOLD
    doc["mute_level"]  = level + 1
    await _save_local_mute(doc)
    return duration, level


async def revert_failed_local_mute(chat_id: int, user_id: int, level_before: int) -> None:
    """
    Rollback state mute jika eksekusi API mute GAGAL (ChatAdminRequired,
    FloodWait lama yang di-skip, dsb) setelah apply_local_mute() sudah
    menulis muted_until ke DB.

    FIXED: Sebelumnya muted_until tetap tertinggal di DB walau mute API
    gagal — akibatnya pesan user berikutnya tetap dihapus otomatis (karena
    filter lain hanya cek muted_until di DB, bukan status mute asli di
    Telegram), padahal user tidak benar-benar dibatasi kirim pesan oleh
    Telegram. Sekarang muted_until direset ke 0 dan mute_level dikembalikan
    ke level sebelumnya, sehingga:
      - Pesan berikutnya TIDAK dihapus berdasarkan state mute palsu.
      - consec_spam tetap di ambang (SPAM_MUTE_THRESHOLD) — pelanggaran
        berikutnya tetap langsung memicu percobaan mute lagi.
      - mute_level tidak naik akibat percobaan yang gagal.
    """
    key = f"lmute_{chat_id}_{user_id}"
    doc = await local_mute_db.find_one({"_id": key, "chat_id": chat_id})
    if doc is None:
        return
    doc["muted_until"] = 0.0
    doc["mute_level"]  = level_before
    await _save_local_mute(doc)


async def reset_local_mute(chat_id: int, user_id: int) -> None:
    """
    Reset hitungan dan level hukuman (dipanggil saat pesan bersih diterima).
    Jika belum ada rekaman, tidak melakukan apa-apa.
    """
    key = f"lmute_{chat_id}_{user_id}"
    doc = await local_mute_db.find_one({"_id": key, "chat_id": chat_id})
    if doc is None:
        return
    # Hanya reset jika ada sesuatu yang perlu direset
    if doc.get("consec_spam", 0) == 0 and doc.get("mute_level", 0) == 0:
        return
    doc["consec_spam"] = 0
    doc["mute_level"]  = 0
    await _save_local_mute(doc)


# ══════════════════════════════════════════════════════════════════════════════
# WARN ONCE — pemberitahuan 1× SEUMUR HIDUP per (user, jenis_spam) — GLOBAL,
# LINTAS GRUP (bukan per grup). Begitu seorang user pernah diberi tahu untuk
# 1 jenis pelanggaran (di grup manapun), dia TIDAK akan diberi tahu ulang
# untuk jenis yang sama lagi di grup lain — sesuai desain: 1 tipe pelanggaran
# × 1 user = 1x notifikasi seumur hidup, titik, di manapun dia berada.
# ══════════════════════════════════════════════════════════════════════════════
# Dokumen: { "_id": "warn_{user_id}_{warn_type}" }
# `chat_id` tetap disimpan (via $setOnInsert) HANYA sebagai catatan grup
# tempat notif PERTAMA kali terkirim — bukan bagian dari kunci keunikan,
# jadi TIDAK memengaruhi logika has_warned_user() di grup lain.
# Tidak ada TTL/expiry — sekali tersimpan, berlaku selamanya.
#
# warn_type yang digunakan:
#   "dup"           — spam duplikat lokal (antispam.py)
#   "gcast"         — anti-gcast global (antispam.py)
#   "vc_bio"        — mute mic VC karena bio mengandung link
#   "vc_nonmember"  — mute mic VC karena bukan anggota grup
#   "vc_peer"       — mute mic VC karena peer belum dikenali
#   "typing"        — mute typing (jika ada filter typing di masa depan)
#
# CATATAN MIGRASI: sebelum perubahan ini, key menyertakan chat_id (per grup).
# Entri lama otomatis tidak terpakai lagi (key beda) — user yang sudah
# pernah diwarn di skema lama akan menerima 1x notif baru di skema global
# ini, setelah itu berlaku 1x seumur hidup seperti seharusnya.
# ══════════════════════════════════════════════════════════════════════════════

async def has_warned_user(chat_id: int, user_id: int, warn_type: str) -> bool:
    """
    Kembalikan True jika user ini sudah PERNAH diberi pemberitahuan jenis
    warn_type — GLOBAL, di grup manapun (chat_id hanya dipertahankan di
    signature untuk kompatibilitas pemanggil lama, tidak dipakai di key).
    Operasi read-only, sangat ringan (lookup by _id).
    """
    key = f"warn_{user_id}_{warn_type}"
    doc = await warn_once_db.find_one({"_id": key})
    return doc is not None


async def mark_warned_user(chat_id: int, user_id: int, warn_type: str) -> None:
    """
    Tandai bahwa user ini sudah diberi pemberitahuan jenis warn_type —
    GLOBAL, berlaku di semua grup. Idempotent — aman dipanggil berkali-kali.
    `chat_id` hanya dicatat sebagai info grup pertama kali notif terkirim
    (tidak ditimpa pada pemanggilan berikutnya dari grup lain).
    """
    key = f"warn_{user_id}_{warn_type}"
    await warn_once_db.update_one(
        {"_id": key},
        {
            "$setOnInsert": {
                "_id": key, "chat_id": chat_id, "user_id": user_id,
                "warn_type": warn_type, "ts": time.time(),
            },
        },
        upsert=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# GROUP ACTION LOG — log aksi per grup (hapus/mute/ban), TTL 7 hari
# ══════════════════════════════════════════════════════════════════════════════

# FIX (performa lambat saat buka/geser menu Log Aktivitas):
#   Sebelumnya get_group_action_log_page menarik SEMUA dokumen grup itu dari DB
#   ke memori (`.find({"chat_id": chat_id}).to_list(None)`), lalu filter/​sort
#   7-hari dan cleanup expired dilakukan satu-per-satu di Python — termasuk
#   `delete_one` berulang di dalam loop tiap kali halaman dibuka/digeser.
#   Untuk grup yang sudah aktif berhari-hari, ini bisa berarti ribuan dokumen
#   ditarik & di-sort ulang hanya untuk menampilkan 10 baris per halaman,
#   sehingga next/prev terasa lambat.
#
#   Perbaikan:
#     1. Index compound (chat_id, ts) dibuat sekali saat startup (idempotent)
#        agar query & sort di MongoDB memakai index, bukan full scan.
#     2. Query halaman langsung pakai sort+skip+limit di level DB (lewat
#        AsyncCursor yang sudah mendukung ini di kedua backend), bukan narik
#        semua dokumen lalu slice di Python.
#     3. Total dihitung via count_documents (bukan len() dari semua dokumen).
#     4. Cleanup entri expired (>7 hari) sekarang satu panggilan delete_many,
#        bukan loop delete_one per dokumen.
_group_action_log_index_created = False

# Throttle cleanup expired — jangan delete_many di SETIAP render halaman,
# cukup sekali per grup per _CLEANUP_INTERVAL detik (cleanup tetap akurat
# karena query halaman selalu pakai filter ts > cutoff secara terpisah).
_group_action_log_last_cleanup: dict[int, float] = {}
_GROUP_ACTION_LOG_CLEANUP_INTERVAL = 600   # 10 menit


async def _ensure_group_action_log_index() -> None:
    """
    Buat index compound (chat_id, ts desc) pada group_action_log.
    Aman dipanggil berulang (idempotent) — no-op di SQLite.
    """
    global _group_action_log_index_created
    if _group_action_log_index_created:
        return
    try:
        from pymongo import ASCENDING, DESCENDING  # type: ignore
        await group_action_log_db.create_index(
            [("chat_id", ASCENDING), ("ts", DESCENDING)],
        )
        _group_action_log_index_created = True
    except Exception as e:
        print(f"[DB] Gagal buat index group_action_log: {e}")


async def insert_group_action_log(
    chat_id:   int,
    aksi:      str,   # "HAPUS" | "MUTE" | "BAN" | "UNADMIN" | "SECOS" | "MUTE-VC-MIC" | "UNMUTE-VC-MIC"
    alasan:    str,   # teks detail bebas (pola cocok, durasi, dll) — BUKAN label jenis
    user_id:   int,
    user_name: str,
    konten:    str = "",
    jenis:     str | None = None,   # kode VIOLATION_* dari core/violation_types.py
) -> None:
    """
    Simpan satu entri log aksi ke group_action_log. Non-blocking, gagal diam-diam.

    `jenis` (BARU): kode terstruktur (lihat core/violation_types.py) yang
    menentukan icon + label Indonesia seragam di panel log grup & LOG_CHANNEL.
    Opsional untuk backward-compat — entri tanpa `jenis` (atau ditulis sebelum
    field ini ada) tetap tampil lewat fallback generik di violation_types.py,
    tidak pernah error.
    """
    try:
        doc = {
            "chat_id":   chat_id,
            "ts":        time.time(),
            "aksi":      aksi[:20],
            "alasan":    alasan[:120],
            "user_id":   user_id,
            "user_name": user_name[:50],
            "konten":    konten[:100],
            "jenis":     jenis,
        }
        await group_action_log_db.insert_one(doc)
    except Exception as e:
        print(f"[DB] insert_group_action_log error (non-fatal): {e}")


async def get_group_action_log_page(
    chat_id:  int,
    page:     int = 1,
    per_page: int = 10,
) -> tuple[list[dict], int]:
    """
    Ambil halaman log aksi grup (7 hari terakhir), urut terbaru dulu.
    Sekaligus bersihkan entri > 7 hari.

    FIXED: tidak lagi menarik semua dokumen ke memori — sort/skip/limit
    dilakukan di level DB, dan cleanup expired pakai satu delete_many.
    """
    try:
        await _ensure_group_action_log_index()
        cutoff = time.time() - (7 * 86400)

        # Bersihkan entri expired — di-throttle per grup, bukan setiap render.
        # Query halaman tetap akurat karena selalu filter ts > cutoff secara
        # terpisah, jadi entri expired tidak akan pernah tampil meski belum
        # sempat dibersihkan dari DB.
        now_mono = time.monotonic()
        last_cleanup = _group_action_log_last_cleanup.get(chat_id, 0.0)
        if now_mono - last_cleanup >= _GROUP_ACTION_LOG_CLEANUP_INTERVAL:
            _group_action_log_last_cleanup[chat_id] = now_mono
            try:
                await group_action_log_db.delete_many(
                    {"chat_id": chat_id, "ts": {"$lt": cutoff}}
                )
            except Exception:
                pass

        query = {"chat_id": chat_id, "ts": {"$gt": cutoff}}
        total = await group_action_log_db.count_documents(query)

        start = (page - 1) * per_page
        docs  = await (
            group_action_log_db.find(query)
            .sort("ts", -1)
            .skip(start)
            .limit(per_page)
            .to_list(None)
        )
        return docs, total
    except Exception as e:
        print(f"[DB] get_group_action_log_page error: {e}")
        return [], 0


async def get_user_violations_lintas_grup(user_id: int, limit: int = 5) -> list[dict]:
    """
    Ambil N pelanggaran terakhir user ini lintas SEMUA grup (7 hari terakhir).
    Dipakai oleh fitur /pelanggaranku — user cek riwayat pelanggarannya sendiri.

    Query by user_id tanpa filter chat_id, sorted terbaru dulu, limit N.
    Entri tanpa field 'jenis' (data lama) tetap ikut — ditampilkan dengan
    fallback label generik lewat get_violation_meta(None) di violation_types.py.
    """
    try:
        cutoff = time.time() - (7 * 86400)
        docs = await (
            group_action_log_db
            .find({"user_id": user_id, "ts": {"$gt": cutoff}})
            .sort("ts", -1)
            .limit(limit)
            .to_list(None)
        )
        return docs
    except Exception as e:
        print(f"[DB] get_user_violations_lintas_grup error: {e}")
        return []


# ── Bangrup — grup dilarang (force leave + block re-add) ───────────────────────
# Dipakai oleh /bangrup, /unbangrup, /listbangrup (plugins/commands/bangrup.py).
# Dokumen: {"_id": chat_id, "banned_at": ts, "banned_by": owner_id,
#           "title": judul grup terakhir diketahui (opsional, buat /listbangrup)}

async def bangrup_add(chat_id: int, banned_by: int, title: str = "") -> None:
    try:
        await banned_groups_db.update_one(
            {"_id": chat_id},
            {"$set": {
                "banned_at": time.time(),
                "banned_by": banned_by,
                "title":     title[:100],
            }},
            upsert=True,
        )
    except Exception as e:
        print(f"[DB] bangrup_add error: {e}")


async def bangrup_remove(chat_id: int) -> bool:
    try:
        result = await banned_groups_db.delete_one({"_id": chat_id})
        return bool(getattr(result, "deleted_count", 0))
    except Exception as e:
        print(f"[DB] bangrup_remove error: {e}")
        return False


async def bangrup_is_banned(chat_id: int) -> bool:
    try:
        doc = await banned_groups_db.find_one({"_id": chat_id})
        return doc is not None
    except Exception as e:
        print(f"[DB] bangrup_is_banned error: {e}")
        return False


async def bangrup_list() -> list[dict]:
    try:
        return await banned_groups_db.find({}).sort("banned_at", -1).to_list(None)
    except Exception as e:
        print(f"[DB] bangrup_list error: {e}")
        return []


# ── NewsCore (Sistem Skor Keaktifan & Admin Otomatis) ──────────────────────────
newscore_stats_db  = db["newscore_stats"]   # skor chat per user per grup
newscore_admin_db  = db["newscore_admins"]  # riwayat admin aktif yang diangkat
newscore_cfg_db    = db["newscore_config"]  # konfigurasi newscore per grup
newscore_titled_db = db["newscore_titled_members"]  # member non-admin yang
                                             # sedang dipasang tag (Auto Title
                                             # Member) — dipakai untuk hapus tag
                                             # otomatis saat member itu tidak lagi
                                             # masuk daftar titel periode baru.
vip_titled_db      = db["vip_titled_members"]  # member VIP yang sedang dipasang
                                             # tag (Title VIP) — dipakai untuk
                                             # hapus tag otomatis saat status VIP
                                             # member itu hilang (lihat
                                             # core/vip_bio_guard.py & free.py).

# ── Cache ns_get_current_admins ───────────────────────────────────────────────
# ns_get_current_admins dipanggil setiap pesan dari NS admin (untuk cek apakah
# dia boleh dihitung skornya). Tanpa cache ini = query MongoDB per pesan.
# Cache TTL sama dengan ADMIN_TTL (120 detik) — konsisten dengan is_admin().
# Key: chat_id, Value: (list_admins, timestamp)
_ns_current_admins_cache: dict[int, tuple[list, float]] = {}
_NS_ADMINS_CACHE_TTL = ADMIN_TTL  # 120 detik — ikut ADMIN_TTL agar konsisten

# ── NewsCore score buffer (rate-limit aman) ───────────────────────────────────
# Daripada langsung update_one ke MongoDB per pesan (banyak grup ramai =
# ribuan write/menit), kita buffer skor di memory dulu lalu flush ke DB
# secara batch setiap _NS_FLUSH_INTERVAL detik.
# Buffer: {(chat_id, user_id): {"name": str, "delta": int}}
# Flush worker dijalankan dari main.py setelah client.start().
import collections as _collections
_ns_score_buffer: dict[tuple, dict] = {}
_ns_score_buffer_lock = asyncio.Lock() if False else None  # diinisialisasi di ns_init_flush_worker
_NS_FLUSH_INTERVAL = int(os.environ.get("NS_FLUSH_INTERVAL", 10))  # detik


# ══════════════════════════════════════════════════════════════════════════════
# NEWSCORE — Sistem Skor Keaktifan & Admin Otomatis
# ══════════════════════════════════════════════════════════════════════════════

from datetime import datetime, timedelta as _timedelta

NEWSCORE_DEFAULT = {
    "enabled":        False,
    "mode":           "day",      # "day" | "date" | "weekday"
    "reset_days":     7,
    "reset_date":     1,
    "reset_weekday":  0,
    "reset_hour":     23,
    "reset_minute":   59,
    "max_admins":     1,
    "next_reset":     None,
    "bio_admin_text": "",   # Teks wajib di bio admin yang diangkat NewsCore.
    "bio_admin_required": True,  # False = sengaja dikosongkan via tombol "Kosongkan"
                             # → admin NewsCore TIDAK diwajibkan apapun di bio.
                             # True + bio_admin_text kosong (default awal, belum
                             # pernah diatur sama sekali) = dianggap wajib tapi
                             # mustahil dipenuhi (semua admin NewsCore di-unadmin
                             # sampai diisi ATAU sampai owner pilih "Kosongkan").
    "admin_title": "",      # Titel custom (maks 16 karakter) yang dipasang via
                             # set_administrator_title saat admin
                             # diangkat NewsCore tiap periode reset. Kosong =
                             # pakai titel default bawaan ("Top Member N 👑").
    "auto_title_enabled": False,  # Auto Title Member: tag otomatis (via Bot API
                             # setChatMemberTag) untuk member NON-admin berdasar
                             # rank typing/leaderboard NewsCore, dipasang bareng
                             # ns_do_reset(). Beda dari admin_title (itu khusus
                             # admin yang diangkat NewsCore).
    "auto_title_names": [],  # List hingga 10 nama tag, urut per kelompok rank
                             # 5 besar: index 0 -> rank 1-5, index 1 -> rank 6-10,
                             # dst. Kosong = fitur tidak aktif walau enabled=True.
    "privileges": {
        "can_delete_messages":   True,
        "can_restrict_members":  True,
        "can_invite_users":      True,
        "can_pin_messages":      True,
        "can_manage_video_chats": False,
    },
}

HARI_MAP_NS = {0: "Senin", 1: "Selasa", 2: "Rabu", 3: "Kamis",
               4: "Jumat", 5: "Sabtu", 6: "Minggu"}


async def ns_get_config(chat_id: int) -> dict:
    now = time.monotonic()
    hit = _ns_config_cache.get(chat_id)
    if hit and (now - hit[1]) < NS_CONFIG_TTL:
        return hit[0]
    doc = await newscore_cfg_db.find_one({"chat_id": chat_id})
    cfg = {k: v for k, v in NEWSCORE_DEFAULT.items()}
    cfg["privileges"] = dict(NEWSCORE_DEFAULT["privileges"])
    if doc:
        for k in NEWSCORE_DEFAULT:
            if k in doc:
                cfg[k] = doc[k]
        if "privileges" in doc:
            cfg["privileges"] = dict(doc["privileges"])
    _ns_config_cache[chat_id] = (cfg, now)
    return cfg


async def ns_update(chat_id: int, updates: dict) -> None:
    await newscore_cfg_db.update_one(
        {"chat_id": chat_id},
        {"$set": {"chat_id": chat_id, **updates}},
        upsert=True,
    )
    _ns_config_cache.pop(chat_id, None)  # invalidasi cache panel


def ns_update_optimistic(
    chat_id: int, updates: dict,
    dm_chat_id: int | None = None, dm_msg_id: int | None = None,
) -> dict:
    """
    Versi "ringan" dari ns_update — dipakai oleh tombol panel NewsCore DM.

    Cache di-update langsung (synchronous) supaya render ulang panel
    instan, sedangkan penulisan ke DB diantrikan via panel_write_queue
    dan dieksekusi belakangan oleh panel_write_worker. Jika gagal permanen
    dan dm_chat_id/dm_msg_id diisi, panel itu akan dikoreksi otomatis.

    Return dict config terbaru (hasil optimistic).
    """
    now = time.monotonic()
    hit = _ns_config_cache.get(chat_id)
    if hit:
        cfg = {k: v for k, v in hit[0].items()}
        cfg["privileges"] = dict(hit[0].get("privileges", NEWSCORE_DEFAULT["privileges"]))
    else:
        cfg = {k: v for k, v in NEWSCORE_DEFAULT.items()}
        cfg["privileges"] = dict(NEWSCORE_DEFAULT["privileges"])
    for k, v in updates.items():
        if k == "privileges":
            cfg["privileges"] = dict(v)
        else:
            cfg[k] = v
    _ns_config_cache[chat_id] = (cfg, now)
    enqueue_ns_write(chat_id, updates, dm_chat_id, dm_msg_id)
    return cfg


def ns_calc_next_reset(cfg: dict) -> str:
    # Pakai TZ_WIB eksplisit (bukan datetime.now() naive) agar next_reset
    # tidak meleset jika server hosting berjalan di timezone selain WIB
    # (mis. UTC default di Railway/Docker). Tanpa ini, jam yang dimasukkan
    # owner di UI (dimaksudkan WIB) bisa dieksekusi di jam yang berbeda.
    now   = datetime.now(TZ_WIB)
    h, m  = cfg.get("reset_hour", 23), cfg.get("reset_minute", 59)
    mode  = cfg.get("mode", "day")
    try:
        if mode == "day":
            days   = cfg.get("reset_days", 7)
            target = (now + _timedelta(days=days)).replace(hour=h, minute=m, second=0, microsecond=0)
        elif mode == "date":
            d = cfg.get("reset_date", 1)
            try:
                target = now.replace(day=d, hour=h, minute=m, second=0, microsecond=0)
                if target <= now:
                    raise ValueError
            except ValueError:
                if now.month == 12:
                    target = now.replace(year=now.year + 1, month=1, day=d, hour=h, minute=m, second=0, microsecond=0)
                else:
                    target = now.replace(month=now.month + 1, day=d, hour=h, minute=m, second=0, microsecond=0)
        else:
            wd         = cfg.get("reset_weekday", 0)
            days_ahead = wd - now.weekday()
            if days_ahead <= 0:
                days_ahead += 7
            target = (now + _timedelta(days=days_ahead)).replace(hour=h, minute=m, second=0, microsecond=0)
            if target <= now:
                target += _timedelta(days=7)
        return target.isoformat()
    except Exception:
        return (datetime.now(TZ_WIB) + _timedelta(days=7)).isoformat()


async def ns_track_message(chat_id: int, user_id: int, user_name: str) -> None:
    """
    Tambah 1 poin skor untuk user di grup.

    RATE-LIMIT SAFE: skor dibuffer di memory (_ns_score_buffer) dan di-flush
    ke MongoDB secara batch setiap _NS_FLUSH_INTERVAL detik oleh
    ns_flush_score_buffer(). Ini menghindari ribuan update_one per menit
    saat banyak grup ramai — DB hanya kena hit saat flush.
    """
    key = (chat_id, user_id)
    if key in _ns_score_buffer:
        _ns_score_buffer[key]["delta"] += 1
        _ns_score_buffer[key]["name"]   = user_name  # update nama terbaru
    else:
        _ns_score_buffer[key] = {"name": user_name, "delta": 1}


async def ns_flush_score_buffer() -> None:
    """
    Flush buffer skor ke MongoDB secara batch.
    Dipanggil oleh background worker (ns_flush_worker_loop) setiap
    _NS_FLUSH_INTERVAL detik. Juga dipanggil sebelum ns_reset_scores()
    agar tidak ada skor yang hilang saat reset tiba.
    """
    if not _ns_score_buffer:
        return

    # Ambil snapshot lalu bersihkan buffer (tidak ada asyncio.Lock di sini
    # karena single-threaded event loop — swap atomik cukup)
    snapshot = dict(_ns_score_buffer)
    _ns_score_buffer.clear()

    try:
        from pymongo import UpdateOne
        ops = [
            UpdateOne(
                {"chat_id": chat_id, "user_id": user_id},
                {"$set": {"user_name": data["name"]}, "$inc": {"score": data["delta"]}},
                upsert=True,
            )
            for (chat_id, user_id), data in snapshot.items()
        ]
        if ops:
            # Collection.bulk_write() (lihat database.py) — 1 round-trip per
            # shard yang relevan, BUKAN 1 round-trip per user seperti versi
            # lama yang salah asumsi newscore_stats_db punya atribut ._col
            # (selalu AttributeError → jatuh ke fallback update_one satu-satu,
            # meniadakan tujuan batching ini). newscore_stats_db sendiri bukan
            # collection yang di-shard, jadi ini selalu 1 round-trip total.
            await newscore_stats_db.bulk_write(ops, ordered=False)
    except Exception as e:
        print(f"[NewsCore] flush error: {e}")
        # Kembalikan data ke buffer agar tidak hilang
        for key, data in snapshot.items():
            if key in _ns_score_buffer:
                _ns_score_buffer[key]["delta"] += data["delta"]
            else:
                _ns_score_buffer[key] = data


async def ns_flush_worker_loop() -> None:
    """
    Background worker: flush score buffer ke DB setiap _NS_FLUSH_INTERVAL detik.
    Jalankan sekali dari main.py setelah await app.start().
    """
    while True:
        await asyncio.sleep(_NS_FLUSH_INTERVAL)
        try:
            await ns_flush_score_buffer()
        except Exception as e:
            print(f"[NewsCore] flush_worker_loop error: {e}")


async def ns_get_leaderboard(chat_id: int, limit: int = 10) -> list:
    """
    Ambil leaderboard skor grup dari DB.
    Catatan: skor yang masih di buffer (_ns_score_buffer) belum termasuk.
    Untuk akurasi penuh saat digunakan pada reset, pastikan
    ns_flush_score_buffer() dipanggil terlebih dahulu (lihat ns_do_reset).
    """
    try:
        cur = newscore_stats_db.find({"chat_id": chat_id}).sort("score", -1).limit(limit)
        return await cur.to_list(length=limit)
    except Exception:
        return []


async def ns_get_active_user_count(chat_id: int) -> int:
    """
    Hitung total user aktif (pernah kirim pesan) dalam periode ini.
    Termasuk yang masih di buffer belum di-flush.
    """
    try:
        db_count = await newscore_stats_db.count_documents({"chat_id": chat_id})
        # Tambahkan user di buffer yang belum ada di DB
        buf_users = {uid for (cid, uid) in _ns_score_buffer if cid == chat_id}
        return db_count + len(buf_users)
    except Exception:
        return 0


async def ns_reset_scores(chat_id: int) -> None:
    """
    Reset semua skor di grup. Flush buffer dulu agar tidak ada skor hilang
    yang masih di memory saat reset dipanggil.
    """
    try:
        # Flush buffer → pastikan semua skor masuk DB sebelum dihapus
        await ns_flush_score_buffer()
        await newscore_stats_db.delete_many({"chat_id": chat_id})
    except Exception as e:
        print(f"[NewsCore] reset error: {e}")


async def ns_get_current_admins(chat_id: int) -> list:
    """
    Ambil daftar admin NewsCore aktif di grup.
    Di-cache _NS_ADMINS_CACHE_TTL detik (default 120s, sama dengan ADMIN_TTL)
    untuk menghindari query MongoDB per pesan saat NS admin sering chat.
    """
    now = time.monotonic()
    hit = _ns_current_admins_cache.get(chat_id)
    if hit and (now - hit[1]) < _NS_ADMINS_CACHE_TTL:
        return hit[0]
    try:
        result = await newscore_admin_db.find({"chat_id": chat_id}).to_list(length=20)
    except Exception:
        result = []
    _ns_current_admins_cache[chat_id] = (result, now)
    return result


async def ns_is_current_admin(chat_id: int, user_id: int) -> bool:
    """
    True jika user_id adalah admin NewsCore AKTIF di grup ini saat ini.

    Dipakai untuk MELARANG admin NewsCore menjadi VIP (baik manual /vip
    maupun otomatis via teks bio) — supaya admin NewsCore tidak pernah bisa
    lolos dari pengecekan "Bio Admin Wajib" hanya karena kebetulan (atau
    disengaja) bio-nya juga memenuhi teks VIP Bio grup yang sama.
    """
    try:
        admins = await ns_get_current_admins(chat_id)
        return user_id in {a["user_id"] for a in admins}
    except Exception:
        return False


def invalidate_ns_admins_cache(chat_id: int) -> None:
    """Hapus cache ns_get_current_admins untuk grup tertentu.
    Dipanggil setelah ns_set_current_admins() atau ns_remove_admin()
    agar data selalu fresh setelah ada perubahan daftar admin.
    """
    _ns_current_admins_cache.pop(chat_id, None)


async def ns_set_current_admins(chat_id: int, admins: list) -> None:
    try:
        await newscore_admin_db.delete_many({"chat_id": chat_id})
        if admins:
            await newscore_admin_db.insert_many(admins)
        invalidate_ns_admins_cache(chat_id)
    except Exception as e:
        print(f"[NewsCore] set admins error: {e}")


async def ns_remove_admin(chat_id: int, user_id: int) -> None:
    """Hapus satu admin NewsCore dari daftar admin aktif (tanpa menyentuh admin lain)."""
    try:
        await newscore_admin_db.delete_many({"chat_id": chat_id, "user_id": user_id})
        invalidate_ns_admins_cache(chat_id)
    except Exception as e:
        print(f"[NewsCore] remove admin error: {e}")


async def ns_get_titled_members(chat_id: int) -> list:
    """
    Ambil daftar member non-admin yang SEDANG dipasang tag (Auto Title
    Member) di grup ini dari periode reset sebelumnya.

    Dipakai _apply_auto_title_member() untuk membandingkan dengan daftar
    titel baru — siapa yang TIDAK lagi masuk daftar baru akan dihapus
    tag-nya (setChatMemberTag dengan tag="").
    """
    try:
        return await newscore_titled_db.find({"chat_id": chat_id}).to_list(length=200)
    except Exception as e:
        print(f"[NewsCore] get titled members error: {e}")
        return []


async def ns_set_titled_members(chat_id: int, members: list) -> None:
    """
    Timpa daftar member bertitel grup ini dengan daftar baru.
    `members` adalah list of dict {"chat_id", "user_id", "user_name", "tag"}.
    Dipanggil SETELAH semua setChatMemberTag (pasang baru + hapus lama)
    selesai dieksekusi di akhir periode reset.
    """
    try:
        await newscore_titled_db.delete_many({"chat_id": chat_id})
        if members:
            await newscore_titled_db.insert_many(members)
    except Exception as e:
        print(f"[NewsCore] set titled members error: {e}")


async def ns_remove_score(chat_id: int, user_id: int) -> None:
    """
    Hapus data skor user dari newscore_stats.
    Dipanggil saat member di-adminkan paksa (bukan via NewsCore) agar
    bot tidak mencoba meng-adminkan dia lagi di periode berikutnya.
    Juga bersihkan dari buffer in-memory jika belum di-flush.
    """
    try:
        # Hapus dari buffer in-memory (belum tentu ada di DB)
        _ns_score_buffer.pop((chat_id, user_id), None)
        # Hapus dari DB
        await newscore_stats_db.delete_many({"chat_id": chat_id, "user_id": user_id})
    except Exception as e:
        print(f"[NewsCore] remove_score error: {e}")


# ── Title VIP — daftar member VIP yang sedang dipasang tag ────────────────────
async def vip_get_titled_members(chat_id: int) -> list:
    """
    Ambil daftar member VIP yang SEDANG dipasang tag (Title VIP) di grup ini.

    Dipakai untuk membandingkan dengan daftar VIP terbaru — siapa yang
    TIDAK lagi VIP akan dihapus tag-nya (setChatMemberTag dengan tag="").
    """
    try:
        return await vip_titled_db.find({"chat_id": chat_id}).to_list(length=500)
    except Exception as e:
        print(f"[VIP-Title] get titled members error: {e}")
        return []


async def vip_set_titled_member(chat_id: int, user_id: int, tag: str) -> None:
    """Catat/timpa satu member VIP sebagai sedang bertitel tag tertentu."""
    try:
        await vip_titled_db.update_one(
            {"chat_id": chat_id, "user_id": user_id},
            {"$set": {"chat_id": chat_id, "user_id": user_id, "tag": tag}},
            upsert=True,
        )
    except Exception as e:
        print(f"[VIP-Title] set titled member error: {e}")


async def vip_remove_titled_member(chat_id: int, user_id: int) -> None:
    """Hapus satu member dari daftar bertitel VIP (setelah tag-nya dicopot)."""
    try:
        await vip_titled_db.delete_one({"chat_id": chat_id, "user_id": user_id})
    except Exception as e:
        print(f"[VIP-Title] remove titled member error: {e}")


async def get_vip_user_ids(chat_id: int) -> set:
    """
    Ambil seluruh user_id Member VIP aktif di grup ini (manual /vip ATAU
    bio_vip — keduanya disimpan di free_per_group, lihat core/vip_bio_guard.py
    & plugins/commands/free.py).

    Dipakai untuk MENGECUALIKAN member VIP dari Auto Title Member NewsCore
    (plugins/commands/newscore.py — sama seperti admin NS dikecualikan),
    supaya kuota 5 member per tier tetap terisi penuh oleh member non-VIP.
    """
    try:
        docs = await db["free_per_group"].find({"chat_id": chat_id}, {"user_id": 1}).to_list(length=2000)
        return {d["user_id"] for d in docs if "user_id" in d}
    except Exception as e:
        print(f"[VIP-Title] get_vip_user_ids error: {e}")
        return set()



# ══════════════════════════════════════════════════════════════════════════════
# MENTION MEMBER CACHE
# ══════════════════════════════════════════════════════════════════════════════
#
# Menyimpan hasil get_chat_member per (chat_id, user_id/username) agar
# deteksi external mention tidak perlu hit Telegram API setiap saat.
#
# Schema dokumen:
#   chat_id    : int          — ID grup
#   user_id    : int          — ID user (tidak pernah berubah)
#   username   : str | None   — @username saat di-cache (bisa berubah)
#   is_member  : bool         — True = member grup ini
#   cached_at  : float        — unix timestamp saat data disimpan
#   expires_at : datetime     — TTL 1 minggu, diperbarui tiap ada mention
#
# Index:
#   unik  (chat_id, user_id)   — lookup by user_id
#   biasa (chat_id, username)  — lookup by username string
#   TTL   expires_at           — MongoDB hapus otomatis setelah 1 minggu
#
# username dan user_id keduanya disimpan karena berbeda:
#   - user_id stabil → cocok untuk TEXT_MENTION / tg://user?id=
#   - username bisa berubah → disimpan apa adanya saat di-cache
#   Jika user ganti username, entry lama (username lama) tetap ada sampai
#   TTL habis. Entry baru (username baru) dibuat saat mention berikutnya.
#   Ini aman karena worst case hanya false negative (miss cache → API call),
#   bukan false positive (member asli dikira external).

MENTION_CACHE_TTL_SECS      = 30 * 24 * 3600   # 1 bulan — per-grup member cache
MENTION_GLOBAL_NON_AKUN_TTL = 30 * 24 * 3600   # 30 hari — username tidak exist / akun mati
MENTION_GLOBAL_ENTITY_TTL   =  7 * 24 * 3600   # 7 hari  — channel / grup

_mention_ttl_index_created = False


async def ensure_mention_cache_index() -> None:
    """
    Buat TTL index pada expires_at di mention_member_cache.
    Idempotent — aman dipanggil tiap startup.
    """
    global _mention_ttl_index_created
    if _mention_ttl_index_created:
        return
    if _BACKEND != "mongo":
        _mention_ttl_index_created = True
        return
    try:
        await mention_cache_db.create_index("expires_at", expireAfterSeconds=0)
        await mention_cache_db.create_index([("chat_id", 1), ("user_id", 1)], unique=True)
        await mention_cache_db.create_index([("chat_id", 1), ("username", 1)])
        _mention_ttl_index_created = True
        print("[MentionCache] ✅ TTL index mention_member_cache siap.")
    except Exception as e:
        print(f"[MentionCache] ⚠️  Gagal buat index: {e}")


def _mention_expires_at() -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=MENTION_CACHE_TTL_SECS)


async def mention_cache_get_by_username(chat_id: int, username: str) -> "bool | None":
    """
    Cari di cache berdasarkan username (lowercase, tanpa @).
    Return: True = member, False = bukan member, None = tidak ada di cache.
    """
    try:
        doc = await mention_cache_db.find_one({
            "chat_id": chat_id,
            "username": username.lower(),
        })
        if doc is None:
            return None
        return doc.get("is_member", False)
    except Exception as e:
        print(f"[MentionCache] get_by_username error: {e}")
        return None


async def mention_cache_get_by_uid(chat_id: int, user_id: int) -> "bool | None":
    """
    Cari di cache berdasarkan user_id.
    Return: True = member, False = bukan member, None = tidak ada di cache.
    """
    try:
        doc = await mention_cache_db.find_one({
            "chat_id": chat_id,
            "user_id": user_id,
        })
        if doc is None:
            return None
        return doc.get("is_member", False)
    except Exception as e:
        print(f"[MentionCache] get_by_uid error: {e}")
        return None


async def mention_cache_set(
    chat_id: int,
    user_id: int,
    is_member: bool,
    username: "str | None" = None,
) -> None:
    """
    Simpan / perbarui cache hasil member check.
    TTL diperbarui ke 1 minggu dari sekarang setiap dipanggil.
    username dan user_id keduanya disimpan (bisa berbeda dari waktu ke waktu).
    """
    try:
        now = time.time()
        await mention_cache_db.update_one(
            {"chat_id": chat_id, "user_id": user_id},
            {"$set": {
                "chat_id":    chat_id,
                "user_id":    user_id,
                "username":   username.lower() if username else None,
                "is_member":  is_member,
                "cached_at":  now,
                "expires_at": _mention_expires_at(),
            }},
            upsert=True,
        )
        # Jika punya username, update juga entry by username
        # (untuk kasus user_id belum dikenal tapi username dikenal)
        if username:
            await mention_cache_db.update_one(
                {"chat_id": chat_id, "username": username.lower(), "user_id": {"$ne": user_id}},
                {"$set": {
                    "chat_id":    chat_id,
                    "user_id":    user_id,
                    "username":   username.lower(),
                    "is_member":  is_member,
                    "cached_at":  now,
                    "expires_at": _mention_expires_at(),
                }},
                upsert=False,   # hanya update jika ada dokumen username lama yang user_id-nya beda
            )
    except Exception as e:
        print(f"[MentionCache] set error: {e}")


async def mention_cache_refresh_ttl(chat_id: int, user_id: "int | None" = None, username: "str | None" = None) -> None:
    """
    Perbarui expires_at ke MENTION_CACHE_TTL_SECS lagi tanpa mengubah data lain.
    Dipanggil setiap ada mention dan entry sudah ada di cache (cache hit) —
    baik member MAUPUN bukan member, supaya keduanya tetap "awet" selama
    masih aktif disebut, tidak cuma yang member.

    Bisa dipanggil dengan user_id (paling akurat — entry unik per user_id)
    ATAU username saja (untuk mention @username yang belum di-resolve ke
    user_id, mis. dari bot utama yang belum sempat lewat monitor bot).
    Minimal salah satu harus diisi.
    """
    if user_id is None and not username:
        return
    try:
        query: dict = {"chat_id": chat_id}
        if user_id is not None:
            query["user_id"] = user_id
        else:
            query["username"] = username.lower()
        await mention_cache_db.update_one(
            query,
            {"$set": {"expires_at": _mention_expires_at()}},
        )
    except Exception as e:
        print(f"[MentionCache] refresh_ttl error: {e}")


async def mention_cache_remove_member(chat_id: int, user_id: int) -> None:
    """
    Tandai user sebagai bukan member lagi (misal: keluar/kick dari grup).
    Tidak menghapus dokumen — TTL tetap jalan, tapi is_member = False.
    """
    try:
        await mention_cache_db.update_one(
            {"chat_id": chat_id, "user_id": user_id},
            {"$set": {
                "is_member":  False,
                "cached_at":  time.time(),
                "expires_at": _mention_expires_at(),
            }},
        )
    except Exception as e:
        print(f"[MentionCache] remove_member error: {e}")


async def mention_reset_all() -> dict:
    """
    Hapus TOTAL semua data mention di SELURUH bot — dipakai owner untuk
    memperbaiki data lama yang sempat kena bug salah-vonis "non_akun" untuk
    akun asli, sebelum fix get_chat_member-first ini ada.

    Menghapus 5 koleksi sekaligus:
      - mention_member_cache      (status member per grup, key: chat_id+user_id/username)
      - mention_global_cache      (non_akun/channel/grup, key: username, lintas semua grup)
      - mention_pending_resolve   (antrian username yang sedang menunggu resolusi background)
      - mention_bio_scan_cache    (hasil scan bio: flagged=True/False, key: username, global)
      - mention_bio_scan_pending  (antrian scan bio yang belum dikerjakan)

    Setelah ini, SETIAP @username di SEMUA grup akan di-resolve ULANG dari
    API Telegram dari nol (pakai logika get_chat_member-first yang sudah
    diperbaiki) — bukan dari cache lama yang mungkin masih menyimpan cap
    salah dari sebelum fix.

    Return: dict berisi jumlah dokumen terhapus per koleksi.
    """
    result = {
        "mention_member_cache":     0,
        "mention_global_cache":     0,
        "mention_pending_resolve":  0,
        "mention_bio_scan_cache":   0,
        "mention_bio_scan_pending": 0,
    }
    try:
        r = await mention_cache_db.delete_many({})
        result["mention_member_cache"] = r.deleted_count
    except Exception as e:
        print(f"[MentionCache] reset_all (member_cache) error: {e}")
    try:
        r = await mention_global_db.delete_many({})
        result["mention_global_cache"] = r.deleted_count
    except Exception as e:
        print(f"[MentionCache] reset_all (global_cache) error: {e}")
    try:
        r = await mention_pending_db.delete_many({})
        result["mention_pending_resolve"] = r.deleted_count
    except Exception as e:
        print(f"[MentionCache] reset_all (pending) error: {e}")
    try:
        r = await mention_bio_scan_cache_db.delete_many({})
        result["mention_bio_scan_cache"] = r.deleted_count
    except Exception as e:
        print(f"[MentionCache] reset_all (bio_scan_cache) error: {e}")
    try:
        r = await mention_bio_scan_pending_db.delete_many({})
        result["mention_bio_scan_pending"] = r.deleted_count
    except Exception as e:
        print(f"[MentionCache] reset_all (bio_scan_pending) error: {e}")
    return result


async def mention_cache_flush_group(chat_id: int) -> int:
    """
    Hapus SEMUA entri mention_member_cache untuk satu grup — baik yang
    berstatus is_member=True (member) MAUPUN is_member=False (non-member).
    Dipakai perintah owner/admin untuk "bersih paksa" cache satu grup,
    supaya setiap @username yang di-tag lagi setelah ini di-resolve ULANG
    dari API (bukan dari cache lama, entah itu cache member atau
    non-member — dua-duanya bisa basi seiring waktu).

    Hanya menghapus data grup ITU (chat_id) — tidak menyentuh grup lain,
    dan tidak menyentuh mention_global_cache (non_akun/channel/grup lintas
    grup, TTL-nya sendiri sudah wajar & tidak butuh flush manual).

    Return: jumlah dokumen yang terhapus.
    """
    try:
        result = await mention_cache_db.delete_many({"chat_id": chat_id})
        return result.deleted_count
    except Exception as e:
        print(f"[MentionCache] flush_group error: {e}")
        return 0


# ═══════════════════════════════════════════════════════════════════════════════
#  MENTION GLOBAL CACHE — non-akun, channel, grup (lintas semua grup)
# ═══════════════════════════════════════════════════════════════════════════════
#
# Koleksi ini menyimpan 3 jenis entri:
#   kind="non_akun"  — username tidak exist / akun mati / bukan entitas valid
#   kind="channel"   — username milik channel Telegram
#   kind="grup"      — username milik grup / supergroup Telegram
#
# Setelah tersimpan, cek per-grup (API call) tidak perlu diulang untuk username
# yang sama di grup manapun — langsung keputusan dari sini.
#
# TTL:
#   non_akun → 30 hari (username mati lama tidak aktif)
#   channel/grup → 7 hari (bisa berubah pemilik atau jenis)
# ───────────────────────────────────────────────────────────────────────────────

_mention_global_index_created = False


async def ensure_mention_global_index() -> None:
    """Index TTL dan username unik untuk mention_global_cache. Idempotent."""
    global _mention_global_index_created
    if _mention_global_index_created:
        return
    if _BACKEND != "mongo":
        _mention_global_index_created = True
        return
    try:
        await mention_global_db.create_index("expires_at", expireAfterSeconds=0)
        await mention_global_db.create_index("username", unique=True)
        _mention_global_index_created = True
        print("[MentionGlobal] ✅ Index mention_global_cache siap.")
    except Exception as e:
        print(f"[MentionGlobal] ⚠️  Gagal buat index: {e}")


async def mention_global_get(username: str) -> "dict | None":
    """
    Cari username di global cache.
    Return doc {"kind": "non_akun"|"channel"|"grup", ...} atau None jika tidak ada.
    """
    try:
        return await mention_global_db.find_one({"username": username.lower()})
    except Exception:
        return None


async def mention_global_set(username: str, kind: str) -> None:
    """
    Simpan username ke global cache.
    kind: "non_akun" | "channel" | "grup"
    TTL disesuaikan per kind.
    """
    if kind == "non_akun":
        ttl_secs = MENTION_GLOBAL_NON_AKUN_TTL
    else:
        ttl_secs = MENTION_GLOBAL_ENTITY_TTL

    expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_secs)
    try:
        await mention_global_db.update_one(
            {"username": username.lower()},
            {"$set": {
                "username":   username.lower(),
                "kind":       kind,
                "cached_at":  time.time(),
                "expires_at": expires_at,
            }},
            upsert=True,
        )
    except Exception as e:
        print(f"[MentionGlobal] set error: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
#  MENTION PENDING RESOLVE — "database khusus" username @mention yang cache-
#  nya MISS di gate mention (cache-only, satu-satunya jalur cek mention —
#  lihat plugins/filters/antispam.py::_is_external_mention_cache_only &
#  core/antispam_queue.py::_gate_mention_cache). Setiap dokumen menyimpan
#  daftar `cids` (semua chat_id grup yang pernah menemui username ini),
#  supaya mention_pending_resolve_loop bisa mengisi mention_member_cache
#  PER GRUP tsb, bukan cuma menandai username-nya valid secara global.
#  Dibersihkan (dihapus dari koleksi ini) oleh mention_pending_resolve_loop
#  setelah berhasil diresolusi.
# ═══════════════════════════════════════════════════════════════════════════════

_mention_pending_index_created = False


async def ensure_mention_pending_index() -> None:
    """Index unik `username` untuk mention_pending_resolve. Idempotent."""
    global _mention_pending_index_created
    if _mention_pending_index_created:
        return
    if _BACKEND != "mongo":
        _mention_pending_index_created = True
        return
    try:
        await mention_pending_db.create_index("username", unique=True)
        _mention_pending_index_created = True
        print("[MentionPending] ✅ Index mention_pending_resolve siap.")
    except Exception as e:
        print(f"[MentionPending] ⚠️  Gagal buat index: {e}")


async def mention_pending_add(username: str, cid: int | None = None) -> None:
    """
    Catat username ke antrian resolusi tunda. Upsert by `username` →
    otomatis TIDAK ADA DUPLIKAT walau dipanggil berkali-kali dari banyak
    grup/lorong berbeda untuk username yang sama persis. `cid` (kalau ada)
    ditambahkan ke set `cids` — dipakai nanti oleh mention_pending_resolve_loop
    untuk mengisi mention_member_cache PER GRUP yang pernah menemui username
    ini (bukan cuma jejak grup pertama), supaya @mention yang sama di grup
    manapun dalam daftar itu ikut "selamat" begitu resolusi selesai.

    `retry_count` di-track untuk mendeteksi username yang tidak pernah bisa
    di-resolve (mis. bot pembantu semua offline) — lihat mention_pending_resolve_loop.
    """
    username = username.lower()
    try:
        update = {
            "$setOnInsert": {
                "username":    username,
                "created_at":  time.time(),
                "retry_count": 0,
            },
            "$set": {"last_seen_at": time.time()},
        }
        if cid is not None:
            update["$addToSet"] = {"cids": cid}
        await mention_pending_db.update_one(
            {"username": username},
            update,
            upsert=True,
        )
    except Exception as e:
        print(f"[MentionPending] add error: {e}")


async def mention_pending_get_batch(limit: int = 20) -> "list[dict]":
    """
    Ambil sejumlah entri tertua dari antrian tunda (FIFO by created_at).
    Return list of {"username", "cids", "retry_count"} — `cids` dipakai
    untuk resolusi status keanggotaan PER GRUP (lihat mention_pending_resolve_loop).
    `retry_count` dipakai untuk deteksi username yang tidak pernah bisa di-resolve.
    """
    try:
        cursor = mention_pending_db.find({}).sort("created_at", 1).limit(limit)
        return [
            {
                "username":    doc["username"],
                "cids":        doc.get("cids") or [],
                "retry_count": doc.get("retry_count", 0),
            }
            async for doc in cursor
        ]
    except Exception as e:
        print(f"[MentionPending] get_batch error: {e}")
        return []


async def mention_pending_remove(username: str) -> None:
    """Hapus username dari antrian tunda — dipanggil setelah berhasil diresolusi."""
    try:
        await mention_pending_db.delete_one({"username": username.lower()})
    except Exception as e:
        print(f"[MentionPending] remove error: {e}")


def _mention_member_status_is_member(member) -> bool:
    """
    Duplikat sengaja dari plugins/filters/antispam.py::_resolve_is_member
    (tidak diimpor dari sana supaya tidak bikin circular import — modul itu
    sudah mengimpor banyak hal dari database.py). Lihat penjelasan lengkap
    di fungsi aslinya soal kenapa status LEFT/BANNED/RESTRICTED dicek
    eksplisit, bukan cuma "member is not None".

    CATATAN: sejak mention_pending_resolve_loop tidak lagi fallback ke bot
    utama untuk cek keanggotaan per grup (WAJIB lewat bot pembantu/
    MonitorInstance — lihat check_member_via_monitor di
    security_os/monitor_bot_reference.py), helper ini tidak lagi dipanggil
    di jalur otomatis. Dibiarkan (bukan dihapus) untuk dipakai ulang kalau
    suatu saat ada jalur lain yang butuh logika status member yang sama.
    """
    if member is None:
        return False
    from pyrogram.enums import ChatMemberStatus
    status = member.status
    if status in (ChatMemberStatus.LEFT, ChatMemberStatus.BANNED):
        return False
    if status == ChatMemberStatus.RESTRICTED:
        return bool(getattr(member, "is_member", True))
    return True


async def mention_pending_resolve_loop(client) -> None:
    """
    Background loop — resolusi PELAN-PELAN username di mention_pending_resolve
    (lihat komentar di deklarasi mention_pending_db), TERUS-MENERUS dengan
    jeda santai antar panggilan API (_API_GAP), TIDAK menunggu antrian
    antispam kosong.

    Karena gate mention (cache-only) SEKARANG SATU-SATUNYA jalur cek mention
    (tidak ada lagi jalur sequential API sesudahnya), loop inilah yang
    bertanggung jawab PENUH mengisi cache:
      • non_akun / channel / grup → mention_global_cache (mention_global_set)
      • akun biasa (PRIVATE)      → mention_member_cache PER GRUP
        (mention_cache_set), untuk SETIAP chat_id di `cids` — supaya
        @mention yang sama di grup manapun dalam daftar itu langsung
        "ketahuan" oleh gate mention di kemunculan berikutnya.

    Loop ini jalan tiap _CHECK_INTERVAL detik apapun kondisi antrian
    antispam, dengan jeda _API_GAP antar 1 panggilan API sebagai
    pengereman satu-satunya (supaya tidak flood Telegram).

    ENV:
      MENTION_PENDING_CHECK_INTERVAL default 20   — jeda antar siklus batch (detik)
      MENTION_PENDING_BATCH_SIZE     default 15   — max username diproses per batch
      MENTION_PENDING_API_GAP        default 1.5  — jeda antar 1 panggilan API resolusi (detik)
    """
    _CHECK_INTERVAL = float(os.environ.get("MENTION_PENDING_CHECK_INTERVAL", 20.0))
    _BATCH_SIZE     = int(os.environ.get("MENTION_PENDING_BATCH_SIZE", 15))
    _API_GAP        = float(os.environ.get("MENTION_PENDING_API_GAP", 1.5))

    for _ in range(60):
        if getattr(client, "is_connected", False):
            break
        await asyncio.sleep(1.0)

    print("[MentionPending] ✅ Resolver background siap (jalan terus-menerus, jeda santai per panggilan API).", flush=True)

    while True:
        await asyncio.sleep(_CHECK_INTERVAL)
        try:
            if not getattr(client, "is_connected", False):
                continue

            batch = await mention_pending_get_batch(_BATCH_SIZE)
            if not batch:
                continue

            from pyrogram.errors import FloodWait

            for item in batch:
                uname = item["username"]
                cids  = item.get("cids") or []

                await wait_global_flood_backoff()

                # Skip kalau duplikat cache — kemungkinan sudah keburu
                # diresolusi lewat siklus sebelumnya.
                already = await mention_global_get(uname)
                if already is not None:
                    await mention_pending_remove(uname)
                    continue

                try:
                    # ── 1. Klasifikasi channel/grup/invalid — lewat BENGKEL
                    #    (workshop_pool.resolve_chat_type), BUKAN bot utama.
                    #    Ini pertanyaan GLOBAL ("@uname itu apa?"), tidak
                    #    butuh bot jadi member grup manapun — pas dijawab
                    #    bengkel (sama seperti resolve kandidat link di bio,
                    #    lihat core/mention_bio_scan.py).
                    from core.workshop_pool import workshop_pool

                    jenis = await workshop_pool.resolve_chat_type(uname) if workshop_pool.size > 0 else None

                    if jenis == "channel":
                        await mention_global_set(uname, "channel")
                        await mention_pending_remove(uname)
                        continue
                    elif jenis == "group":
                        await mention_global_set(uname, "grup")
                        await mention_pending_remove(uname)
                        continue
                    elif jenis == "invalid":
                        # SAFETY: Sebelum menyimpan "non_akun" ke global cache
                        # (berlaku permanen selama TTL), cek dulu apakah username
                        # ini sudah PERNAH dikonfirmasi member di salah satu grup
                        # (entry is_member=True di mention_member_cache). Kalau ada:
                        #   → JANGAN simpan "non_akun". Kemungkinan bengkel sedang
                        #     transient error (network timeout, FloodWait worker lain
                        #     yang terlewat), atau username baru saja di-rename
                        #     pemiliknya padahal member cache lama masih valid.
                        #
                        # KENAPA INI PENTING: menyimpan "non_akun" di sini padahal
                        # user sebetulnya member menyebabkan SEMUA tag berikutnya
                        # (hingga TTL habis) dianggap "Username tidak valid / akun
                        # tidak ditemukan" — persis gejala yang dilaporkan: tag ke-2
                        # dan seterusnya dianggap "invalid" walau bio grup sudah
                        # pernah terdeteksi 1x di console log.
                        _is_confirmed_member = False
                        for _chk_cid in cids:
                            try:
                                _m = await mention_cache_get_by_username(_chk_cid, uname)
                                if _m is True:
                                    _is_confirmed_member = True
                                    break
                            except Exception:
                                pass
                        if not _is_confirmed_member:
                            await mention_global_set(uname, "non_akun")
                        else:
                            print(
                                f"[MentionPending] ⚠️  resolve_chat_type(@{uname}) → 'invalid' "
                                f"tapi user sudah dikonfirmasi member di salah satu grup — "
                                f"SKIP simpan 'non_akun' (kemungkinan transient error bengkel)."
                            )
                        await mention_pending_remove(uname)
                        continue
                    elif jenis is None and workshop_pool.size > 0:
                        # Bengkel ADA tapi semua worker gagal/timeout kali ini
                        # (transient) — jangan fallback ke bot utama, biarkan
                        # di antrian, dicoba lagi siklus berikutnya.
                        continue
                    # jenis == "private" (akun biasa), ATAU tidak ada
                    # TOKEN_BACKUP* di-set sama sekali (workshop_pool.size
                    # == 0, fallback ke jalur lama bot utama di bawah) →
                    # lanjut ke langkah 2.

                    # ── 2. Status keanggotaan PER GRUP — HANYA lewat BOT
                    #    PEMBANTU (MonitorInstance khusus grup itu,
                    #    check_member_via_monitor). TIDAK ADA fallback ke
                    #    bot utama — toggle "mention_batasi_akun" (dan
                    #    saudara-saudaranya di panel Bio Cek & Mention)
                    #    memang TIDAK BISA dinyalakan sebelum bot pemantau
                    #    grup itu terpasang & siap (lihat gating di
                    #    plugins/ui/handlers_dm.py::cb_toggle), jadi kalau
                    #    bot pembantu grup ini justru lagi tidak aktif/error
                    #    saat ini, grup ini SKIP dulu (tidak diputuskan apa-
                    #    apa) — dicoba lagi siklus berikutnya, BUKAN
                    #    dikerjakan bot utama.
                    from monitor_bot_reference import check_member_via_monitor

                    resolved_as_user = False
                    # Catat grup mana saja yang bot pembantunya menjawab None
                    # (mati/tidak aktif) di SIKLUS INI — TIDAK ada tembakan API
                    # tambahan, cuma menampung hasil check_member_via_monitor
                    # yang memang sudah dipanggil di bawah untuk keperluan
                    # resolusi mention itu sendiri. Dipakai di titik ambang
                    # (threshold) di bawah untuk memutuskan grup mana yang
                    # perlu dipaksa OFF fitur "Bio Cek & Mention"-nya.
                    _dead_monitor_cids: list[int] = []
                    for g_cid in cids:
                        await wait_global_flood_backoff()

                        is_member = await check_member_via_monitor(g_cid, uname)
                        if is_member is None:
                            # Bot pembantu grup ini tidak aktif/error — skip
                            # grup ini, JANGAN dialihkan ke bot utama.
                            _dead_monitor_cids.append(g_cid)
                            continue

                        # Bot pembantu grup ini berhasil jawab (cache
                        # member/user_id sudah otomatis ditulis di dalam
                        # check_is_member) — tidak perlu apa-apa lagi di
                        # sini selain titip scan bio kalau member.
                        resolved_as_user = True
                        if is_member:
                            try:
                                _uid_for_bio = await mention_cache_get_user_id_by_username(g_cid, uname)
                                _g_cfg = await get_config(g_cid)
                                if _g_cfg.get("mention_batasi_akun", False):
                                    _bio_cached = await mention_bio_scan_get(uname)
                                    if _bio_cached is None:
                                        await mention_bio_scan_pending_add(uname, _uid_for_bio or 0, g_cid)
                            except Exception as _e_bioscan:
                                print(f"[MentionPending] gagal titip scan bio {uname}: {_e_bioscan}")
                        await asyncio.sleep(_API_GAP)

                    if resolved_as_user:
                        await mention_pending_remove(uname)
                        continue

                    # BUG FIX — Infinite pending loop prevention:
                    # Semua bot pembantu offline untuk semua cids → username
                    # tidak bisa di-resolve status keanggotaannya. Sebelumnya
                    # ini menyebabkan username TETAP di antrian selamanya —
                    # setiap mention baru = "cache_miss" = langsung dihapus,
                    # tidak peduli apakah user itu member sah atau bukan.
                    #
                    # Fix: track retry_count. Setelah threshold tercapai,
                    # hapus dari pending supaya tidak loop selamanya. Username
                    # akan ditambahkan kembali ke pending saat mention berikutnya
                    # (jika cache masih miss) — siklus jadi bounded bukan infinite.
                    # Threshold default 8: dengan interval 20 detik, artinya
                    # ~2,5 menit mencoba sebelum menyerah sementara.
                    _PENDING_RETRY_MAX = int(os.environ.get("MENTION_PENDING_RETRY_MAX", 8))
                    _item_retry = item.get("retry_count", 0)
                    if _item_retry >= _PENDING_RETRY_MAX:
                        print(
                            f"[MentionPending] ⚠️  @{uname} gagal resolve "
                            f"{_item_retry}x (semua bot pembantu offline) — "
                            f"hapus dari pending sementara. Akan retry saat mention berikutnya."
                        )
                        await mention_pending_remove(uname)

                        # ── Paksa OFF "Bio Cek & Mention" REAKTIF — TANPA
                        # tembakan API tambahan sama sekali (baik dari bot
                        # utama maupun bot pemantau). Sinyal "bot pembantu
                        # mati" (_dead_monitor_cids) sudah didapat GRATIS
                        # sebagai efek samping check_member_via_monitor() di
                        # atas (yang sendiri dikonfirmasi via getMe() oleh bot
                        # pemantau ITU SENDIRI, bukan bot utama — lihat
                        # MonitorInstance._handle_dead_signal() di
                        # monitor_bot_reference.py). Ambang 8x retry di atas
                        # yang jadi debounce-nya (persis kondisi log ini),
                        # supaya tidak paksa OFF gara-gara 1 blip sesaat.
                        for _dead_cid in _dead_monitor_cids:
                            try:
                                await force_disable_bio_mention_features(_dead_cid)
                            except Exception as _e_force:
                                print(f"[MentionPending] gagal paksa OFF Bio Cek & Mention grup {_dead_cid}: {_e_force}")
                    else:
                        # Masih di bawah threshold → increment counter, coba lagi siklus berikutnya.
                        try:
                            await mention_pending_db.update_one(
                                {"username": uname},
                                {"$inc": {"retry_count": 1}},
                            )
                        except Exception:
                            pass
                    continue

                except FloodWait as fw:
                    set_global_flood_backoff(fw.value)
                    break  # hentikan batch ini, lanjut siklus check_interval berikutnya
                except Exception as e:
                    print(f"[MentionPending] ⚠️  Gagal resolve @{uname}: {e}")
                    # Tidak dihapus dari pending — dicoba lagi di siklus berikutnya

                await asyncio.sleep(_API_GAP)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[MentionPending] ❌ Resolver loop error: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
#  MENTION BIO SCAN — ekstensi Anti Mention: bahkan kalau @username MEMANG
#  member grup, kalau bio profilnya mempromosikan grup LAIN (ada link/
#  @username lain yang resolve-nya GRUP/SUPERGROUP), mention ke dia tetap
#  diperlakukan seperti mention non-member DI GRUP ITU. Bagian dari toggle
#  "Batasi Tag Akun Promosi" (cfg["mention_batasi_akun"]) — satu toggle yang
#  sama juga menangani akun non-valid/non-member, lihat
#  plugins/ui/pages.py::page_bio_panel (panel Bio Cek & Mention).
#
#  Orkestrasi (extract kandidat dari bio, panggil workshop_pool untuk
#  resolve_chat_type) ada di core/mention_bio_scan.py — di sini HANYA
#  collection + CRUD mentah, mengikuti pola yang sama dengan
#  mention_pending_resolve/mention_member_cache di atas.
#
#  mention_bio_scan_cache: GLOBAL (bukan per-grup) — bio seseorang sama di
#  manapun dia di-mention, jadi hasil scan dipakai bersama lintas grup yang
#  toggle-nya ON, tidak perlu discan ulang per grup. TTL 7 hari (bio bisa
#  berubah — link grup bisa dihapus/diganti admin akun tsb).
# ═══════════════════════════════════════════════════════════════════════════════

mention_bio_scan_cache_db   = db["mention_bio_scan_cache"]     # GLOBAL, key: username
mention_bio_scan_pending_db = db["mention_bio_scan_pending"]   # antrian scan tunda, key: username

MENTION_BIO_SCAN_TTL_SECS = 7 * 24 * 3600   # 7 hari — sesuai keputusan owner

_mention_bio_scan_index_created = False


async def ensure_mention_bio_scan_index() -> None:
    """TTL index di mention_bio_scan_cache + index unik di mention_bio_scan_pending.
    Idempotent — aman dipanggil tiap startup (lihat main.py)."""
    global _mention_bio_scan_index_created
    if _mention_bio_scan_index_created:
        return
    if _BACKEND != "mongo":
        _mention_bio_scan_index_created = True
        return
    try:
        await mention_bio_scan_cache_db.create_index("expires_at", expireAfterSeconds=0)
        await mention_bio_scan_cache_db.create_index("username", unique=True)
        await mention_bio_scan_pending_db.create_index("username", unique=True)
        _mention_bio_scan_index_created = True
        print("[MentionBioScan] ✅ Index mention_bio_scan siap.")
    except Exception as e:
        print(f"[MentionBioScan] ⚠️  Gagal buat index: {e}")


async def mention_cache_get_user_id_by_username(chat_id: int, username: str) -> "int | None":
    """
    Ambil user_id yang tersimpan bareng entry mention_member_cache ini
    (kalau ada). Utility umum, tersedia untuk pemanggil manapun yang
    butuh — CATATAN: mention_bio_scan TIDAK lagi wajib pakai ini (scan
    bio sekarang resolve langsung via USERNAME lewat
    workshop_pool.fetch_full_user_by_username, tidak butuh user_id numerik
    sama sekali — lihat core/mention_bio_scan.py untuk alasannya).
    """
    try:
        doc = await mention_cache_db.find_one({"chat_id": chat_id, "username": username.lower()})
        return doc.get("user_id") if doc else None
    except Exception as e:
        print(f"[MentionBioScan] get_user_id error: {e}")
        return None


async def mention_bio_scan_get(username: str) -> "bool | None":
    """
    Cek cache GLOBAL hasil scan bio untuk username ini.
    Return: True = bio-nya kedapatan promosi grup lain (flagged), False =
    sudah discan dan aman, None = belum pernah discan sama sekali (cache miss).
    """
    try:
        doc = await mention_bio_scan_cache_db.find_one({"username": username.lower()})
        if doc is None:
            return None
        return bool(doc.get("flagged", False))
    except Exception as e:
        print(f"[MentionBioScan] get error: {e}")
        return None


async def mention_bio_scan_set(username: str, user_id: int, flagged: bool, trigger: str = "") -> None:
    """Simpan hasil scan bio (GLOBAL, per-username) — TTL 7 hari."""
    try:
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=MENTION_BIO_SCAN_TTL_SECS)
        await mention_bio_scan_cache_db.update_one(
            {"username": username.lower()},
            {"$set": {
                "username":    username.lower(),
                "user_id":     user_id,
                "flagged":     flagged,
                "trigger":     trigger[:100],
                "checked_at":  time.time(),
                "expires_at":  expires_at,
            }},
            upsert=True,
        )
    except Exception as e:
        print(f"[MentionBioScan] set error: {e}")


async def mention_bio_scan_pending_add(username: str, user_id: int, cid: int) -> None:
    """
    Titip job scan bio ke antrian tunda. Upsert by username — otomatis
    tidak ada duplikat walau banyak grup menemui username yang sama.
    `cid` (opsional secara konsep, tapi selalu ada di jalur pemanggilan
    saat ini) ditambahkan ke `cids` — dipakai murni untuk audit/log, hasil
    scan sendiri GLOBAL (tidak per-grup).
    """
    username = username.lower()
    try:
        await mention_bio_scan_pending_db.update_one(
            {"username": username},
            {
                "$setOnInsert": {"username": username, "created_at": time.time()},
                "$set":         {"user_id": user_id, "last_seen_at": time.time()},
                "$addToSet":    {"cids": cid},
            },
            upsert=True,
        )
    except Exception as e:
        print(f"[MentionBioScan] pending_add error: {e}")


async def mention_bio_scan_pending_get_batch(limit: int = 10) -> "list[dict]":
    """Ambil sejumlah entri tertua dari antrian scan tunda (FIFO by created_at).
    CATATAN: user_id BOLEH 0/kosong (cuma metadata opsional) — kalau kosong,
    _scan_one() tidak akan bisa lewat bot pembantu (butuh user_id numerik
    per grup, lihat core/mention_bio_scan.py), jadi username adalah
    satu-satunya field yang benar-benar wajib ada di sini.
    `cids` disertakan — dipakai _scan_one() untuk memilih grup mana yang
    bot pembantu (MonitorInstance)-nya dipakai mengambil bio."""
    try:
        cursor = mention_bio_scan_pending_db.find({}).sort("created_at", 1).limit(limit)
        return [
            {
                "username": doc["username"],
                "user_id":  doc.get("user_id") or 0,
                "cids":     doc.get("cids") or [],
            }
            async for doc in cursor
            if doc.get("username")
        ]
    except Exception as e:
        print(f"[MentionBioScan] pending_get_batch error: {e}")
        return []


async def mention_bio_scan_pending_remove(username: str) -> None:
    """Hapus username dari antrian scan tunda — dipanggil setelah selesai discan."""
    try:
        await mention_bio_scan_pending_db.delete_one({"username": username.lower()})
    except Exception as e:
        print(f"[MentionBioScan] pending_remove error: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
#  VC BIO LINK CLASSIFICATION — Inspeksi Bio Link Security OS (userbot utama
#  & Custom Userbot). Orkestrasi (extract kandidat, panggil resolve_chat_type)
#  ada di core/vc_bio_link_scan.py & security_os/video_call.py — di sini HANYA
#  collection + CRUD mentah.
#
#  BEDA dari mention_bio_scan_cache di atas: di-key per KANDIDAT ITU SENDIRI
#  (bukan per profil yang bio-nya dicek) — pertanyaan "apakah @xyz sebuah
#  grup" tidak bergantung pada bio siapa munculnya, jadi satu hasil
#  klasifikasi dipakai ulang untuk SEMUA profil yang menyebut @xyz yang sama.
#  GLOBAL (bukan per-grup), TTL 7 hari (link bisa berubah jenis/dihapus).
# ═══════════════════════════════════════════════════════════════════════════════

vc_bio_link_cache_db   = db["vc_bio_link_cache"]     # GLOBAL, key: candidate
vc_bio_link_pending_db = db["vc_bio_link_pending"]   # antrian klasifikasi tunda, key: candidate

VC_BIO_LINK_TTL_SECS = 7 * 24 * 3600   # 7 hari — sama seperti mention_bio_scan

_vc_bio_link_index_created = False


async def ensure_vc_bio_link_index() -> None:
    """TTL index di vc_bio_link_cache + index unik di vc_bio_link_pending.
    Idempotent — aman dipanggil tiap startup (lihat main.py)."""
    global _vc_bio_link_index_created
    if _vc_bio_link_index_created:
        return
    if _BACKEND != "mongo":
        _vc_bio_link_index_created = True
        return
    try:
        await vc_bio_link_cache_db.create_index("expires_at", expireAfterSeconds=0)
        await vc_bio_link_cache_db.create_index("candidate", unique=True)
        await vc_bio_link_pending_db.create_index("candidate", unique=True)
        _vc_bio_link_index_created = True
        print("[VCBioLink] ✅ Index vc_bio_link siap.")
    except Exception as e:
        print(f"[VCBioLink] ⚠️  Gagal buat index: {e}")


async def vc_bio_link_get(candidate: str) -> "str | None":
    """
    Cek cache GLOBAL jenis kandidat ini.
    Return "group"/"channel"/"private"/"invalid" (lihat
    workshop_pool.resolve_chat_type utk arti masing-masing), atau None kalau
    belum pernah diklasifikasi (cache miss — BELUM TERVERIFIKASI, pemanggil
    HARUS menganggapnya aman untuk sekarang, bukan otomatis pelanggaran).
    """
    try:
        doc = await vc_bio_link_cache_db.find_one({"candidate": candidate.lower()})
        return doc.get("jenis") if doc else None
    except Exception as e:
        print(f"[VCBioLink] get error: {e}")
        return None


async def vc_bio_link_set(candidate: str, jenis: "str | None") -> None:
    """Simpan hasil klasifikasi kandidat ini (GLOBAL) — TTL 7 hari."""
    try:
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=VC_BIO_LINK_TTL_SECS)
        await vc_bio_link_cache_db.update_one(
            {"candidate": candidate.lower()},
            {"$set": {
                "candidate":   candidate.lower(),
                "jenis":       jenis,
                "checked_at":  time.time(),
                "expires_at":  expires_at,
            }},
            upsert=True,
        )
    except Exception as e:
        print(f"[VCBioLink] set error: {e}")


async def vc_bio_link_pending_add(candidate: str) -> None:
    """Titip kandidat ke antrian klasifikasi tunda. Upsert by candidate —
    otomatis tidak ada duplikat walau banyak profil menyebut kandidat sama."""
    candidate = candidate.lower()
    try:
        await vc_bio_link_pending_db.update_one(
            {"candidate": candidate},
            {
                "$setOnInsert": {"candidate": candidate, "created_at": time.time()},
                "$set":         {"last_seen_at": time.time()},
            },
            upsert=True,
        )
    except Exception as e:
        print(f"[VCBioLink] pending_add error: {e}")


async def vc_bio_link_pending_get_batch(limit: int = 10) -> "list[dict]":
    """Ambil sejumlah entri tertua dari antrian klasifikasi tunda (FIFO)."""
    try:
        cursor = vc_bio_link_pending_db.find({}).sort("created_at", 1).limit(limit)
        return [{"candidate": doc["candidate"]} async for doc in cursor if doc.get("candidate")]
    except Exception as e:
        print(f"[VCBioLink] pending_get_batch error: {e}")
        return []


async def vc_bio_link_pending_remove(candidate: str) -> None:
    """Hapus kandidat dari antrian klasifikasi tunda — dipanggil setelah selesai diklasifikasi."""
    try:
        await vc_bio_link_pending_db.delete_one({"candidate": candidate.lower()})
    except Exception as e:
        print(f"[VCBioLink] pending_remove error: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
#  MENTION WHITELIST — per grup, username raw
# ═══════════════════════════════════════════════════════════════════════════════

async def mention_wl_get(chat_id: int) -> list[str]:
    """Ambil semua username whitelist untuk grup ini (lowercase, tanpa @).

    Username yang baru saja ditekan hapus (masih menunggu $pull ke DB via
    panel_write_queue) disaring dulu di sini — supaya panel DAN pengecekan
    antispam sama-sama langsung menganggapnya sudah tidak di-whitelist,
    tanpa menunggu penulisan ke DB benar-benar selesai.
    """
    try:
        doc = await mention_wl_db.find_one({"chat_id": chat_id})
        usernames = doc.get("usernames", []) if doc else []
        return [u for u in usernames if not is_pending_delete(chat_id, "mention_wl", u)]
    except Exception:
        return []


async def mention_wl_add(chat_id: int, username: str) -> bool:
    """
    Tambah username ke whitelist grup.
    Return True jika berhasil ditambah, False jika sudah ada.
    """
    uname = username.lower().lstrip("@")
    try:
        existing = await mention_wl_db.find_one({"chat_id": chat_id})
        if existing and uname in existing.get("usernames", []):
            return False
        await mention_wl_db.update_one(
            {"chat_id": chat_id},
            {"$addToSet": {"usernames": uname}},
            upsert=True,
        )
        return True
    except Exception as e:
        print(f"[MentionWL] add error: {e}")
        return False


async def mention_wl_remove(chat_id: int, username: str) -> bool:
    """
    Hapus username dari whitelist grup.
    Return True jika berhasil dihapus, False jika tidak ada.
    """
    uname = username.lower().lstrip("@")
    try:
        existing = await mention_wl_db.find_one({"chat_id": chat_id})
        if not existing or uname not in existing.get("usernames", []):
            return False
        await mention_wl_db.update_one(
            {"chat_id": chat_id},
            {"$pull": {"usernames": uname}},
        )
        return True
    except Exception as e:
        print(f"[MentionWL] remove error: {e}")
        return False
