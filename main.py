"""
main.py — Entry Point Bot Antispam + Nexus AI
Jalankan: python main.py

CATATAN MIGRASI: file ini sebelumnya bernama antigcast.py — di-rename ke
main.py supaya nama entry point tidak ambigu dengan fitur "Anti-GCast
Global" (VIOLATION_GCAST_GLOBAL, command /antigcast, plugins/commands/
antigcast_group.py) yang SAMA SEKALI TIDAK berhubungan — itu cuma
kebetulan nama serupa. Isi & struktur file TIDAK berubah, cuma nama file
+ semua komentar/docstring lintas-file yang menyebut "antigcast.py".

Sistem yang berjalan:
  [REFACTOR] plugins/filters/    → antispam, bio, cas  (group filter)
  [REFACTOR] plugins/commands/   → settings, regex, free, log, antigcast_group
  [REFACTOR] plugins/ui/         → DM panel interaktif (pages, handlers_dm, handlers_fsm)
  [NEXUS]    plugins/nexus/      → nexus_group.py, nexus_handlers.py
             core/               → engine.py (komputasi AI)

Database (otomatis dipilih saat startup):
  1. MongoDB  — jika MONGO_URL ada di .env dan bisa tersambung
  2. SQLite   — fallback ke penyimpanan internal HP (Termux)
"""

import os
import sys
import time
import socket
import asyncio
import threading
from pathlib import Path as _Path

# ── Custom DNS global (opsional, via .env) ────────────────────────────────────
# Patch socket.getaddrinfo di level paling dasar — otomatis kepakai oleh SEMUA
# koneksi yang lewat asyncio/socket standar: Pyrogram (Telegram), httpx
# (Gemini/Groq/OpenRouter), dst. Tidak perlu ubah tiap file klien satu-satu.
# TIDAK menyentuh resolver Mongo SRV (dns.resolver di database.py) — itu jalur
# terpisah (dnspython dipakai langsung oleh pymongo untuk mongodb+srv://).
#
# Toggle: CUSTOM_DNS_ENABLED=1 (default nyala) / 0 (matikan, balik ke DNS
# default jaringan HP/Termux sepenuhnya).
# Server: CUSTOM_DNS_SERVERS=1.1.1.1,1.0.0.1 (default Cloudflare, dipisah koma).
_CUSTOM_DNS_ENABLED = os.environ.get("CUSTOM_DNS_ENABLED", "1").strip().lower() in ("1", "true", "yes")
_CUSTOM_DNS_SERVERS = [s.strip() for s in os.environ.get("CUSTOM_DNS_SERVERS", "1.1.1.1,1.0.0.1").split(",") if s.strip()]
_CUSTOM_DNS_CACHE_TTL_SECS = 300  # cache hasil resolve per host, biar gak query DNS tiap konek

if _CUSTOM_DNS_ENABLED:
    import dns.resolver as _dns_resolver_mod

    _custom_dns_resolver = _dns_resolver_mod.Resolver(configure=False)
    _custom_dns_resolver.nameservers = _CUSTOM_DNS_SERVERS
    _custom_dns_cache: dict[str, tuple[str, float]] = {}
    _original_getaddrinfo = socket.getaddrinfo

    def _custom_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        # PENGAMAN TOTAL: apapun yang terjadi di dalam (bytes vs str, tipe
        # aneh dari httpx/anyio, dns.resolver internal error, dll), fungsi ini
        # TIDAK BOLEH pernah bikin exception lolos ke caller (Pyrogram/httpx/
        # motor). Kalau ada apapun yang gak terduga → langsung fallback ke
        # resolver asli (DNS default HP) memakai argumen ASLI yang diterima.
        try:
            return _resolve_custom(host, port, family, type, proto, flags)
        except Exception:
            return _original_getaddrinfo(host, port, family, type, proto, flags)

    def _resolve_custom(host, port, family, type, proto, flags):
        # Host kosong / famili v6 eksplisit → langsung pakai resolver asli.
        if not host or family == socket.AF_INET6:
            return _original_getaddrinfo(host, port, family, type, proto, flags)

        # Sebagian caller (httpx/anyio) ngasih host sebagai bytes/bytearray,
        # bukan str — inet_aton/dns.resolver butuh str. Normalisasi dulu.
        host_str = host
        if isinstance(host, (bytes, bytearray)):
            try:
                host_str = bytes(host).decode("idna")
            except Exception:
                host_str = bytes(host).decode("ascii", errors="ignore")
        elif not isinstance(host_str, str):
            host_str = str(host_str)

        try:
            socket.inet_aton(host_str)  # sudah IPv4 literal, skip resolve
            return _original_getaddrinfo(host, port, family, type, proto, flags)
        except OSError:
            pass

        now = time.time()
        cached = _custom_dns_cache.get(host_str)
        if cached and now - cached[1] < _CUSTOM_DNS_CACHE_TTL_SECS:
            ip = cached[0]
        else:
            answer = _custom_dns_resolver.resolve(host_str, "A", lifetime=5.0)
            ip = answer[0].to_text()
            _custom_dns_cache[host_str] = (ip, now)

        try:
            return _original_getaddrinfo(ip, port, family, type, proto, flags)
        except OSError:
            # IP hasil custom DNS ternyata gak konek — fallback ke resolver asli.
            return _original_getaddrinfo(host, port, family, type, proto, flags)

    socket.getaddrinfo = _custom_getaddrinfo
    print(f"[DNS] Custom DNS aktif: {', '.join(_CUSTOM_DNS_SERVERS)} (semua koneksi: Telegram, Gemini, Groq, OpenRouter)")

# ── Auto-cek & auto-install requirements.txt ──────────────────────────────────
# Jalan SEBELUM import library pihak ketiga (pyrogram, motor, dll) di bawah ini.
# Cara kerja: baca requirements.txt, cek tiap package sudah ke-install atau
# belum pakai importlib.metadata (stdlib, tanpa perlu pip installed dulu untuk
# cek-nya). Kalau ada yang kurang → jalanin `pip install -r requirements.txt`
# sekali secara diam-diam, baru lanjut import seperti biasa.
#
# Toggle: AUTO_INSTALL_REQUIREMENTS=1 (default nyala) / 0 (matikan, balik ke
# perilaku lama — kamu pip install manual sendiri).
#
# SENGAJA fail-safe: kalau proses install gagal (mis. HP lagi offline), TIDAK
# menghentikan bot — tetap lanjut jalan pakai package yang sudah ada, karena
# bisa jadi semua sudah lengkap dan cek-nya saja yang gagal.
_AUTO_INSTALL_REQS = os.environ.get("AUTO_INSTALL_REQUIREMENTS", "1").strip().lower() in ("1", "true", "yes")

if _AUTO_INSTALL_REQS:
    def _ensure_requirements_installed():
        import re
        import subprocess
        import importlib.metadata as _ilm

        req_path = _Path(__file__).resolve().parent / "requirements.txt"
        if not req_path.exists():
            return

        try:
            lines = req_path.read_text(encoding="utf-8").splitlines()
        except OSError as e:
            print(f"[REQS] Gak bisa baca requirements.txt: {e}")
            return

        missing: list[str] = []
        for raw in lines:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            # Ambil nama package doang (buang versi/extras), biar cek longgar —
            # kita cuma mau tahu "ke-install atau nggak", bukan cocokin versi persis.
            name = re.split(r"[<>=!~\[; ]", line, maxsplit=1)[0].strip()
            if not name:
                continue
            try:
                _ilm.version(name)
            except _ilm.PackageNotFoundError:
                missing.append(line)

        if not missing:
            return

        print(f"[REQS] {len(missing)} package belum ke-install — auto-install jalan...")
        try:
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "--break-system-packages",
                 "-q", "-r", str(req_path)],
                check=True,
                timeout=600,
            )
            print("[REQS] Auto-install selesai.")
        except Exception as e:
            print(f"[REQS] Auto-install gagal ({e}) — lanjut pakai package yang ada.")

    _ensure_requirements_installed()

# ── Tagging log Pyrogram/Pyrofork generik (socket.send() raised exception,
# Connection lost, dst) supaya ketahuan client mana yang bermasalah — lihat
# core/client_log_tag.py untuk detail lengkap cara kerjanya.
from core.client_log_tag import install_client_log_tagging
install_client_log_tagging()

from pyrogram import Client, idle
from security_os.device_pool import device_for_seed as _device_for_seed
from pyrogram import filters

# BYPASS UNTUK JALUR RAILWAY AGAR TIDAK ATTRIBUTE ERROR
if not hasattr(filters, "forum"):
    # Jika .forum tidak ada, arahkan otomatis ke .topics atau pasang filter kustom
    filters.forum = getattr(filters, "topics", getattr(filters, "forum_topic", filters.group))
from pyrogram.types import (
    BotCommand,
    BotCommandScopeAllPrivateChats,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllChatAdministrators,
)
from http.server import BaseHTTPRequestHandler, HTTPServer

# ── Path fix: pastikan semua import lokal bisa ditemukan dari CWD manapun ─────
# _BOT_DIR adalah folder tempat main.py berada (misal: /sdcard/bot-main/).
# sys.path.insert memastikan Python selalu menemukan modules lokal (database,
# plugins/, core/, dll) meskipun script dijalankan dari direktori lain.
_BOT_DIR = _Path(__file__).resolve().parent
if str(_BOT_DIR) not in sys.path:
    sys.path.insert(0, str(_BOT_DIR))

# ── Folder security_os/ ────────────────────────────────────────────────────────
# video_call.py, monitor_bot_reference.py, dan admin_session.py dipindah ke
# subfolder security_os/ agar tidak bercampur dengan file utama di root proyek.
# Ditambahkan ke sys.path (bukan diimpor sebagai package security_os.xxx) supaya
# SEMUA import lama yang sudah ada di seluruh proyek — `from video_call import
# ...`, `import admin_session as ...`, `from monitor_bot_reference import ...`
# — tetap berfungsi tanpa perlu diubah satu per satu di setiap file plugin.
_SECURITY_OS_DIR = _BOT_DIR / "security_os"
if str(_SECURITY_OS_DIR) not in sys.path:
    sys.path.insert(0, str(_SECURITY_OS_DIR))

from database import setup_db, delete_worker, panel_write_worker, close_db, get_bot_config, save_bot_config, get_active_backend, ensure_mention_global_index, ensure_mention_pending_index, mention_pending_resolve_loop, ensure_mention_bio_scan_index
from admin_session import start_cleanup_task as _adm_cleanup
from video_call import start_userbot, stop_userbot

# ── Termux: ambil OWNER_ID ────────────────────────────────────────────────────
OWNER_ID = int(os.environ.get("OWNER_ID", 0))

# ── Env ───────────────────────────────────────────────────────────────────────
API_ID    = int(os.environ.get("API_ID", 0))
API_HASH  = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CODE_BOT  = os.environ.get("CODE_BOT", "").strip()

# ── Session name — berbasis CODE_BOT jika tersedia, fallback ke bot_id ────────
# Jika CODE_BOT diset:
#   • Semua bot dengan CODE_BOT yang sama berbagi satu file session.
#   • Ganti BOT_TOKEN → session lama tetap dipakai, pengaturan grup tidak reset.
# Jika CODE_BOT kosong:
#   • Fallback ke bot_id dari token (perilaku lama) agar tidak patah.
_BOT_ID = BOT_TOKEN.split(":")[0] if ":" in BOT_TOKEN else "default"

# ── Session suffix: selalu berbasis CODE_BOT + BOT_ID ─────────────────────────
# Tujuan: 2 bot clone (CODE_BOT sama, BOT_TOKEN beda) bisa jalan bersamaan
# tanpa berebut file session. Data grup/regex/dll tetap berbagi lewat CODE_BOT.
# Contoh:
#   Bot 1: CODE_BOT=produksi, BOT_ID=111 → session: antispam_bot_produksi_111
#   Bot 2: CODE_BOT=produksi, BOT_ID=222 → session: antispam_bot_produksi_222
#   Keduanya baca/tulis database namespace "produksi" yang sama.
_SESSION_SUFFIX = f"{CODE_BOT}_{_BOT_ID}" if CODE_BOT else f"token_{_BOT_ID}"
_SESSION_NAME = str(_BOT_DIR / f"antispam_bot_{_SESSION_SUFFIX}")


# ── Lock instance lokal — FIX AUTH_KEY_DUPLICATED di panel non-Railway ────────
# _deploy_signal_new()/_deploy_watch_and_release() di bawah HANYA aktif kalau
# backend DB = MongoDB (get_active_backend() == "mongo"). Di panel hosting
# generik (Pterodactyl/VPS pakai SQLite, seperti pada screenshot log user —
# "[LOG ERROR] Telegram says: [406 AUTH_KEY_DUPLICATED] ... session file
# used in more than one place simultaneously") TIDAK ADA proteksi apapun
# terhadap 2 proses yang kebetulan jalan bersamaan memakai file .session yang
# sama — misal admin klik "Restart" di panel sebelum proses lama benar-benar
# berhenti, atau proses lama macet/zombie. Lock PID lokal ini menutup celah
# itu terlepas dari backend DB yang dipakai: sebelum login ke Telegram, cek
# apakah ada PID lain yang MASIH HIDUP memegang lock session yang sama — kalau
# ada, tolak start dengan pesan jelas alih-alih diam-diam membuat 2 sesi aktif
# bersamaan (yang berujung AUTH_KEY_DUPLICATED dan membuat kedua proses saling
# menimpa data, termasuk state Upgrade Speed di memory/DB).
def _session_lock_path() -> str:
    return _SESSION_NAME + ".lock"


def _pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # proses ada, cuma beda owner — tetap anggap hidup
    except Exception:
        return False
    return True


def _acquire_session_lock() -> None:
    """
    Cek + tulis lock PID lokal untuk session ini. SystemExit kalau proses lain
    yang masih hidup sudah memegang lock (mencegah AUTH_KEY_DUPLICATED akibat
    2 proses jalan bersamaan dengan session file yang sama).
    """
    lock_path = _session_lock_path()
    if os.path.exists(lock_path):
        try:
            with open(lock_path, "r") as f:
                old_pid = int((f.read() or "0").strip())
        except Exception:
            old_pid = 0
        if old_pid and old_pid != os.getpid() and _pid_is_alive(old_pid):
            print(
                f"\n❌ [Lock] Proses lain (PID {old_pid}) MASIH BERJALAN memakai "
                f"session yang sama ({_SESSION_NAME}.session).\n"
                f"   Ini penyebab paling umum error 'AUTH_KEY_DUPLICATED' di log.\n"
                f"   → Tekan Stop di panel, TUNGGU sampai proses lama benar-benar\n"
                f"     mati, baru tekan Start lagi (jangan langsung Restart saat\n"
                f"     proses sebelumnya belum sempat berhenti sempurna).\n"
            )
            raise SystemExit(1)
        if old_pid and not _pid_is_alive(old_pid):
            print(f"[Lock] ℹ️  Lock lama (PID {old_pid}) sudah mati — diambil alih.")
        # Lock basi (proses lama sudah mati) — hapus dulu supaya O_EXCL di
        # bawah tidak salah menganggap masih ada pemegang lock.
        try:
            os.remove(lock_path)
        except Exception:
            pass
    # Tulis PAKAI O_EXCL (atomic create-if-not-exists di level OS) supaya 2
    # proses yang start nyaris bersamaan (race check-then-write di atas)
    # tidak bisa DUANYA lolos menulis lock — hanya satu yang berhasil buka
    # file, yang kalah dapat FileExistsError dan tahu harus mundur.
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w") as f:
            f.write(str(os.getpid()))
    except FileExistsError:
        # Proses lain menang race barusan — cek ulang apakah PID-nya hidup.
        try:
            with open(lock_path, "r") as f:
                race_pid = int((f.read() or "0").strip())
        except Exception:
            race_pid = 0
        if race_pid and race_pid != os.getpid() and _pid_is_alive(race_pid):
            print(f"\n❌ [Lock] Proses lain (PID {race_pid}) memenangkan race start "
                  f"bersamaan untuk session yang sama — proses ini berhenti untuk "
                  f"mencegah AUTH_KEY_DUPLICATED.\n")
            raise SystemExit(1)
    except Exception as e:
        print(f"[Lock] ⚠️  Gagal tulis lock file (lanjut tanpa lock): {e}")


def _release_session_lock() -> None:
    lock_path = _session_lock_path()
    try:
        if os.path.exists(lock_path):
            with open(lock_path, "r") as f:
                pid_in_file = int((f.read() or "0").strip())
            if pid_in_file == os.getpid():
                os.remove(lock_path)
    except Exception as e:
        print(f"[Lock] ⚠️  Gagal hapus lock file: {e}")


def _print_startup_banner():
    """Tampilkan banner info bot saat startup di Termux."""
    print(f"\n")
    print(f"{'  BOT ANTISPAM + NEXUS AI  ':^52}")

    token_display = (BOT_TOKEN[:8] + "…" + BOT_TOKEN[-4:]) if len(BOT_TOKEN) > 12 else "(tidak diset)"
    sess_display  = f"antispam_bot_{_SESSION_SUFFIX}.session"
    print(f"  API_ID    : {str(API_ID) if API_ID else '(tidak diset)':<39}")
    print(f"  BOT_TOKEN : {token_display:<39}")
    print(f"  BOT_ID    : {_BOT_ID:<39}")
    print(f"  Session   : {sess_display:<39}")
    print(f"  OWNER_ID  : {str(OWNER_ID) if OWNER_ID else '(tidak diset)':<39}")
    if CODE_BOT:
        print(f"  CODE_BOT  : [{CODE_BOT}]{'':>{39 - len(CODE_BOT) - 2}}")
        print(f"  Namespace : aktif — data & session berbagi per CODE_BOT")
    else:
        print(f"  CODE_BOT  : (kosong — tidak ada isolasi)        ")
        print(f"  ⚠️  Set CODE_BOT di .env agar data tidak campur ")

    print(f"  Info backend database menyusul di bawah...      ")
    print(f"\n")

# ── Client ────────────────────────────────────────────────────────────────────
# Session name = path absolut + bot_id suffix.
# Tiap BOT_TOKEN punya file .session sendiri → tidak pernah bentrok.
# plugins root tetap "plugins" (nama modul Python, bukan path filesystem) —
# Python sudah tahu mencarinya lewat sys.path yang sudah diset di atas.
_SESSION_DB_KEY = f"pyrogram_session_{_SESSION_SUFFIX}"

# Fingerprint device untuk "Perangkat Aktif" Telegram — deterministik dari
# _BOT_ID (bukan random.choice biasa) supaya TETAP SAMA setiap restart/redeploy,
# bukannya ganti device tiap kali bot naik ulang (yang justru terlihat aneh).
_DEVICE_MODEL, _SYSTEM_VERSION, _APP_VERSION = _device_for_seed(int(_BOT_ID) if _BOT_ID.isdigit() else hash(_BOT_ID))

app: Client = None  # diinisialisasi di _build_client() dalam main()


async def _build_client() -> Client:
    """
    Buat Pyrogram Client pakai file session lokal seperti biasa.
    Setelah login, file session disimpan ke MongoDB sebagai backup.
    """
    client = Client(
        _SESSION_NAME,
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
        plugins=dict(root="plugins"),
        device_model=_DEVICE_MODEL,
        system_version=_SYSTEM_VERSION,
        app_version=_APP_VERSION,
    )
    return client


async def _restore_session_from_mongo() -> bool:
    """
    Pulihkan file .session dari MongoDB jika file lokal tidak ada.
    Hanya restore jika file lokal TIDAK ADA (misal setelah Railway redeploy).
    Jika BOT_TOKEN berubah sejak session terakhir disimpan → hapus session lama
    dan biarkan bot login ulang dengan token baru.
    """
    import base64, os as _os

    if get_active_backend() != "mongo":
        return False

    session_path = _SESSION_NAME + ".session"
    if _os.path.exists(session_path):
        return False  # File lokal ada, tidak perlu restore

    # ── Cek apakah BOT_TOKEN berubah sejak session terakhir disimpan ──────────
    _TOKEN_DB_KEY = f"last_bot_token_{_SESSION_SUFFIX}"
    saved_token = await get_bot_config(_TOKEN_DB_KEY)
    if saved_token and saved_token != BOT_TOKEN:
        print(f"[Session] ⚠️  BOT_TOKEN berubah — session lama dihapus, bot login ulang.")
        await save_bot_config(_SESSION_DB_KEY, None)
        await save_bot_config(_TOKEN_DB_KEY, None)
        return False

    saved_bytes = await get_bot_config(_SESSION_DB_KEY)
    if not saved_bytes:
        print(f"[Session] ℹ️  Belum ada session di MongoDB, bot akan login baru.")
        return False

    try:
        raw = base64.b64decode(saved_bytes.encode())
        with open(session_path, "wb") as _f:
            _f.write(raw)
        print(f"[Session] ✅ File session dipulihkan dari MongoDB.")
        return True
    except Exception as e:
        print(f"[Session] ⚠️  Gagal pulihkan session: {e}")
        return False


async def _clear_session_from_mongo() -> None:
    """Hapus session dari MongoDB — dipanggil jika session yang dipulihkan ditolak Telegram."""
    try:
        await save_bot_config(_SESSION_DB_KEY, None)
        print(f"[Session] 🗑️  Session lama dihapus dari MongoDB.")
    except Exception as e:
        print(f"[Session] ⚠️  Gagal hapus session dari MongoDB: {e}")


async def _save_session_to_mongo() -> None:
    """
    Baca file .session dari disk dan simpan isinya (base64) ke MongoDB.
    Dipanggil setelah app.start() berhasil — MongoDB selalu diupdate dari file lokal.
    Juga menyimpan BOT_TOKEN aktif agar saat redeploy bisa deteksi token berubah.
    """
    import base64, os as _os

    if get_active_backend() != "mongo":
        return
    try:
        session_path = _SESSION_NAME + ".session"
        if not _os.path.exists(session_path):
            return
        with open(session_path, "rb") as _f:
            raw = _f.read()
        encoded = base64.b64encode(raw).decode()
        await save_bot_config(_SESSION_DB_KEY, encoded)
        # Simpan token aktif untuk deteksi perubahan di deploy berikutnya
        _TOKEN_DB_KEY = f"last_bot_token_{_SESSION_SUFFIX}"
        await save_bot_config(_TOKEN_DB_KEY, BOT_TOKEN)
        print(f"[Session] ✅ Session disimpan ke MongoDB.")
    except Exception as e:
        print(f"[Session] ⚠️  Gagal simpan session ke MongoDB: {e}")


async def _periodic_session_backup() -> None:
    """
    Simpan session ke MongoDB setiap 20 menit secara berkala.

    Tujuan: peer cache di .session terus bertambah saat bot berjalan
    (setiap user/grup/channel baru yang ditemui langsung masuk ke SQLite lokal).
    Tanpa backup berkala, redeploy berikutnya hanya mendapat snapshot saat startup —
    semua peer baru yang ditemui setelah itu hilang → PeerIdInvalid.

    Interval 20 menit = trade-off antara write ke MongoDB vs freshness peer cache.
    """
    while True:
        await asyncio.sleep(20 * 60)  # 20 menit
        await _save_session_to_mongo()
        print("[Session] 🔄 Periodic backup session selesai.")

# ── Passive DM User Collector ─────────────────────────────────────────────────
# Mencatat user yang DM bot ke dm_users (dipakai /cast untuk broadcast).

from pyrogram import Client as _PClient, filters as _pfilters
from pyrogram.handlers import MessageHandler as _MsgHandler
from pyrogram.types import Message as _Msg
from core import dm_peer_cache as _dm_peer_cache


async def _dm_peer_collector_handler(client, message: _Msg) -> None:
    """
    Handler pasif untuk CHAT PRIBADI — mencatat user yang pernah DM bot
    langsung ke dm_users (lihat database.register_dm_user), dipakai /cast
    untuk broadcast.

    DEBOUNCE PENTING: dm_users dipetakan ke GLOBAL_CLUSTER (cluster 3, lihat
    core/mongo_shard.py) — cluster free-tier ~100 ops/detik, dipakai bareng
    gcast_global/mention_global_cache/warn_once dkk. Alur DM di bot ini (FSM
    regex/VIP/request-peer/donasi) bisa menghasilkan BANYAK pesan beruntun
    dari 1 user dalam waktu singkat — kalau tiap pesan langsung nulis ke
    Mongo, ini bisa ikut menyita kuota cluster 3 tanpa perlu (data user jarang
    berubah). Maka di-cache in-memory dulu, HANYA benar2 nulis ke Mongo kalau
    user ini belum tercatat dalam _DM_PEER_DEBOUNCE_TTL detik terakhir.
    """
    try:
        user = message.from_user
        if not user or user.is_bot:
            return
        uid = user.id

        if not _dm_peer_cache.should_write(uid):
            return

        from database import register_dm_user
        asyncio.create_task(register_dm_user(uid))
    except Exception as _exc:
        _ec = type(_exc).__name__
        if _ec not in ("FloodWait", "PeerIdInvalid", "UserNotParticipant", "UsernameNotOccupied"):
            print(f"[DmPeerCollector] ⚠️ {_ec}: {_exc}")


def register_peer_collector(app) -> None:
    """
    Daftarkan handler pasif ke instance Client.
    Dipanggil setelah app.start() — sebelum idle().
    Group=-1 agar jalan sebelum handler lain, tapi tidak pernah stop propagation.
    """
    app.add_handler(_MsgHandler(_dm_peer_collector_handler, _pfilters.private), group=-1)
    print("[PeerCollector] ✅ Handler pasif aktif — setiap DM masuk akan dicatat ke dm_users.")


# ── Deploy Handshake via MongoDB ──────────────────────────────────────────────
# Masalah: Railway start instance baru SEBELUM instance lama benar-benar mati.
# Dua koneksi aktif ke Telegram → AuthKeyDuplicated → session invalid.
#
# Solusi: instance baru sinyal ke MongoDB, instance lama deteksi dan disconnect
# lebih dulu, baru instance baru lanjut app.start().
#
# Flag MongoDB yang dipakai (key = f"deploy_{_SESSION_SUFFIX}"):
#   "pending"  → instance baru sudah siap, minta instance lama shutdown
#   "released" → instance lama sudah disconnect, instance baru boleh start
#   "active"   → instance baru sudah running (tulis setelah app.start())

_DEPLOY_FLAG_KEY = f"deploy_{_SESSION_SUFFIX}"

# FIX (bug: session userbot tidak tersimpan saat redeploy): _DEPLOY_ID
# SEBELUMNYA hanya str(os.getpid()). Di container Docker/Railway, proses
# pertama yang dijalankan di dalam container baru hampir selalu mendapat
# PID 1 (PID namespace baru per container). Akibatnya deploy LAMA dan
# deploy BARU bisa punya _DEPLOY_ID yang SAMA PERSIS ("1"). Pengecekan
# `data.get("by") != _DEPLOY_ID` di _deploy_watch_and_release() jadi
# False terus — instance lama menyangka sinyal "pending" itu datang dari
# dirinya sendiri, sehingga graceful_shutdown() (yang menyimpan session
# userbot via stop_userbot()) TIDAK PERNAH terpanggil lewat jalur ini.
# Satu-satunya jalur penyelamat tersisa adalah SIGTERM, yang tidak selalu
# sempat selesai sebelum Railway mengirim SIGKILL.
#
# Solusi: _DEPLOY_ID sekarang gabungan PID + waktu proses dimulai + token
# acak — kombinasi ini praktis mustahil sama antara dua proses berbeda,
# bahkan jika kebetulan keduanya mendapat PID yang sama.
import time as _time_deploy_id
import uuid as _uuid_deploy_id
_DEPLOY_ID = f"{os.getpid()}-{int(_time_deploy_id.time())}-{_uuid_deploy_id.uuid4().hex[:8]}"


async def _deploy_signal_new() -> None:
    """
    Instance BARU: cek dulu apakah ada instance aktif (state='active') di MongoDB.
    - Tidak ada flag / flag bukan 'active'  → deploy pertama atau script lama
                                               → langsung lanjut, tidak perlu tunggu.
    - Flag 'active' ada (script baru sudah jalan sebelumnya)
                                               → tulis 'pending', tunggu 'released'
                                                 maks 30 detik.
    """
    if get_active_backend() != "mongo":
        return

    import json, time

    # ── Cek apakah ada instance aktif ────────────────────────────────────────
    raw = await get_bot_config(_DEPLOY_FLAG_KEY)
    if raw:
        try:
            existing = json.loads(raw)
        except Exception:
            existing = {}
    else:
        existing = {}

    if existing.get("state") != "active":
        # Tidak ada instance lama yang pakai script baru → lanjut langsung
        print(f"[Deploy] ℹ️  Tidak ada instance aktif di MongoDB (state={existing.get('state', 'kosong')!r}). "
              f"Lanjut start tanpa tunggu.")
        return

    # ── Ada instance aktif → sinyal dan tunggu ───────────────────────────────
    payload = json.dumps({"state": "pending", "by": _DEPLOY_ID, "ts": time.time()})
    await save_bot_config(_DEPLOY_FLAG_KEY, payload)
    print(f"[Deploy] 🆕 Instance aktif ditemukan. Flag 'pending' ditulis (deploy_id={_DEPLOY_ID}). "
          f"Tunggu instance lama release (maks 30 detik)...")

    # FIX: get_event_loop() di dalam async function → pakai get_running_loop()
    # (sudah ada running loop saat ini, tidak perlu get/create loop baru)
    deadline = asyncio.get_running_loop().time() + 30
    while asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(1)
        raw = await get_bot_config(_DEPLOY_FLAG_KEY)
        if not raw:
            break
        try:
            data = json.loads(raw)
        except Exception:
            break
        if data.get("state") == "released":
            print(f"[Deploy] ✅ Instance lama sudah release. Lanjut start...")
            return

    print(f"[Deploy] ⏰ Timeout 30 detik — lanjut start paksa "
          f"(instance lama tidak merespons atau sudah mati).")


async def _deploy_watch_and_release() -> None:
    """
    Instance LAMA: poll MongoDB setiap 2 detik. Jika ada flag 'pending' dari
    deploy baru (bukan dari diri sendiri), HANYA update session ke MongoDB
    lalu tulis flag 'released' agar instance baru bisa lanjut start.
    Instance lama TIDAK shutdown — Railway yang akan kill prosesnya via SIGTERM.
    Berjalan sebagai background task sejak awal.

    Kenapa tidak shutdown di sini:
      Jika instance lama langsung shutdown + loop.stop() saat flag 'pending'
      terdeteksi, ada race condition — flag 'released' sudah ditulis tapi
      _save_session_to_mongo() belum tentu selesai saat instance baru sudah
      ambil alih koneksi Telegram. Session jadi tidak tersimpan sempurna.
      Dengan membiarkan instance lama tetap jalan, Railway yang kill pada
      waktunya via SIGTERM — dan saat itu graceful_shutdown() via SIGTERM
      handler yang menangani save session dengan benar.
    """
    if get_active_backend() != "mongo":
        return

    import json
    print(f"[Deploy] 👀 Deploy watcher aktif (pid={_DEPLOY_ID}).")
    while True:
        await asyncio.sleep(2)
        try:
            raw = await get_bot_config(_DEPLOY_FLAG_KEY)
            if not raw:
                continue
            data = json.loads(raw)
        except Exception:
            continue

        # Ada permintaan deploy baru, bukan dari diri sendiri
        if data.get("state") == "pending" and data.get("by") != _DEPLOY_ID:
            print(f"[Deploy] 🔄 Deploy baru terdeteksi. Update session ke MongoDB (tanpa shutdown)...")

            # v5.5 — Tandai instance ini "releasing" SEBELUM save session,
            # supaya semua loop VC (main/custom/promo userbot) langsung
            # berhenti memulai join/scan BARU secepat mungkin — instance
            # baru bakal ambil alih semua VC-related API calls dari sini.
            # Lihat core/deploy_state.py untuk alasan lengkapnya.
            try:
                from core.deploy_state import mark_releasing
                mark_releasing()
            except Exception as e:
                print(f"[Deploy] ⚠️  Gagal set deploy_state.mark_releasing(): {e}")

            # Save session dulu — pastikan peer cache terbaru tersimpan
            try:
                await _save_session_to_mongo()
                from monitor_bot_reference import save_all_sessions
                await save_all_sessions()
            except Exception as e:
                print(f"[Deploy] ⚠️  Gagal update session sebelum release: {e}")

            # Tulis flag 'released' agar instance baru tidak menunggu timeout 30 detik
            import time
            released = json.dumps({"state": "released", "by": _DEPLOY_ID, "ts": time.time()})
            try:
                await save_bot_config(_DEPLOY_FLAG_KEY, released)
            except Exception:
                pass

            print(f"[Deploy] ✅ Session sudah diupdate & flag 'released' ditulis. "
                  f"Instance lama TETAP berjalan hingga Railway kill via SIGTERM.")
            # Tidak shutdown, tidak stop loop — lanjut polling seperti biasa
            continue


async def _deploy_mark_active() -> None:
    """Instance baru setelah app.start() berhasil: tulis flag 'active'."""
    if get_active_backend() != "mongo":
        return
    import json, time
    payload = json.dumps({"state": "active", "by": _DEPLOY_ID, "ts": time.time()})
    await save_bot_config(_DEPLOY_FLAG_KEY, payload)
    print(f"[Deploy] ✅ Flag 'active' ditulis (deploy_id={_DEPLOY_ID}).")


async def _rewarm_known_peers(client) -> None:
    """
    Setelah redeploy, session baru tidak punya peer cache sama sekali.
    Fungsi ini resolve ulang semua grup/channel yang sudah dikenal di DB
    agar langsung masuk ke peer cache — mencegah PeerIdInvalid saat bot
    pertama kali mencoba kirim pesan ke chat tersebut.

    Dipanggil sekali setelah app.start() + _restore_session_from_mongo().
    Jika session berhasil di-restore dari MongoDB, rewarm tetap dijalankan
    untuk memastikan semua peer yang mungkin hilang ter-resolve ulang.
    """
    from database import config_db, nexus_grup_db, get_active_backend as _backend
    from database import group_action_log_db, local_mute_db

    if _backend() != "mongo":
        return

    peer_ids: set[int] = set()
    # username_map: chat_id → "@username" — dipakai sebagai jalur resolve
    # utama saat sesi baru (username tidak butuh access hash)
    username_map: dict[int, str] = {}

    # Grup/channel dari config_db
    try:
        async for doc in config_db.find({}):
            cid = doc.get("chat_id")
            if cid:
                cid = int(cid)
                peer_ids.add(cid)
                uname = doc.get("username")
                if uname:
                    username_map[cid] = f"@{uname.lstrip('@')}"
    except Exception as e:
        print(f"[Rewarm] ⚠️  Gagal baca config_db: {e}")

    # Grup dari nexus_grup_db
    try:
        async for doc in nexus_grup_db.find({}):
            cid = doc.get("chat_id")
            if cid:
                cid = int(cid)
                peer_ids.add(cid)
                uname = doc.get("username")
                if uname and cid not in username_map:
                    username_map[cid] = f"@{uname.lstrip('@')}"
    except Exception as e:
        print(f"[Rewarm] ⚠️  Gagal baca nexus_grup_db: {e}")

    # CHANNEL_OWNER, LOG_CHANNEL, LOG_QRIS dari env
    for _env_key in ("CHANNEL_OWNER", "LOG_CHANNEL", "LOG_QRIS"):
        try:
            _ch_id = int(os.environ.get(_env_key, 0))
            if _ch_id:
                peer_ids.add(_ch_id)
                # Baca username yang disimpan saat startup sebelumnya
                from database import get_bot_config as _gcfg
                _uname = await _gcfg(f"{_env_key.lower()}_username")
                if _uname and _ch_id not in username_map:
                    username_map[_ch_id] = f"@{_uname.lstrip('@')}"
        except Exception:
            pass

    # Resolve grup/channel — prioritas @username (tidak butuh access hash di sesi baru),
    # fallback ke integer ID (butuh access hash; mungkin gagal di sesi baru).
    #
    # JEDA 3 DETIK (bukan 0.3): client.get_chat() pada channel/supergroup
    # memanggil channels.GetFullChannel di balik layar — method ini punya
    # limit rate yang ketat di Telegram (umumnya kena FloodWait jauh sebelum
    # 0.3s/panggilan terkumpul banyak). Jeda 0.3s TETAP memicu FloodWait
    # berulang setiap redeploy untuk akun dengan banyak grup — yang artinya
    # rewarm tidak benar-benar tercapai pada saat-saat itu (request dibuang,
    # lalu Pyrofork sendiri yang menunggu durasi FloodWait), jadi 0.3s di
    # sini bukan "lebih cepat", hanya membuang kuota request percuma.
    # 3 detik = aman di bawah ambang limit channels.GetFullChannel pada
    # kondisi normal, sehingga FloodWait seharusnya tidak terpicu sama sekali.
    REWARM_CHAT_DELAY = float(os.environ.get("REWARM_CHAT_DELAY", 3.0))

    from pyrogram.errors import FloodWait as _RewarmFloodWait

    async def _get_chat_safe(ident) -> bool:
        """Coba get_chat 1x; jika FloodWait, tunggu durasinya lalu retry SEKALI
        (supaya peer ini tetap ter-resolve, bukan langsung dianggap gagal
        permanen hanya karena kena rate limit sesaat)."""
        try:
            await client.get_chat(ident)
            return True
        except _RewarmFloodWait as fw:
            print(f"[Rewarm] ⏳ FloodWait {fw.value}s saat resolve {ident} — menunggu lalu retry 1x...")
            await asyncio.sleep(fw.value + 1)
            try:
                await client.get_chat(ident)
                return True
            except Exception:
                return False
        except Exception:
            return False

    ok, fail = 0, 0
    for cid in peer_ids:
        resolved = False
        # Coba via @username dulu — lebih andal di sesi baru
        if cid in username_map:
            resolved = await _get_chat_safe(username_map[cid])
        # Fallback ke integer ID (berhasil jika access hash masih ada di session)
        if not resolved:
            resolved = await _get_chat_safe(cid)
        ok += 1 if resolved else 0
        fail += 0 if resolved else 1
        await asyncio.sleep(REWARM_CHAT_DELAY)
    print(f"[Rewarm] ✅ Chat: {ok} berhasil, {fail} gagal ({len(peer_ids)} total)")

# ── Health Check ──────────────────────────────────────────────────────────────
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        # FIX: sebelumnya SELALU 200 apa pun kondisinya — platform (Railway
        # dst) yang mengandalkan endpoint ini untuk auto-restart-on-failure
        # TIDAK PERNAH tahu bot sebenarnya macet (mis. nyangkut di retry
        # "socket.send() raised exception." internal Pyrogram tanpa pernah
        # pulih). Sekarang dicek ke core/network_watchdog — 503 kalau ada
        # client yang downtime-nya sudah lewat ambang, supaya platform ikut
        # restart sebagai lapis pengaman kedua (lapis pertama: os._exit()
        # otomatis dari watchdog itu sendiri begitu downtime lewat batas).
        try:
            from core.network_watchdog import overall_healthy
            healthy = overall_healthy()
        except Exception:
            healthy = True  # watchdog belum aktif/gagal diimpor — jangan block startup awal
        self.send_response(200 if healthy else 503)
        self.end_headers()
        self.wfile.write(b"Bot Antispam + Nexus AI Online 2026" if healthy else b"Unhealthy: koneksi Telegram macet, menunggu restart.")

    def log_message(self, *args):
        pass


def run_health_check():
    try:
        port = int(os.environ.get("PORT", 8000))
        server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
        server.serve_forever()
    except Exception as e:
        print(f"[HealthCheck] Error: {e}")


# ── Set Bot Commands ──────────────────────────────────────────────────────────
#
# ARSITEKTUR SCOPE (3 lapisan grup + 1 DM global + per-user DM via dm_menu.py):
#
#   GRUP — BotCommandScopeAllGroupChats         → terlihat semua MEMBER di semua grup
#   GRUP — BotCommandScopeAllChatAdministrators → terlihat hanya ADMIN di semua grup
#   DM   — BotCommandScopeAllPrivateChats       → terlihat semua user (baseline)
#   DM   — BotCommandScopeChat(user_id)         → di-set ulang tiap /start DM
#                                                  via core/dm_menu.py (owner/admin/member)
#
# CATATAN:
#   • Telegram menerapkan scope paling spesifik — admin grup melihat list
#     BotCommandScopeAllChatAdministrators, bukan BotCommandScopeAllGroupChats.
#   • Command khusus owner di grup (bjoin, bleave, ns_reset, addregex, dll)
#     TIDAK dimasukkan ke scope global karena Telegram tidak punya scope
#     "owner grup" — owner sudah hafal perintahnya sendiri.
#   • Menu DM per-role (member/admin/owner) dikelola di core/dm_menu.py dan
#     di-refresh setiap kali user /start di DM.
#
async def _setup_commands():
    try:
        # ── 1. GRUP — Semua member (termasuk non-admin) ───────────────────────
        await app.set_bot_commands(
            commands=[
                BotCommand("antigcast",    "bot apa ini"),
                BotCommand("unmutemic",    "hapus/privat link bio dulu, baru bisa ngobrol"),
                BotCommand("govip",        "info cara daftar jadi VIP member"),
                BotCommand("reportsticker","reply stiker → lapor pack spam ke owner"),
                BotCommand("reportonkem",  "lapor onkem dadakan / cek VC aktif"),
                BotCommand("pelanggaranku","cek riwayat pelanggaranmu di grup ini"),
            ],
            scope=BotCommandScopeAllGroupChats(),
        )

        # ── 2. GRUP — Admin saja (menimpa list member untuk admin) ────────────
        await app.set_bot_commands(
            commands=[
                BotCommand("antigcast",       "bot apa ini"),
                # Dapat semua perintah member +
                BotCommand("unmutemic",       "hapus/privat link bio dulu, baru bisa ngobrol"),
                BotCommand("govip",           "info cara daftar jadi VIP member"),
                BotCommand("reportsticker",   "reply stiker → lapor pack spam ke owner"),
                BotCommand("reportonkem",     "lapor onkem dadakan / cek VC aktif"),
                BotCommand("pelanggaranku",   "cek riwayat pelanggaranmu di grup ini"),
                # Panel & konfigurasi grup
                BotCommand("status",          "cek status fitur bot di grup ini"),
                BotCommand("spam",            "reply pesan → lapor spam ke AI Nexus"),
                # Toggle fitur
                BotCommand("setlocal",        "toggle anti-duplikat lokal on/off"),
                BotCommand("setglobal",       "toggle anti-gcast global on/off"),
                BotCommand("setbio",          "toggle filter bio on/off"),
                BotCommand("setwaktu",        "atur durasi expiry tracker duplikat"),
                # Welcome
                BotCommand("setwelcome",      "toggle/atur pesan welcome anggota baru"),
                BotCommand("setwelcomedelay", "atur jeda hapus otomatis pesan welcome"),
                BotCommand("setwelcometext",  "ubah teks welcome custom"),
                # Regex per-grup
                BotCommand("addgroupregex",   "tambah filter kata spam di grup ini"),
                BotCommand("delgroupregex",   "hapus filter kata spam di grup ini"),
                BotCommand("listgroupregex",  "daftar filter kata spam aktif"),
                # VIP per-grup
                BotCommand("vip",             "reply user → jadikan VIP (bypass filter)"),
                BotCommand("unvip",           "reply user → cabut status VIP"),
                # CAS
                BotCommand("wlcas",           "whitelist user dari ban CAS global"),
                BotCommand("unwlcas",         "hapus whitelist CAS user"),
                # Newscore & Mention
                BotCommand("ns_score",        "leaderboard keaktifan newscore"),
                BotCommand("resetmentioncache","reset cache mention di grup ini"),
                BotCommand("testprivmode",    "cek apakah privmode bot aktif di grup"),
            ],
            scope=BotCommandScopeAllChatAdministrators(),
        )

        # ── 3. DM — Semua user (baseline, sebelum per-role di-set oleh /start) ─
        await app.set_bot_commands(
            commands=[
                BotCommand("start",         "menu utama & cara pakai bot"),
                BotCommand("pelanggaranku", "cek riwayat pelanggaranmu di grup"),
                BotCommand("govip",         "info cara daftar jadi VIP member"),
            ],
            scope=BotCommandScopeAllPrivateChats(),
        )

        print(
            "✅ Bot commands berhasil diset:\n"
            "   • Grup member  (BotCommandScopeAllGroupChats)         — 5 perintah\n"
            "   • Grup admin   (BotCommandScopeAllChatAdministrators) — 25 perintah\n"
            "   • DM baseline  (BotCommandScopeAllPrivateChats)       — 3 perintah\n"
            "   • DM per-role  (BotCommandScopeChat via /start DM)    — dikelola dm_menu.py"
        )
    except Exception as e:
        print(f"⚠️  Gagal set bot commands: {e}")


# ── Resolve Channel Peer ──────────────────────────────────────────────────────
async def _resolve_channel_peer(client):
    """
    Resolve CHANNEL_OWNER, LOG_CHANNEL, dan LOG_QRIS dari .env ke Telegram peer.

    Strategi resolve per channel (urutan prioritas):
      1. Integer ID langsung — berhasil jika access hash sudah ada di session
      2. Invite link dari DB  — berhasil di sesi baru tanpa access hash
      3. Generate invite link baru via export_chat_invite_link() — butuh bot
         sudah jadi admin dengan izin "Invite Users", lalu simpan ke DB

    Invite link disimpan permanen di DB dan dipakai ulang setiap restart.
    Hanya di-generate ulang jika channel_id di env berubah (db_key berisi
    channel_id yang mana invite link itu milik — deteksi otomatis).

    Dipanggil sekali setelah app.start() di main().
    """
    from database import save_bot_config, get_bot_config

    async def _resolve_one(env_key: str, ch_id: int) -> "object | None":
        """
        Resolve satu channel. Return Chat object jika berhasil, None jika gagal.
        Side-effect: simpan/update invite link dan info channel ke DB.
        """
        db_key_link    = f"{env_key.lower()}_invite_link"
        db_key_link_id = f"{env_key.lower()}_invite_link_for_id"  # channel_id pemilik link

        # ── 1. Coba integer ID langsung ──────────────────────────────────────
        try:
            ch = await client.get_chat(ch_id)
            print(f"[Startup] ✅ {env_key} ({ch_id}) di-resolve via integer ID.")
            return ch
        except Exception:
            pass

        # ── 1b. Coba @username dari DB ───────────────────────────────────────
        # Sesi baru setelah redeploy sering gagal resolve integer ID channel
        # karena access hash belum ada. @username tidak butuh access hash,
        # jadi ini jalur paling andal untuk LOG_CHANNEL dan LOG_QRIS.
        try:
            _saved_uname = await get_bot_config(f"{env_key.lower()}_username")
            if _saved_uname:
                ch = await client.get_chat(f"@{_saved_uname.lstrip('@')}")
                print(f"[Startup] ✅ {env_key} ({ch_id}) di-resolve via @username dari DB.")
                return ch
        except Exception:
            pass

        # ── 2. Coba invite link dari DB ──────────────────────────────────────
        saved_link    = await get_bot_config(db_key_link)
        saved_link_id = await get_bot_config(db_key_link_id)

        # Invalidasi link lama jika channel_id di env sudah berubah
        if saved_link and saved_link_id and int(saved_link_id) != ch_id:
            print(
                f"[Startup] ℹ️  {env_key}: channel_id berubah "
                f"({saved_link_id} → {ch_id}), invite link lama diabaikan."
            )
            saved_link = None

        if saved_link:
            try:
                ch = await client.get_chat(saved_link)
                print(f"[Startup] ✅ {env_key} ({ch_id}) di-resolve via invite link dari DB.")
                return ch
            except Exception as _e:
                print(f"[Startup] ⚠️  {env_key}: invite link dari DB gagal ({_e}), coba generate baru.")

        # ── 3. Generate invite link baru ─────────────────────────────────────
        # Butuh bot sudah jadi admin dengan izin "Invite Users" di channel.
        try:
            link = await client.export_chat_invite_link(ch_id)
            if link:
                await save_bot_config(db_key_link,    link)
                await save_bot_config(db_key_link_id, str(ch_id))
                ch = await client.get_chat(link)
                print(
                    f"[Startup] ✅ {env_key} ({ch_id}) di-resolve via invite link baru "
                    f"(disimpan ke DB)."
                )
                return ch
        except Exception as _e2:
            print(
                f"[Startup] ⚠️  {env_key} ({ch_id}): semua metode resolve gagal. "
                f"Pastikan bot admin dengan izin 'Invite Users'. Error: {_e2}"
            )
        return None

    # ── Resolve ketiga channel ────────────────────────────────────────────────
    for _env_key in ("LOG_CHANNEL", "LOG_QRIS", "CHANNEL_OWNER"):
        try:
            _ch_id = int(os.environ.get(_env_key, 0))
            if not _ch_id:
                continue
            ch = await _resolve_one(_env_key, _ch_id)
            if ch is None:
                continue

            # Simpan info channel ke DB (dipakai rewarm & tampilan /start)
            _title    = getattr(ch, "title", "") or ""
            _username = getattr(ch, "username", None) or ""
            await save_bot_config(f"{_env_key.lower()}_id",    _ch_id)
            await save_bot_config(f"{_env_key.lower()}_title",  _title)
            if _username:
                await save_bot_config(f"{_env_key.lower()}_username", _username)

            # Kompatibilitas mundur: CHANNEL_OWNER masih simpan key lama juga
            if _env_key == "CHANNEL_OWNER":
                await save_bot_config("channel_owner_id",       _ch_id)
                await save_bot_config("channel_owner_title",    _title)
                await save_bot_config("channel_owner_username", _username)
                label = f"@{_username}" if _username else f"(no username, id={_ch_id})"
                print(f"[Startup] ✅ CHANNEL_OWNER '{_title}' {label} berhasil di-cache ke DB.")

        except Exception as _outer_e:
            print(f"[Startup] ⚠️  {_env_key}: error tak terduga: {_outer_e}")


# ── Graceful Shutdown ─────────────────────────────────────────────────────────
async def _notify_owner():
    """Kirim notif ke owner lalu return. Dibatasi timeout 8 detik."""
    if not OWNER_ID:
        return
    try:
        await asyncio.wait_for(
            app.send_message(OWNER_ID, "⚠️ Bot offline — shutdown/maintenance."),
            timeout=8.0,
        )
        print("📢 Notifikasi shutdown terkirim ke owner.")
    except Exception as e:
        print(f"[Shutdown] Gagal kirim notif owner: {e}")


async def graceful_shutdown():
    """
    Tutup bot dengan bersih. Urutan:
      1. Simpan session terbaru ke MongoDB (peer cache yang ditemui sejak start
         ikut terbawa — PALING PENTING, harus sebelum app.stop()/close_db())
      2. Kirim notif ke owner (timeout 8 detik)
      3. Cancel semua background task
      4. Tutup koneksi database
      5. Stop Pyrogram (timeout 5 detik)
    """
    print("\n🛑 Memulai prosedur shutdown...")

    # CATATAN: lock PID lokal (_release_session_lock) SENGAJA TIDAK dilepas
    # di sini di awal. Melepasnya sebelum app.stop() benar-benar memutus
    # koneksi Telegram membuka celah singkat di mana proses baru bisa lolos
    # cek lock lalu login sementara proses lama MASIH terhubung —persis
    # skenario AUTH_KEY_DUPLICATED yang mau dicegah. Lock baru dilepas di
    # akhir fungsi ini, SETELAH app.stop().

    # ── Tulis flag 'released' ke MongoDB SEKARANG JUGA ───────────────────────
    # Harus dilakukan PERTAMA sebelum apapun — termasuk sebelum simpan session.
    # Tujuan: instance baru yang sedang menunggu (poll 1 detik) langsung tahu
    # instance ini sudah siap dilepas dan bisa lanjut app.start().
    # Jika ini ditunda sampai setelah simpan session/stop pyrogram,
    # instance baru akan timeout 30 detik karena Railway kill container
    # lebih cepat dari proses shutdown selesai.
    try:
        import json as _json, time as _time
        _released = _json.dumps({"state": "released", "by": _DEPLOY_ID, "ts": _time.time()})
        await save_bot_config(_DEPLOY_FLAG_KEY, _released)
        print("[Deploy] 🔓 Flag 'released' ditulis — instance baru boleh start.")
    except Exception as _e:
        print(f"[Deploy] ⚠️  Gagal tulis flag released: {_e}")

    # Simpan dulu sebelum apapun lain — ini yang mencegah peer cache (CHANNEL_OWNER,
    # grup, dll yang ditemui selama bot berjalan) hilang saat Railway redeploy.
    # Tanpa ini, MongoDB hanya punya snapshot session terakhir kali backup periodik
    # 20-menit jalan, sehingga peer baru yang ditemui setelah itu selalu hilang
    # tiap kali container di-restart/redeploy.
    try:
        await _save_session_to_mongo()
    except Exception as e:
        print(f"[Shutdown] ⚠️  Gagal simpan session sebelum shutdown: {e}")

    # Backup juga session semua bot pemantau (monitor) yang aktif — sama alasannya:
    # mencegah peer cache per-grup hilang setiap kali container di-redeploy.
    try:
        from monitor_bot_reference import save_all_sessions
        await save_all_sessions()
    except Exception as e:
        print(f"[Shutdown] ⚠️  Gagal simpan session monitor: {e}")

    # v7.3 — Flush NEXUS AI model (Bayes + PatternMemory) sebelum mati.
    # Jaring pengaman KEDUA: sumber utama sudah save tiap event belajar
    # (groq_queue.py, bridge.py), tapi kalau salah satu
    # save itu sempat gagal (mis. hiccup koneksi Mongo sesaat), ini kesempatan
    # terakhir nyimpen state yang masih nyangkut di RAM sebelum SIGTERM
    # beneran mematikan proses.
    try:
        from nexus.ai_core.bridge import get_nexus_ai
        _ai = get_nexus_ai()
        if _ai._loaded:
            await _ai.save()
            print("[Shutdown] 💾 NEXUS AI model (Bayes + PatternMemory) disimpan.")
    except Exception as e:
        print(f"[Shutdown] ⚠️  Gagal simpan NEXUS AI model sebelum shutdown: {e}")

    # FIX (bug: sesi userbot lama tidak terbaca saat redeploy): backup juga
    # session userbot (Security OS) ke MongoDB. Sebelumnya graceful_shutdown()
    # (dipanggil dari SIGTERM handler — jalur redeploy Railway yang sebenarnya)
    # tidak pernah menyentuh session userbot sama sekali; stop_userbot() hanya
    # dipanggil di finally block main(), yang TIDAK TENTU tereksekusi saat
    # proses dimatikan paksa lewat SIGTERM. Tanpa baris ini, peer cache userbot
    # (termasuk login yang baru saja berhasil) hilang setiap redeploy.
    try:
        from video_call import stop_userbot as _stop_ub
        await _stop_ub()
    except Exception as e:
        print(f"[Shutdown] ⚠️  Gagal simpan/stop session userbot: {e}")

    # Bengkel (mode lama, standalone): putus semua koneksi token backup.
    # Tidak ada session penting yang perlu dibackup di sini — token backup
    # tidak menyimpan peer cache grup manapun (stateless, hanya dipakai
    # sesaat untuk GetFullUser).
    try:
        from core.workshop_pool import workshop_pool
        await workshop_pool.stop_all()
    except Exception as e:
        print(f"[Shutdown] ⚠️  Gagal stop Bengkel: {e}")

    # Bengkel Join Pool (mode baru): BERBEDA dari di atas — Bengkel di sini
    # mungkin sedang JADI MEMBER di suatu grup. stop_all() akan leave_chat()
    # dengan rapi dulu sebelum disconnect, supaya tidak ada Bengkel yang
    # "nyangkut" sebagai member grup setelah proses berhenti/redeploy.
    try:
        from core.workshop_join_pool import workshop_join_pool
        await workshop_join_pool.stop_all()
    except Exception as e:
        print(f"[Shutdown] ⚠️  Gagal stop Bengkel Join: {e}")

    await _notify_owner()

    # FIX: drain antrian kerja SEBELUM cancel task — agar item yang masih
    # in-flight (hapus pesan, tulis panel, aksi moderasi) tidak hilang saat
    # container dimatikan SIGTERM. Timeout 5 detik per queue agar shutdown
    # tidak menggantung terlalu lama jika queue sangat penuh.
    try:
        from core.moderation_queue import moderation_queue as _mod_q
        from database import delete_queue as _del_q, panel_write_queue as _pw_q
        for _q, _qname in (
            (_del_q,  "delete_queue"),
            (_pw_q,   "panel_write_queue"),
            (_mod_q,  "moderation_queue"),
        ):
            try:
                await asyncio.wait_for(_q.join(), timeout=5.0)
            except asyncio.TimeoutError:
                print(f"[Shutdown] ⚠️ {_qname} drain timeout 5s — lanjut.")
            except Exception as _qe:
                print(f"[Shutdown] ⚠️ {_qname} drain error: {_qe}")
        print("[Shutdown] ✅ Queue berhasil di-drain.")
    except Exception as _e:
        print(f"[Shutdown] ⚠️ Gagal drain queue: {_e}")

    current = asyncio.current_task()
    tasks   = [t for t in asyncio.all_tasks() if t is not current]
    if tasks:
        print(f"🔄 Membatalkan {len(tasks)} background task...")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        print("✅ Semua task dibatalkan.")

    await close_db()

    try:
        if app.is_connected:
            await asyncio.wait_for(app.stop(), timeout=5.0)
            print("✅ Koneksi Telegram berhasil diputus.")
    except asyncio.TimeoutError:
        print("⚠️  app.stop() timeout — paksa keluar.")
    except Exception as e:
        print(f"[Shutdown] app.stop error (diabaikan): {e}")

    # Lock PID lokal dilepas DI SINI (bukan di awal fungsi) — setelah
    # app.stop() memastikan koneksi Telegram sudah benar-benar putus, supaya
    # tidak ada celah di mana proses baru sempat login sementara proses lama
    # ini masih terhubung.
    _release_session_lock()

    print("🛑 Bot berhasil dimatikan dengan bersih.")


# ── Main ──────────────────────────────────────────────────────────────────────
def _spawn(coro, name: str, *, delay: float = 0.0) -> asyncio.Task:
    """Pengganti asyncio.create_task() langsung untuk SEMUA task background
    yang dibuat di main() — dua masalah stabilitas yang diperbaiki sekaligus:

    1. STAGGER (delay opsional, detik):
       Sebelumnya PULUHAN task background (worker antrian, loop scan, DAN
       beberapa login Client Pyrogram terpisah — userbot Security OS, Custom
       Userbot, Promo Userbot, bot pemantau) semua di-create_task() nyaris
       BERSAMAAN, persis di detik-detik pertama setelah app.start() bot utama
       baru saja konek. Koneksi yang masih "baru dingin" jauh lebih rentan
       kalau langsung dibanjiri banyak invoke/koneksi sekaligus — inilah pola
       klasik penyebab banjir log "socket.send() raised exception." bertubi-
       tubi di awal startup. `delay` di sini menyebar titik mulai tiap task
       supaya tidak semua menembak Telegram di waktu yang sama persis.

    2. LOGGING EKSPLISIT SAAT CRASH:
       asyncio.create_task() polos artinya kalau coroutine di dalamnya raise
       exception dan tidak pernah di-await/di-cek hasilnya, exception itu
       cuma nongol di log asyncio sebagai "Task exception was never
       retrieved" generik — TANPA tahu task mana yang crash. Wrapper ini
       menangkap & mencetak exception dengan nama task yang jelas, supaya
       loop mana yang mati bisa langsung ketahuan dari log.
    """
    async def _runner():
        if delay > 0:
            await asyncio.sleep(delay)
        try:
            await coro
        except asyncio.CancelledError:
            raise
        except Exception as e:
            import traceback
            print(f"[Startup] ❌ Task '{name}' berhenti karena exception: {type(e).__name__}: {e}", flush=True)
            traceback.print_exc()

    return asyncio.create_task(_runner(), name=name)


async def main():
    global app

    # Banner startup — tampil sebelum apapun
    _print_startup_banner()

    # Lock instance lokal — cegah 2 proses jalan bersamaan dengan session yang
    # sama (root cause AUTH_KEY_DUPLICATED di panel non-Railway). Lihat
    # komentar _acquire_session_lock() di atas.
    #
    # CATATAN untuk Railway (deploy handshake via Mongo di bawah, lihat
    # _deploy_signal_new): redeploy Railway menjalankan instance BARU di
    # CONTAINER/FILESYSTEM TERPISAH dari instance lama, jadi file lock lokal
    # ini tidak pernah dilihat bersama oleh keduanya — tidak mengganggu alur
    # handshake Mongo (yang memang sengaja membiarkan instance lama tetap
    # hidup sampai di-SIGTERM Railway). Lock ini murni jaring pengaman untuk
    # kasus 1 mesin/panel yang sama menjalankan 2 proses (mis. klik Restart
    # sebelum proses lama benar-benar berhenti) — skenario di screenshot log.
    _acquire_session_lock()

    # Health check thread (daemon)
    threading.Thread(target=run_health_check, daemon=True).start()

    # Setup database (auto-pilih MongoDB atau SQLite)
    await setup_db()

    # ── Bengkel: login semua token backup GetFullUser di background ─────────
    # TIDAK di-await — login N token backup tidak boleh menunda app.start()
    # bot utama. Kalau Bengkel belum siap saat request pertama datang,
    # check_and_save() akan lazy-start sendiri (lihat workshop_pool.py).
    try:
        from core.workshop_pool import workshop_pool
        if workshop_pool.size > 0:
            _spawn(workshop_pool.start_all(), "workshop_pool.start_all")
            print(f"[Workshop] 🔧 {workshop_pool.size} token backup terdeteksi, login di background...")
    except Exception as e:
        print(f"[Workshop] Gagal inisialisasi pool: {e}")

    # ── Deploy Handshake ──────────────────────────────────────────────────────
    # Sinyal ke instance lama bahwa deploy baru siap — tunggu sampai instance lama
    # disconnect dari Telegram (maks 30 detik) agar tidak terjadi AuthKeyDuplicated.
    await _deploy_signal_new()

    # Pulihkan session dari MongoDB jika file lokal tidak ada (misal setelah Railway redeploy)
    await _restore_session_from_mongo()

    # Bangun Client
    app = await _build_client()

    # Admin session cleanup — hapus sesi kedaluwarsa setiap 10 menit
    _spawn(_adm_cleanup(), "admin_session_cleanup")

    # RAM cache janitor (FIX MEMORY LEAK) — bersihkan _local_flood_cache,
    # _global_text_tracker, _global_text_blacklist (plugins/filters/antispam.py)
    # dan _vip_free_cache (core/antispam_queue.py) dari entry basi setiap 5
    # menit. Sebelumnya cache-cache ini menumpuk selamanya sejak proses start
    # tanpa pernah dibersihkan — RAM naik terus proporsional ke jumlah user
    # unik yang pernah kirim pesan, bukan ke traffic real-time.
    try:
        from plugins.filters.antispam import start_ram_cache_janitor
        _spawn(start_ram_cache_janitor(), "ram_cache_janitor")
        print("[Janitor] 🧹 RAM cache janitor aktif (sweep tiap 5 menit).")
    except Exception as e:
        print(f"[Janitor] ⚠️  Gagal start RAM cache janitor: {e}")

    # Deploy watcher — deteksi jika ada deploy baru selama bot berjalan → auto shutdown
    _spawn(_deploy_watch_and_release(), "deploy_watch_and_release")

    # Nexus midnight scheduler dinonaktifkan — passive collection (auto-tulis corpus
    # dari setiap pesan spam yang dihapus) sudah tidak dipakai. Regenerasi regex
    # dari corpus yang tidak diisi lagi tidak ada gunanya.
    # Corpus tetap bisa diisi via /spam (laporan manual admin grup).

    # Jalankan bot
    # Tag "MainBot" SEBELUM start() — task internal Pyrogram (ping/receive
    # loop, dsb) yang di-spawn DI DALAM app.start() mewarisi context ini,
    # jadi log generik macam "socket.send() raised exception." nanti
    # kebawa tag [MainBot] otomatis (lihat core/client_log_tag.py).
    from core.client_log_tag import set_client_tag
    set_client_tag("MainBot")
    try:
        await app.start()
    except Exception as _start_err:
        # Jika session yang dipulihkan dari MongoDB ditolak Telegram → hapus dan login fresh
        if "AUTH_KEY_DUPLICATED" in str(_start_err) or "AUTH_KEY_UNREGISTERED" in str(_start_err):
            print(f"[Session] ⚠️  Session dari MongoDB tidak valid ({type(_start_err).__name__}), hapus dan login ulang...")
            import os as _os
            session_path = _SESSION_NAME + ".session"
            if _os.path.exists(session_path):
                _os.remove(session_path)
            await _clear_session_from_mongo()
            # Buat client baru tanpa session lama
            app = Client(
                _SESSION_NAME,
                api_id=API_ID,
                api_hash=API_HASH,
                bot_token=BOT_TOKEN,
                plugins=dict(root="plugins"),
                device_model=_DEVICE_MODEL,
                system_version=_SYSTEM_VERSION,
                app_version=_APP_VERSION,
            )
            await app.start()
        else:
            raise

    # ── Watchdog koneksi TERSTRUKTUR — didaftarkan PALING DULU, SEBELUM task
    # background apapun lain dibuat (fix "socket.send() raised exception."
    # bertubi-tubi tanpa pernah pulih sendiri saat sinyal sempat putus).
    # Urutan ini SENGAJA: kalau watchdog baru didaftarkan belakangan (setelah
    # puluhan task lain sudah menembak Telegram), jendela waktu sebelum
    # watchdog aktif jadi buta terhadap kegagalan yang muncul di detik-detik
    # paling awal justru saat traffic startup paling padat. Lihat
    # core/network_watchdog.py untuk detail 2 lapis SOFT/HARD recovery.
    try:
        from core.network_watchdog import register as _wd_register
        _wd_register("MainBot", app, hard_exit=True)
    except Exception as e:
        print(f"[NetWatchdog] ⚠️ Gagal daftarkan watchdog bot utama (non-fatal): {e}")

    # Background task delete_worker dijalankan SETELAH app.start() agar client
    # sudah terkoneksi saat worker pertama kali mencoba menghapus pesan.
    _spawn(delete_worker(app), "delete_worker")

    # ── Pemulihan registry grup (config_db) — lapis 1: dari dialog USERBOT ──
    # (bukan bot utama — akun bot tidak punya dialog list di sisi Telegram
    # sama sekali, lihat docstring bootstrap_groups_from_dialogs). Hanya
    # menjangkau grup yang juga punya userbot Security OS; grup lain pulih
    # sendiri lewat lapis 2 (ensure_group_registered, dari lalu-lintas
    # pesan grup — lihat plugins/filters/antispam.py).
    # Ditunda beberapa detik supaya userbot (kalau ada) sempat online dulu.
    try:
        from database import bootstrap_groups_from_dialogs

        async def _delayed_bootstrap():
            await asyncio.sleep(15)
            await bootstrap_groups_from_dialogs()

        _spawn(_delayed_bootstrap(), "bootstrap_groups_from_dialogs")
    except Exception as e:
        print(f"[Bootstrap] Gagal jadwalkan pemulihan registry grup: {e}")

    # ── Bengkel Join Pool: bot bengkel yang bisa masuk/keluar grup ───────────
    # Login token backup (sama dengan workshop_pool di atas, tapi role
    # berbeda — masuk grup sebagai member, bukan standalone) DAN bootstrap
    # resolvability ke bot utama (kirim 1x DM). Harus setelah app.start()
    # karena perlu app.get_me() (untuk DM bootstrap) dan nanti
    # add_chat_members() butuh app yang sudah terkoneksi.
    try:
        from core.workshop_join_pool import workshop_join_pool
        if workshop_join_pool.size > 0:
            _spawn(workshop_join_pool.start_all(app), "workshop_join_pool.start_all", delay=6.0)
            print(f"[BengkelJoin] 🔧 {workshop_join_pool.size} Bengkel join terdeteksi, login + bootstrap di background...")
    except Exception as e:
        print(f"[BengkelJoin] Gagal inisialisasi pool: {e}")

    # Background task moderation_worker_loop — eksekusi mute/unmute/ban satu
    # per satu dengan jeda kecil antar aksi, agar tidak ada banyak aksi
    # moderasi ditembak bersamaan ke Telegram API saat raid terjadi.
    from core.moderation_queue import moderation_worker_loop
    _spawn(moderation_worker_loop(app), "moderation_worker_loop", delay=0.5)

    # Background task fuzzy_batch_worker_loop — TAHAP 2 training Groq (lihat
    # core/groq_queue.py), diproses BATCH (maks FUZZY_BATCH_SIZE kalimat per
    # 1 panggilan API), jauh lebih hemat token dibanding satu per satu.
    from core.groq_queue import fuzzy_batch_worker_loop
    _spawn(fuzzy_batch_worker_loop(), "fuzzy_batch_worker_loop", delay=1.0)

    # Background task spam_claim_worker_loop — TAHAP 1→2 training Groq
    # (v7.0, lihat core/spam_claim_queue.py). Semua gate antispam KECUALI
    # AI Manual (Gate E) mengklaim teks spam ke sini (dedupe global + TTL 1
    # bulan); worker ini yang generate varian koreksi typo lalu masukkan ke
    # fuzzy_batch_worker_loop() di atas.
    from core.spam_claim_queue import spam_claim_worker_loop
    _spawn(spam_claim_worker_loop(), "spam_claim_worker_loop", delay=1.5)
    try:
        from nexus.ai_core.fuzzy_normalizer import ensure_vocab_ready
        ensure_vocab_ready()
    except Exception:
        pass

    # Background task mention_pending_resolve_loop — resolusi pelan-pelan
    # username @mention yang sengaja ditunda oleh Gate E (single-flight
    # in-flight & mention ke-2/dst di pesan multi-mention), HANYA saat
    # antrian antispam sepi. Lihat database.py::mention_pending_resolve_loop.
    _spawn(mention_pending_resolve_loop(app), "mention_pending_resolve_loop", delay=2.0)

    # Background task mention_bio_scan_loop — bagian dari toggle "Batasi Tag
    # Akun Promosi" (cfg["mention_batasi_akun"], per-grup opt-in). Jalan
    # independen dari loop di atas, pakai bengkel (workshop_pool)
    # untuk fetch bio + resolve kandidat link/@username. Lihat
    # core/mention_bio_scan.py untuk alur lengkapnya.
    from core.mention_bio_scan import mention_bio_scan_loop
    _spawn(mention_bio_scan_loop(), "mention_bio_scan_loop", delay=2.5)

    # Background task vc_bio_link_scan_loop — klasifikasi jenis link/@username
    # di bio untuk Inspeksi Bio Link Security OS (userbot bawaan & Custom
    # Userbot). Pola sama dengan mention_bio_scan_loop di atas (Bengkel utk
    # resolve_chat_type), tapi cache di-key per KANDIDAT bukan per profil.
    # Lihat core/vc_bio_link_scan.py & security_os/video_call.py untuk alur.
    from core.vc_bio_link_scan import vc_bio_link_scan_loop
    _spawn(vc_bio_link_scan_loop(), "vc_bio_link_scan_loop", delay=3.0)

    # Background task perm_watchdog_loop — cek berkala & bergilir kuasa
    # ban/mute bot di setiap grup. Jika hilang → paksa OFF local/global/cas
    # di database (bukan cache sesaat) agar worker hapus pesan & ban benar-
    # benar berhenti untuk grup itu sampai izin dikembalikan. Jika grup
    # sudah tidak ditemukan (dikick/dihapus) → blokir & hapus dari DB.
    from core.perm_watchdog import perm_watchdog_loop
    _spawn(perm_watchdog_loop(app), "perm_watchdog_loop", delay=3.5)

    # Background task admin_roster_reconcile_loop — jaring pengaman harian
    # untuk group_admin_roster (dipakai userbot Security OS, lihat
    # database.py bagian "ADMIN ROSTER"). Update reaktif via
    # on_chat_member_updated (nexus_group.py) sudah menutup sebagian besar
    # kasus, tapi event yang ter-skip saat bot restart/down tidak pernah
    # di-backfill Telegram — loop ini re-scan penuh 1x/24 jam sebagai
    # cadangan supaya roster tidak basi selamanya.
    from database import admin_roster_reconcile_loop
    _spawn(admin_roster_reconcile_loop(app), "admin_roster_reconcile_loop", delay=4.0)

    # Background task panel_write_worker — menulis ke DB hasil tombol panel
    # (toggle, +/-, dsb) secara antri. Client diteruskan agar worker bisa
    # mengoreksi tampilan panel di DM admin jika penulisan gagal permanen.
    _spawn(panel_write_worker(app), "panel_write_worker", delay=0.2)

    try:
        # Tandai instance ini sebagai aktif di MongoDB
        try:
            await _deploy_mark_active()
        except Exception as e:
            print(f"[Startup] ⚠️  _deploy_mark_active gagal (dilanjutkan): {e}")

        # Simpan session lokal ke MongoDB setelah login berhasil
        try:
            await _save_session_to_mongo()
        except Exception as e:
            print(f"[Startup] ⚠️  _save_session_to_mongo gagal (dilanjutkan): {e}")

        try:
            await _setup_commands()
        except Exception as e:
            print(f"[Startup] ⚠️  _setup_commands gagal (dilanjutkan): {e}")

        # Refresh menu DM owner otomatis tiap startup/redeploy — sebelumnya
        # CMDS_OWNER (core/dm_menu.py) cuma ke-set ulang saat owner /start di
        # DM, jadi command baru (mis. bangrup, unbangrup, listbangrup,
        # teksbangrup) baru muncul di menu kalau owner /start manual dulu.
        # Dengan panggilan ini, BotCommandScopeChat(OWNER_ID) langsung
        # disinkronkan tiap kali bot hidup, tanpa perlu aksi manual owner.
        try:
            if OWNER_ID:
                from core.dm_menu import set_dm_menu_for_user
                await set_dm_menu_for_user(app, OWNER_ID)
                print(f"[Startup] ✅ Menu DM owner ({OWNER_ID}) di-refresh otomatis.")
        except Exception as e:
            print(f"[Startup] ⚠️  Gagal refresh menu DM owner (dilanjutkan): {e}")

        # Resolve CHANNEL_OWNER peer → simpan ke DB agar dikenal sesi baru
        try:
            await _resolve_channel_peer(app)
        except Exception as e:
            print(f"[Startup] ⚠️  _resolve_channel_peer gagal (dilanjutkan): {e}")

        # Daftarkan handler pasif untuk mencatat user yang DM bot ke dm_users
        # (dipakai /cast untuk broadcast)
        try:
            register_peer_collector(app)
        except Exception as e:
            print(f"[Startup] ⚠️  register_peer_collector gagal (dilanjutkan): {e}")

        # Pastikan index mention_global_cache ada
        try:
            await ensure_mention_global_index()
        except Exception as e:
            print(f"[Startup] ⚠️  ensure_mention_global_index gagal (dilanjutkan): {e}")

        # Pastikan index mention_pending_resolve ada ("database khusus"
        # username @mention yang resolusinya ditunda — lihat database.py)
        try:
            await ensure_mention_pending_index()
        except Exception as e:
            print(f"[Startup] ⚠️  ensure_mention_pending_index gagal (dilanjutkan): {e}")

        # Pastikan index mention_bio_scan ada (ekstensi Anti Mention — "Cek
        # Bio Promosi Grup", lihat core/mention_bio_scan.py)
        try:
            await ensure_mention_bio_scan_index()
        except Exception as e:
            print(f"[Startup] ⚠️  ensure_mention_bio_scan_index gagal (dilanjutkan): {e}")

        # Pastikan index vc_bio_link ada (Inspeksi Bio Link Security OS —
        # klasifikasi jenis link, lihat core/vc_bio_link_scan.py)
        try:
            from database import ensure_vc_bio_link_index
            await ensure_vc_bio_link_index()
        except Exception as e:
            print(f"[Startup] ⚠️  ensure_vc_bio_link_index gagal (dilanjutkan): {e}")

        # Pastikan index spam_claim_queue ada (TTL 1 bulan + index generated,
        # lihat core/spam_claim_queue.py & database.py::ensure_spam_claim_index)
        try:
            from database import ensure_spam_claim_index
            await ensure_spam_claim_index()
        except Exception as e:
            print(f"[Startup] ⚠️  ensure_spam_claim_index gagal (dilanjutkan): {e}")

        # Muat cache in-memory kata kustom kategori (panel "Kategori Kata",
        # lihat nexus/ai_core/category_detector.py::reload_custom_words) —
        # CategoryDetector.detect() sinkron jadi tidak bisa query Mongo
        # langsung tiap pesan, harus di-prime di startup.
        try:
            from nexus.ai_core.category_detector import reload_custom_words
            await reload_custom_words()
        except Exception as e:
            print(f"[Startup] ⚠️  reload_custom_words (Kategori Kata) gagal (dilanjutkan): {e}")

        # Sinkron kata DEFAULT kategori (CATWORD_DEFAULTS, 11 kategori) ke
        # Bayes — sekali seumur hidup model (flag ai.defaults_synced),
        # lihat plugins/nexus/nexus_handlers.py::sync_category_defaults_to_bayes.
        try:
            from plugins.nexus.nexus_handlers import sync_category_defaults_to_bayes
            await sync_category_defaults_to_bayes()
        except Exception as e:
            print(f"[Startup] ⚠️  sync_category_defaults_to_bayes gagal (dilanjutkan): {e}")

        # Isi ulang peer cache dari semua grup/channel yang dikenal di DB
        # → mencegah PeerIdInvalid setelah Railway redeploy (filesystem bersih)
        try:
            await _rewarm_known_peers(app)
        except Exception as e:
            print(f"[Startup] ⚠️  _rewarm_known_peers gagal (dilanjutkan): {e}")

        # Backup session ke MongoDB setiap 20 menit
        # → peer baru yang ditemui saat bot berjalan ikut tersimpan
        try:
            _spawn(_periodic_session_backup(), "periodic_session_backup", delay=10.0)
        except Exception as e:
            print(f"[Startup] ⚠️  Gagal create_task _periodic_session_backup: {e}")

        # ── Userbot Security OS ───────────────────────────────────────────────
        # Dijalankan SETELAH bot biasa start & siap agar OTP bisa dikirim ke owner.
        # start_userbot tidak blocking — ia menjalankan task sendiri di background.
        #
        # FIX (disederhanakan kembali ke pola versi lama yang terbukti selalu
        # bekerja): sebelumnya ada wrapper _run_start_userbot_safely() di sekitar
        # create_task ini. Wrapper itu seharusnya setara secara fungsional, tapi
        # untuk menyingkirkan kemungkinan ada interaksi tak terduga, baris ini
        # dikembalikan ke bentuk paling sederhana — create_task langsung pada
        # start_userbot(app), identik dengan versi yang sudah terbukti membuat
        # userbot selalu aktif sebelumnya. start_userbot() sendiri SUDAH
        # membungkus setiap langkah internalnya dengan try/except masing-masing
        # (lihat video_call.py) sehingga tidak butuh wrapper tambahan di sini.
        #
        # Print log EKSPLISIT ditambahkan tepat sebelum & sesudah create_task
        # ini — jika suatu saat baris "[UB] ▶️" di video_call.py tidak pernah
        # muncul di log lagi, baris print di bawah ini akan menunjukkan dengan
        # pasti apakah create_task ini sendiri tercapai atau tidak.
        # STAGGER (3s/6s/9s di bawah): userbot Security OS, Custom Userbot, dan
        # Promo Userbot masing-masing membuka Client Pyrogram TERPISAH (koneksi
        # TCP + login sendiri-sendiri). Kalau ketiganya di-create_task() di
        # detik yang sama persis dengan bot utama baru start(), semua koneksi
        # baru ini "berebut" di jendela waktu yang sama — pola inilah yang
        # paling sering memicu banjir "socket.send() raised exception." di
        # log startup. Menyebar titik mulainya beberapa detik membiarkan
        # tiap koneksi benar-benar stabil dulu sebelum yang berikutnya dibuka.
        print("[Startup] ▶️  Menjadwalkan start_userbot (delay 3s)...", flush=True)
        try:
            _spawn(start_userbot(app), "start_userbot", delay=3.0)
            print("[Startup] ✅ start_userbot dijadwalkan.", flush=True)
        except Exception as e:
            import traceback
            print(f"[UB] ❌ Gagal create_task start_userbot: {e}", flush=True)
            traceback.print_exc()

        # ── Custom Userbot — pulihkan sesi milik grup (login mandiri admin) ────
        try:
            from security_os import custom_userbot as _cub
            _spawn(_cub.resume_all(app), "custom_userbot.resume_all", delay=6.0)
            print("[Startup] ✅ custom_userbot.resume_all dijadwalkan (delay 6s).", flush=True)
        except Exception as e:
            print(f"[CustomUB] ❌ Gagal create_task resume_all: {e}", flush=True)

        # ── Promo Userbot — pulihkan sesi & loop Join All VC (redeploy-safe) ───
        try:
            from security_os import promo_userbot as _pub
            _spawn(_pub.resume_all(app), "promo_userbot.resume_all", delay=9.0)
            print("[Startup] ✅ promo_userbot.resume_all dijadwalkan (delay 9s).", flush=True)
        except Exception as e:
            print(f"[PromoUB] ❌ Gagal create_task resume_all: {e}", flush=True)

        # ── NewsCore Time-Checker Loop ────────────────────────────────────────
        try:
            from plugins.commands.newscore import newscore_checker_loop
            _spawn(newscore_checker_loop(app), "newscore_checker_loop", delay=12.0)
        except Exception as e:
            print(f"[Startup] ⚠️  newscore_checker_loop gagal dimulai: {e}")

        # ── NewsCore Bio Admin Sweep Loop (inspeksi berkala jam 03:00 WIB) ────
        try:
            from plugins.commands.newscore import newscore_bio_sweep_loop
            _spawn(newscore_bio_sweep_loop(app), "newscore_bio_sweep_loop", delay=12.5)
        except Exception as e:
            print(f"[Startup] ⚠️  newscore_bio_sweep_loop gagal dimulai: {e}")

        # ── VIP Bio Checker Loop (auto-keluar VIP saat teks hilang dari bio) ──
        try:
            from core.vip_bio_guard import vip_bio_checker_loop
            _spawn(vip_bio_checker_loop(), "vip_bio_checker_loop", delay=13.0)
        except Exception as e:
            print(f"[Startup] ⚠️  vip_bio_checker_loop gagal dimulai: {e}")

        # ── NewsCore Score Buffer Flush Worker ────────────────────────────────
        # Flush skor yang di-buffer di memory ke MongoDB secara batch,
        # setiap NS_FLUSH_INTERVAL detik (default 10 detik). Murni tulis-DB
        # (bukan Telegram API), jadi aman dijalankan lebih awal.
        try:
            from database import ns_flush_worker_loop
            _spawn(ns_flush_worker_loop(), "ns_flush_worker_loop", delay=1.0)
        except Exception as e:
            print(f"[Startup] ⚠️  ns_flush_worker_loop gagal dimulai: {e}")

        # ── LOG_CHANNEL Flush Worker ───────────────────────────────────────────
        # Flush antrian log (spam lokal/global, regex, sistem) ke LOG_CHANNEL
        # secara batch setiap LOG_FLUSH_INTERVAL detik (default 8 detik).
        # FIXED: Mencegah FloodWait menumpuk saat grup ramai — semua log
        # dikumpulkan dulu lalu dikirim sebagai 1 pesan gabungan per siklus.
        try:
            from plugins.commands.log import log_flush_worker_loop
            _spawn(log_flush_worker_loop(app), "log_flush_worker_loop", delay=1.2)
        except Exception as e:
            print(f"[Startup] ⚠️  log_flush_worker_loop gagal dimulai: {e}")

        # ── Antispam Detection — Worker Pool PER-GRUP (auto-scaling) ───────────
        # Tiap grup punya antrian & worker pool sendiri, independen dari grup
        # lain. Raid besar di 1 grup TIDAK memperlambat deteksi spam di grup
        # lain sama sekali — beda antrian, beda worker, scaling naik/turun
        # dihitung dari kondisi grup itu sendiri saja.
        #
        # start_antispam_detection_workers() cuma menjalankan 1 task
        # supervisor yang memantau semua pool grup secara berkala. Pool tiap
        # grup dibuat OTOMATIS (lazy) begitu grup itu punya pesan pertama
        # yang perlu dideteksi — baseline ANTISPAM_GROUP_WORKER_MIN (default
        # 1) worker, naik sampai ANTISPAM_GROUP_WORKER_MAX (default 5) kalau
        # grup itu sendiri kebanjiran, turun lagi kalau sepi, dan pool-nya
        # dibubarkan total kalau grup itu idle lama (hemat resource untuk
        # bot yang melayani banyak grup sekaligus).
        #
        # Kenapa perlu worker terpisah dari handler pyrogram:
        #   bio.py  → 1 bot pemantau per grup (paralel aman, API terdistribusi)
        #   antispam → 1 bot utama untuk SEMUA grup → tetap pakai antrian +
        #   pool per-grup agar burst API call (mention check, gcast query)
        #   di 1 grup tidak memblokir grup lain sama sekali.
        #
        # Koordinasi FloodWait:
        #   Walau antrian & worker sekarang per-grup, semua worker di semua
        #   grup tetap berbagi set_global_flood_backoff / wait_global_flood_
        #   backoff yang sama dengan delete_worker, moderation_worker_loop,
        #   log_flush_worker_loop → begitu 1 worker (di grup mana pun) kena
        #   FloodWait, semua worker di semua grup ikut mundur.
        # ── Antispam Detection Worker Pool ──────────────────────────────────────
        # N worker (default 3, atur via env ANTISPAM_WORKER_COUNT) yang
        # sama-sama membaca dari detection_queue yang sama (shared, bukan
        # assignment tetap per-pesan). Kalau 1 worker nyangkut lama (mis.
        # Gate E mention nunggu API Telegram), worker lain tetap ambil
        # pesan berikutnya dari queue — tidak ada ruang tunggu percuma.
        #
        # Burst API tetap terkendali karena semua worker (+ delete_worker,
        # moderation_worker_loop, log_flush_worker_loop) berbagi
        # set_global_flood_backoff / wait_global_flood_backoff yang sama —
        # begitu satu worker kena FloodWait, semuanya ikut mundur.
        # ── Upgrade Speed — muat ulang boost aktif SEBELUM worker pool mulai ───
        # Default SEMUA grup dikunci di speed minimal (lihat
        # core/antispam_queue.py::_effective_max_workers). load_active_speed_
        # boosts() WAJIB dipanggil SEBELUM start_antispam_detection_workers(),
        # supaya grup yang sedang dalam masa boost aktif TIDAK tiba-tiba
        # "terkunci" balik ke minimal cuma karena redeploy di tengah jalan —
        # lihat core/speed_boost.py untuk detail anti-miss redeploy.
        try:
            from core.speed_boost import load_active_speed_boosts, speed_boost_expiry_loop
            _n_boost = await load_active_speed_boosts()
            _spawn(speed_boost_expiry_loop(app), "speed_boost_expiry_loop", delay=13.5)
            print(f"[Startup] ✅ Upgrade Speed: {_n_boost} grup boost dimuat ulang, "
                  f"expiry watchdog aktif.", flush=True)
        except Exception as e:
            print(f"[Startup] ⚠️  Upgrade Speed (speed_boost) gagal dimulai: {e}")

        # ── Promo Userbot "💎 Upgrade Akun" — expiry watchdog akses donasi ─────
        # Tidak perlu preload seperti load_active_speed_boosts() di atas —
        # access_until akun TIDAK pernah di-cache di memory (selalu dibaca
        # live dari DB tiap kali panel/loop butuh), jadi tidak ada state
        # in-memory yang perlu "dipanaskan ulang" saat redeploy. Loop ini
        # sendiri query ulang ke DB tiap siklus, jadi expiry yang jatuh SAAT
        # bot down otomatis ketahuan begitu bot hidup lagi.
        try:
            from security_os.promo_userbot import promo_access_expiry_loop
            _spawn(promo_access_expiry_loop(app), "promo_access_expiry_loop", delay=14.0)
            print("[Startup] ✅ Promo Userbot Upgrade Akun: expiry watchdog aktif.", flush=True)
        except Exception as e:
            print(f"[Startup] ⚠️  Promo Userbot Upgrade Akun (promo_access_expiry_loop) gagal dimulai: {e}")

        # ── Promo Userbot "Hidup" di VC — obrolan otomatis dibantu Groq ──────
        # Terpisah total dari fitur VC milik userbot utama/Custom Userbot
        # (security_os/video_call.py) — lihat security_os/promo_vc_chat.py.
        # Nonaktif secara default (PROMO_VC_CHAT_ENABLED=0 di .env); modul
        # itu sendiri yang cek flag ini, jadi aman dipanggil selalu di sini.
        try:
            from security_os.promo_vc_chat import start_promo_vc_chat_loop
            start_promo_vc_chat_loop()
            print("[Startup] ✅ Promo Userbot VC Chatter: loop dimulai (cek PROMO_VC_CHAT_ENABLED di log).", flush=True)
        except Exception as e:
            print(f"[Startup] ⚠️  Promo Userbot VC Chatter (promo_vc_chat) gagal dimulai: {e}")

        # ── Force-flash berdasar admin ID tertentu — muat ulang override aktif
        # SEBELUM worker pool mulai, sama seperti Upgrade Speed di atas, supaya
        # grup yang lagi di-force-flash tidak tiba-tiba balik ke floor cuma
        # karena redeploy. Jalur ini TERPISAH total dari Upgrade Speed (lihat
        # core/admin_flash_watch.py) — no-op kalau ADMIN_FLASH_USER_ID tidak
        # diset di .env.
        try:
            from core.admin_flash_watch import load_admin_flash_overrides, is_enabled as _flash_enabled
            _n_flash = await load_admin_flash_overrides()
            if _flash_enabled():
                print(f"[Startup] ✅ Admin-Flash: {_n_flash} grup override dimuat ulang.", flush=True)
        except Exception as e:
            print(f"[Startup] ⚠️  Admin-Flash (admin_flash_watch) gagal dimulai: {e}")

        # ── Backfill trial 3-hari untuk grup LAMA (sudah terpasang bot SEBELUM
        # fitur ini rilis) — dijalankan sebagai background task supaya tidak
        # menunda startup kalau jumlah grup terdaftar banyak. Idempotent &
        # aman dipanggil tiap redeploy (grup yang sudah tercatat otomatis
        # dilewati) — lihat plugins/nexus/nexus_group.py untuk detail &
        # toggle env NEW_GROUP_TRIAL_BACKFILL_ENABLED.
        try:
            from plugins.nexus.nexus_group import backfill_new_group_trial_for_existing_groups
            _spawn(backfill_new_group_trial_for_existing_groups(app), "backfill_new_group_trial", delay=20.0)
        except Exception as e:
            print(f"[Startup] ⚠️  Backfill trial grup lama gagal dimulai: {e}")

        try:
            from core.antispam_queue import start_antispam_detection_workers
            start_antispam_detection_workers(app)
            print("[Startup] ✅ Antispam detection worker pool siap.", flush=True)
        except Exception as e:
            print(f"[Startup] ⚠️  antispam_detection_worker pool gagal dimulai: {e}")

        # ── Bot Pemantau (Monitor) — independen dari userbot ──────────────────
        # FIX: Sebelumnya _load_instances_from_db() hanya dipanggil dari dalam
        # _voice_chat_monitor_loop() di video_call.py — yang hanya berjalan jika
        # userbot berhasil start. Akibatnya, jika userbot off atau belum punya
        # session, bot pemantau yang sudah di-generate tidak pernah aktif.
        #
        # Solusi: panggil _load_instances_from_db() langsung di sini, setelah
        # bot utama siap, TANPA menunggu userbot. Bot pemantau berjalan
        # independen — mereka hanya butuh token di DB, bukan sesi userbot.
        # _voice_chat_monitor_loop() di video_call.py tetap memanggil
        # _load_instances_from_db() juga, tapi karena fungsi itu idempotent
        # (grup yang sudah ada di _active_instances dilewati), tidak ada duplikasi.
        try:
            from monitor_bot_reference import (
                _load_instances_from_db as _monitor_load,
                _periodic_session_backup as _monitor_session_backup,
                group_gate_watchdog_loop as _monitor_gate_loop,
            )
            # Jeda singkat sebelum memuat bot pemantau — memberi ruang bagi
            # login userbot Security OS/Custom/Promo (dijadwalkan 3s/6s/9s di
            # atas) untuk sempat stabil dulu sebelum menambah beban koneksi
            # baru lagi (_load_instances_from_db sendiri sudah sekuensial per
            # grup, jadi tidak menambah burst — ini murni jeda titik mulai).
            await asyncio.sleep(2)
            await _monitor_load()
            _spawn(_monitor_session_backup(), "monitor_session_backup", delay=15.0)
            _spawn(_monitor_gate_loop(), "monitor_gate_loop", delay=15.5)
            print("[Startup] ✅ Bot pemantau (monitor) dimuat dari DB — independen dari userbot.", flush=True)
            print("[Startup] ✅ Group-gate watchdog bot pemantau aktif (grup off/dihapus/kuasa dicabut otomatis distop).", flush=True)
        except Exception as e:
            print(f"[Startup] ⚠️  Gagal load bot pemantau (monitor): {e}", flush=True)

        # ── Welcome member baru — sweep loop hapus otomatis ────────────────────
        # Jalan independen dari monitor load di atas: sweep loop hanya BACA
        # _active_instances tiap siklus (lazy), jadi aman walau monitor belum
        # sempat load duluan — dia akan skip & coba lagi 5 detik berikutnya.
        try:
            from plugins.commands.welcome import welcome_delete_sweep_loop
            _spawn(welcome_delete_sweep_loop(), "welcome_delete_sweep_loop", delay=16.0)
            print("[Startup] ✅ Welcome delete sweep loop siap.", flush=True)
        except Exception as e:
            print(f"[Startup] ⚠️  Gagal start welcome_delete_sweep_loop: {e}", flush=True)

        # ── Mention Member Cache TTL Index ────────────────────────────────────
        try:
            from database import ensure_mention_cache_index
            await ensure_mention_cache_index()
        except Exception as e:
            print(f"[Startup] ⚠️  Gagal buat mention cache index: {e}", flush=True)

        # ── Deteksi Ubot TTL Index ────────────────────────────────────────────
        try:
            from core.ubot_detect import ensure_ubot_detect_index
            await ensure_ubot_detect_index()
        except Exception as e:
            print(f"[Startup] ⚠️  Gagal buat ubot_detect index: {e}", flush=True)

        print("🚀 Bot Antispam + Nexus AI aktif! Tekan Ctrl+C untuk berhenti.", flush=True)
        await idle()
    except (KeyboardInterrupt, asyncio.CancelledError):
        # graceful_shutdown mungkin sudah dipanggil via SIGTERM handler —
        # _shutdown_triggered mencegah pemanggilan ganda
        if not globals().get("_shutdown_triggered", False):
            await graceful_shutdown()
    finally:
        # Hentikan userbot dengan bersih sebelum tutup program
        try:
            await stop_userbot()
        except Exception:
            pass
        try:
            if app.is_connected:
                await app.stop()
        except Exception:
            pass
        # Jaring pengaman: pastikan lock PID lokal selalu dilepas walau
        # graceful_shutdown() tidak sempat/tidak pernah terpanggil, supaya
        # proses berikutnya tidak ditolak salah oleh lock basi.
        try:
            _release_session_lock()
        except Exception:
            pass

if __name__ == "__main__":
    import signal

    # FIX: asyncio.get_event_loop() deprecated Python 3.10+, RuntimeError di 3.12+.
    # Buat event loop baru secara eksplisit dan set sebagai loop aktif.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # ── Exception handler global — redam noise PeerIdInvalid dari Pyrogram ──
    # Bot pemantau (monitor_bot_reference.py) menjalankan banyak Client
    # Pyrogram sekaligus. Saat Telegram mengirim raw update untuk sebuah
    # channel yang BELUM dikenal sesi monitor tertentu (belum punya peer
    # cache/access_hash-nya), Client.handle_updates() internal Pyrogram
    # melempar exception (PeerIdInvalid / KeyError: ID not found) di dalam
    # task-nya sendiri — bukan di kode kita, jadi tidak tertangkap try/except
    # manapun di aplikasi. Exception ini TIDAK FATAL (peer akan dikenal
    # dengan sendirinya begitu monitor benar-benar berinteraksi dengan
    # channel itu), tapi membanjiri log sebagai "Task exception was never
    # retrieved" lengkap dengan traceback panjang.
    #
    # Handler ini meredam KHUSUS exception jenis itu (cukup 1 baris info),
    # dan tetap menampilkan traceback lengkap untuk exception lain yang
    # benar-benar perlu diperhatikan.
    def _global_exception_handler(loop, context):
        exc = context.get("exception")
        msg = context.get("message", "")
        if isinstance(exc, (KeyError, ValueError)) and (
            "Peer id invalid" in str(exc) or "ID not found" in str(exc)
        ):
            print(f"[Pyrogram] ℹ️  Peer belum dikenal sesi monitor (diabaikan, tidak fatal): {exc}")
            return
        # Exception lain yang tidak dikenali — tetap tampilkan penuh seperti default asyncio
        loop.default_exception_handler(context)

    loop.set_exception_handler(_global_exception_handler)

    # ── SIGTERM & SIGINT handler ─────────────────────────────────────────────
    # Railway (dan Docker) mengirim SIGTERM saat redeploy/stop. SIGINT (Ctrl+C,
    # dipakai saat jalanin manual di Termux/VPS) SEBELUMNYA TIDAK didaftarkan
    # eksplisit di sini — cuma mengandalkan perilaku default Python yang
    # melempar KeyboardInterrupt. Itu KADANG tidak konsisten (tergantung state
    # event loop persis saat Ctrl+C ditekan), beda dengan add_signal_handler
    # yang dijamin selalu terpanggil oleh asyncio sendiri. Sekarang SIGINT
    # didaftarkan eksplisit juga, pakai handler yang SAMA PERSIS dengan SIGTERM
    # — biar prosedur shutdown selalu konsisten dari jalur manapun dipicunya.
    _shutdown_triggered = False

    def _handle_sigterm():
        if globals().get("_shutdown_triggered", False):
            return
        globals()["_shutdown_triggered"] = True
        print("\n[Signal] SIGTERM diterima — memulai graceful shutdown...")
        # Schedule graceful_shutdown sebagai task di loop yang sedang berjalan
        loop.create_task(graceful_shutdown())

    def _handle_sigint():
        if globals().get("_shutdown_triggered", False):
            return
        globals()["_shutdown_triggered"] = True
        print("\n[Signal] SIGINT (Ctrl+C) diterima — memulai graceful shutdown...")
        loop.create_task(graceful_shutdown())

    loop.add_signal_handler(signal.SIGTERM, _handle_sigterm)
    loop.add_signal_handler(signal.SIGINT, _handle_sigint)

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        # 1. Ambil semua task yang masih menggantung/pending
        pending_tasks = asyncio.all_tasks(loop)

        # 2. Batalkan semua task tersebut
        for task in pending_tasks:
            task.cancel()

        # 3. Berikan waktu sejenak agar sistem memproses pembatalan task —
        #    TAPI DIBATASI WAKTU (timeout). Sebagian task (terutama yang
        #    menyentuh panggilan NATIVE lewat py-tgcalls/ntgcalls — bukan
        #    pure-Python asyncio) TIDAK selalu merespons task.cancel() dengan
        #    benar, karena cancel() cuma bisa "menyela" di titik await
        #    Python — kalau task itu sedang macet di panggilan native (C++)
        #    yang tidak pernah balik, cancel() efeknya nol dan
        #    asyncio.gather() di bawah bisa menggantung SELAMANYA (persis
        #    gejala "Ctrl+C ditekan tapi terminal tidak pernah balik ke $").
        #    Solusinya: batasi tunggu maksimal SHUTDOWN_TASK_WAIT_TIMEOUT
        #    detik — kalau belum semua beres di waktu itu, JANGAN tunggu
        #    lebih lama lagi, lanjut paksa keluar saja (lihat os._exit di
        #    bawah) daripada proses nyangkut selamanya butuh kill -9 manual.
        SHUTDOWN_TASK_WAIT_TIMEOUT = 8.0
        if pending_tasks:
            try:
                loop.run_until_complete(
                    asyncio.wait_for(
                        asyncio.gather(*pending_tasks, return_exceptions=True),
                        timeout=SHUTDOWN_TASK_WAIT_TIMEOUT,
                    )
                )
            except asyncio.TimeoutError:
                print(
                    f"[Shutdown] ⚠️  {len(pending_tasks)} task belum selesai dibatalkan "
                    f"setelah {SHUTDOWN_TASK_WAIT_TIMEOUT:.0f}s (kemungkinan macet di "
                    f"panggilan native py-tgcalls/ntgcalls) — dipaksa lanjut keluar, "
                    f"tidak ditunggu lebih lama lagi."
                )
            except Exception:
                pass

        # 4. Baru setelah itu tutup loop dengan aman
        try:
            loop.close()
        except Exception:
            pass

        print("🛑 Bot berhasil dimatikan dengan bersih.")

        # 5. JARING PENGAMAN TERAKHIR — force-exit proses.
        #    py-tgcalls/ntgcalls kadang menyisakan THREAD NATIVE (bukan
        #    asyncio task, jadi TIDAK ikut ke-cancel oleh langkah 1-2 di
        #    atas sama sekali) yang masih hidup di background walau loop
        #    Python sudah ditutup — ini yang bikin proses Python TIDAK
        #    benar-benar exit walau baris "Bot berhasil dimatikan dengan
        #    bersih" sudah tercetak, dan terminal tidak pernah kembali ke
        #    prompt $ (persis keluhan awal). os._exit() mematikan proses
        #    SECARA PAKSA di level OS, tidak peduli thread apa saja yang
        #    masih hidup, tidak menjalankan cleanup Python lagi (semua
        #    cleanup penting SUDAH selesai di atas) — garansi terminal
        #    selalu balik ke prompt tanpa perlu kill -9 manual lagi.
        import os as _os
        _os._exit(0)

