"""
plugins/nexus/nexus_group.py
─────────────────────────────
Handler grup untuk sistem Nexus AI:
  - /spam  (admin grup) → klaim kalimat ke antrean verifikasi Groq (TAHAP 1)
  - on_chat_member_updated → track masuk/keluar bot di grup
  - Data-layer whitelist Nexus (get_nexus_whitelist_docs) — dipakai
    core/antispam_queue.py::_gate_nexus_ai sebagai filter RAW SEBELUM
    DIPROSES AI Manual (lihat v8.1 di bawah)

v8.1 — Silent filter (dulu group=5: nexus_regex legacy + CategoryDetector
standalone + whitelist + keroyokan) DIHAPUS TOTAL. Alasan:
  1. CategoryDetector sudah jadi Layer 3 di AI Manual (Gate E, core/
     antispam_queue.py::_gate_nexus_ai → nexus_ai_auto_detect) — standalone
     check di sini murni duplikat & sumber race condition (2x hapus/log
     untuk 1 pesan yang sama: "🤖 Dihapus oleh AI Nexus" vs "🧠 AI Manual
     Menangkap Duluan").
  2. nexus_regex_db ("Pola AI Interlock") legacy — mesin pengisinya
     (midnight regeneration) sudah dinonaktifkan sejak v8.0 (lihat main.py
     & plugins/nexus/engine.py), isinya beku, tidak lagi relevan untuk
     produksi (masih bisa dilihat lewat panel 🧬 VISUALISASI FILTER &
     sandbox nexus_handlers.py::nexus_sandbox_processor, keduanya baca
     nexus_get_all_regex() langsung, tidak lewat file ini).
  3. Auto-delete di jalur lama TIDAK PERNAH memberi sinyal training ke
     Groq/AI Manual (tidak panggil claim_spam_text) — beda dari /spam
     (tetap ada, lihat nexus_spam_handler di bawah) dan Trigger AI/Regex Grup
     (Gate A, tetap ada di core/antispam_queue.py::_gate_regex) yang
     keduanya TETAP memicu claim_spam_text() untuk training. Menghapus
     jalur ini TIDAK mengubah urutan/pipeline training Groq → AI Manual
     sama sekali — normalisasi varian, generate multi-kalimat, dan verifikasi
     Groq semuanya tetap identik seperti sebelumnya.
  4. Kalau ada 1 pesan cuma memicu CategoryDetector (tanpa sinyal PatternMemory
     sama sekali), AI Manual TETAP bisa menghapusnya sendiri lewat aturan
     NEXUS_NONPATTERN_WEIGHT (default 3.0x — lihat nexus/ai_core/constants.py)
     yang melipatgandakan skor gabungan Bayes+Feature+Category saat
     PatternMemory kosong, supaya layer lain tetap "bicara".

Whitelist Nexus TETAP dipertahankan (bukan dead code) — datanya sekarang
dipakai sebagai filter RAW SEBELUM DIPROSES di AI Manual sendiri (lihat
get_nexus_whitelist_docs() di bawah), supaya proteksi false-positive yang
sudah dikurasi owner tidak hilang saat CategoryDetector standalone dicabut.

ATURAN KEROYOKAN (v2, HISTORIS): dulu berlaku hanya di silent filter lama
(2+ pola berbeda sekaligus → whitelist kalah). AI Manual tidak punya
konsep "keroyokan" terpisah — kombinasi banyak sinyal spam sekaligus
otomatis menaikkan skor gabungan lewat scoring berlapis (Bayes+Feature+
Category+PatternMemory), efeknya setara tanpa logika khusus tambahan.
"""

import os
import asyncio
from datetime import datetime, timezone, timedelta

from pyrogram import Client, filters
from pyrogram.types import Message, ChatMemberUpdated
from pyrogram.enums import ChatMemberStatus

from database import (
    nexus_track_grup,
    nexus_remove_grup,
    nexus_whitelist_get_all,
    force_disable_group_moderation,
    restore_group_moderation_if_forced,
    get_admin_roster,
    admin_roster_upsert_user,
    bootstrap_admin_roster,
    is_admin,
)

LOG_CHANNEL = int(os.environ.get("LOG_CHANNEL", 0))
TZ_WIB      = timezone(timedelta(hours=7))

# Guard: cegah bootstrap_admin_roster berjalan dobel untuk grup yang sama
# secara bersamaan (mis. event ChatMemberUpdated datang beruntun cepat).
_admin_roster_bootstrap_in_progress: set[int] = set()

from plugins.nexus.engine import pipeline_pembersihan
from core.spam_claim_queue import claim_spam_text
import admin_session as _adm_sess

# ── /spam — Admin grup lapor spam ─────────────────────────────────────────────

@Client.on_message(filters.command("spam") & (filters.group | filters.forum))
async def nexus_spam_handler(client: Client, message: Message):
    cid = message.chat.id
    uid = message.from_user.id if message.from_user else None

    from database import is_admin
    if not await is_admin(client, cid, uid):
        return
    if not message.reply_to_message:
        return

    teks_mentah = message.reply_to_message.text or message.reply_to_message.caption
    if not teks_mentah:
        return

    teks_clean = pipeline_pembersihan(teks_mentah)
    if not teks_clean:
        return

    await nexus_track_grup(cid, message.chat.title or str(cid), message.chat.username)

    # ── [FIX] /spam sekarang diperlakukan PERSIS seperti klaim gate antispam
    # lain (Trigger AI, Link Detector, Bio Filter, dll) — BUKAN langsung
    # ditulis ke nexus_kalimat_db & langsung ditrain ke AI Manual seperti
    # desain lama. Sekarang lewat claim_spam_text() → antre TAHAP 1 (dedupe
    # + persist Mongo) → TAHAP 2 (normalisasi/generate varian di
    # spam_claim_worker_loop) → antrean fuzzy batch → Groq menilai
    # spam/nospam-nya baru AI Manual ditrain (core/groq_queue.py). Urutan
    # proses di dalam antrean itu sendiri TIDAK diubah sama sekali di sini.
    asyncio.create_task(claim_spam_text(teks_clean, cid, "admin_spam_report"))

    try:
        await message.reply_to_message.delete()
        await message.delete()
        notif = await client.send_message(
            chat_id=cid,
            text=(
                f"✅ **Laporan Diterima — Nexus AI**\n"
                f"Pesan berhasil dihapus & masuk antrean verifikasi AI Groq "
                f"untuk dipastikan spam atau bukan sebelum melatih AI. ⏳"
            )
        )
        await asyncio.sleep(5)
        await notif.delete()
    except Exception as e:
        print(f"[nexus_group] spam handler error: {e}")


# ── [NEXUS AI CORE] Background helpers (fire-and-forget) ─────────────────────
# NOTE: _ai_learn_background() (train langsung dari /spam tanpa verifikasi
# Groq) sudah DIHAPUS — /spam sekarang lewat claim_spam_text() di atas,
# mengikuti antrean verifikasi Groq yang sama dengan gate antispam lain.

# ── [NEXUS AI CORE] Background helpers (fire-and-forget) ─────────────────────
# NOTE: _ai_learn_background() (train langsung dari /spam tanpa verifikasi
# Groq) sudah DIHAPUS — /spam sekarang lewat claim_spam_text() di atas,
# mengikuti antrean verifikasi Groq yang sama dengan gate antispam lain.
# NOTE (v8.0): _ai_passive_background() (passive learning per-pesan tanpa
# verifikasi Groq, termasuk auto-train ham dari SETIAP pesan yang lolos)
# juga sudah DIHAPUS bersama nexus/ai_core/passive_learner.py — semua
# training AI Manual sekarang HANYA lewat jalur terverifikasi Groq.

# ── [/NEXUS AI CORE] ─────────────────────────────────────────────────────────


# ── Cache whitelist ──────────────────────────────────────────────────────────
#
# v8.1 — nexus_silent_filter (regex Nexus + CategoryDetector standalone) DIHAPUS
# TOTAL dari sini. Alasan (lihat diskusi/keputusan terkait):
#   1. CategoryDetector sudah jadi Layer 3 di AI Manual (Gate E, core/
#      antispam_queue.py::_gate_nexus_ai → nexus_ai_auto_detect) — standalone
#      check di sini murni duplikat & sumber race condition (2x hapus/log
#      untuk 1 pesan).
#   2. nexus_regex_db ("Pola AI Interlock") legacy — mesin pengisinya
#      (midnight regeneration) sudah dinonaktifkan sejak v8.0 (lihat main.py
#      & plugins/nexus/engine.py), jadi isinya beku, tidak lagi relevan.
#   3. Auto-delete di sini TIDAK PERNAH memberi sinyal training ke Groq/AI
#      Manual (tidak panggil claim_spam_text) — beda dari /spam (tetap ada,
#      lihat nexus_spam_handler di atas) dan Trigger AI/Regex Grup (Gate A, tetap
#      ada di core/antispam_queue.py::_gate_regex) yang keduanya TETAP
#      memicu claim_spam_text() untuk training. Menghapus jalur ini TIDAK
#      mengubah urutan/pipeline training Groq → AI Manual sama sekali.
#
# Whitelist Nexus TETAP dipertahankan (bukan cuma dead code) — datanya
# sekarang dipakai sebagai filter RAW SEBELUM DIPROSES di AI Manual sendiri
# (lihat get_nexus_whitelist_docs() di bawah, dipanggil dari
# core/antispam_queue.py::_gate_nexus_ai sebelum scoring Bayes+Feature+
# Category+PatternMemory) — supaya proteksi false-positive yang sudah
# dikurasi owner tidak hilang saat CategoryDetector standalone dicabut.

_nexus_wl_cache: list[dict] = []
_nexus_wl_cache_ts: float   = 0.0

_NEXUS_REGEX_TTL = 300  # 5 menit


async def _get_whitelist_docs() -> list[dict]:
    global _nexus_wl_cache, _nexus_wl_cache_ts
    import time
    now = time.monotonic()
    if now - _nexus_wl_cache_ts < _NEXUS_REGEX_TTL:
        return _nexus_wl_cache
    _nexus_wl_cache    = await nexus_whitelist_get_all()
    _nexus_wl_cache_ts = now
    return _nexus_wl_cache


async def get_nexus_whitelist_docs() -> list[dict]:
    """API publik — dipakai core/antispam_queue.py::_gate_nexus_ai untuk cek
    whitelist pada RAW content SEBELUM AI Manual memproses/menskor pesan."""
    return await _get_whitelist_docs()


def invalidate_nexus_wl_cache():
    """
    Paksa cache whitelist kadaluarsa.
    Dipanggil dari nexus_handlers.py setelah operasi tambah/hapus/clear whitelist.
    """
    global _nexus_wl_cache_ts
    _nexus_wl_cache_ts = 0.0


# ── Tracking bot masuk/keluar grup ────────────────────────────────────────────

@Client.on_chat_member_updated(group=7)
async def nexus_react_bot_perm_change(client: Client, update: ChatMemberUpdated):
    """
    Reaktif — bukan polling. Telegram mengirim ChatMemberUpdated SAAT ITU JUGA
    saat privilege bot (atau status bot) di suatu grup berubah, termasuk saat
    admin mencabut/mengembalikan izin hapus pesan / ban-mute. Payload update
    ini SUDAH berisi privileges terbaru — tidak perlu panggil API apapun
    (get_chat_member dll) untuk membacanya, jadi TIDAK ADA risiko FloodWait
    di handler ini, walau triggernya ramai sekaligus (mis. demote massal).

    Sebelumnya status "perm_forced_off" (dipakai panel "Grup Terdaftar")
    HANYA di-update oleh perm_watchdog yang polling tiap 3600 detik (1 jam)
    — owner bisa lihat status basi sampai 1 jam. Handler ini menutup gap itu:
    begitu privilege berubah, perm_forced_off langsung disinkronkan ke DB
    detik itu juga. perm_watchdog TETAP jalan sebagai fallback/safety-net
    (mis. event ter-skip karena restart container) — tidak dihapus, tidak
    diubah, cuma jadi cadangan, bukan satu-satunya sumber update lagi.

    group=7 — sebelum nexus_tracking_grup (group=8) supaya status izin
    tersinkron lebih dulu sebelum tracking masuk/keluar grup diproses.
    """
    try:
        me = client.me
        new_member = update.new_chat_member
        if not new_member or not new_member.user or new_member.user.id != me.id:
            return  # bukan update soal bot ini sendiri — abaikan

        chat_id = update.chat.id
        status  = new_member.status

        if status in (ChatMemberStatus.BANNED, ChatMemberStatus.LEFT):
            # Bot dikick/keluar — biarkan nexus_tracking_grup (group=8) yang
            # urus penghapusan dari DB, di sini tidak perlu apa-apa lagi.
            return

        privs        = getattr(new_member, "privileges", None)
        can_del      = bool(getattr(privs, "can_delete_messages",  False)) if privs else False
        can_restrict = bool(getattr(privs, "can_restrict_members", False)) if privs else False
        has_perms    = can_del and can_restrict

        if not has_perms:
            await force_disable_group_moderation(chat_id)
        else:
            await restore_group_moderation_if_forced(chat_id)
    except Exception as e:
        print(f"[nexus_react_bot_perm_change] {e}")


@Client.on_chat_member_updated(group=8)
async def nexus_tracking_grup(client: Client, update: ChatMemberUpdated):
    try:
        from pyrogram.enums import ChatType
        me = client.me
        if not update.new_chat_member or update.new_chat_member.user.id != me.id:
            return

        try:
            chat = await client.get_chat(update.chat.id)
            if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
                return
        except Exception:
            return

        chat_id    = update.chat.id
        new_status = update.new_chat_member.status

        if new_status in (ChatMemberStatus.BANNED, ChatMemberStatus.LEFT):
            await nexus_remove_grup(chat_id)
        elif new_status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.MEMBER):
            await nexus_track_grup(chat_id, update.chat.title or str(chat_id), update.chat.username)
            asyncio.create_task(_maybe_bootstrap_admin_roster(client, chat_id))
            asyncio.create_task(_maybe_grant_new_group_trial(client, chat_id))
    except Exception as e:
        print(f"[nexus_tracking_grup] {e}")


async def _maybe_grant_new_group_trial(client: Client, chat_id: int) -> bool:
    """
    Jatah GRATIS 3 hari Upgrade Speed ("mode flash") untuk grup yang BENAR-
    BENAR baru pertama kali pakai bot ini.

    Kelayakan dicek dari database.try_register_new_group_trial() — riwayat
    PERMANEN yang terpisah dari nexus_grup_db/config_db, sehingga TIDAK
    tersentuh kalau bot di-kick, di-unadmin, atau grup direset. Artinya
    kalau grup ini SUDAH PERNAH tercatat kapan pun sebelumnya (termasuk
    sesi pemakaian bot yang lama, sebelum bot sempat dikeluarkan), grup
    ini TIDAK dapat jatah trial kedua — sistem lain (boost donasi manual
    lewat /ram, dst) tetap berjalan seperti biasa, sama sekali tidak
    diubah oleh jatah trial ini.

    Dipanggil dari 2 jalur (satu-satunya sumber logic grant, tidak ada
    duplikasi):
      1. nexus_tracking_grup — reaktif, begitu bot terdeteksi masuk/jadi
         admin grup (grup baru SETELAH fitur ini rilis).
      2. backfill_new_group_trial_for_existing_groups (dipanggil sekali
         saat startup) — untuk grup LAMA yang sudah terpasang bot SEBELUM
         fitur trial ini ada, supaya ikut kebagian jatah 3 hari juga.

    Return True kalau jatah trial BERHASIL diberikan barusan (dipakai
    backfill untuk hitung & log), False kalau tidak (sudah pernah trial,
    atau grup sedang punya boost aktif lain).
    """
    try:
        from database import try_register_new_group_trial
        is_new = await try_register_new_group_trial(chat_id)
        if not is_new:
            return False

        from core.speed_boost import get_boost_status, set_boost
        st = await get_boost_status(chat_id)
        if st["active"]:
            # Grup ini kebetulan SUDAH punya boost aktif (mis. donasi manual
            # yang lebih dulu di-set owner lewat /ram) — jangan ditimpa jatah
            # trial (bisa memperpendek masa boost yang sudah lebih baik).
            # Riwayat trial TETAP tercatat di atas, supaya grup ini tidak
            # dapat jatah trial lagi setelah boost yang sekarang habis.
            print(f"[TrialBoost] ℹ️  Grup {chat_id} sudah punya boost aktif "
                  f"(source={st.get('source')}) — jatah trial dilewati, "
                  f"riwayat tetap dicatat.")
            return False

        from datetime import datetime, timedelta
        from database import TZ_WIB
        # FIX BUG TIMEZONE: konsisten dengan core/speed_boost.py — pakai WIB
        # eksplisit, bukan datetime.now() naive (timezone lokal server, bisa
        # UTC), supaya jatah trial 3 hari benar-benar berakhir end-of-day WIB.
        until_dt = datetime.now(TZ_WIB) + timedelta(days=3)
        await set_boost(chat_id, until_dt, set_by=0, source="trial")
        print(f"[TrialBoost] 🎁 Grup {chat_id} dapat jatah 3 hari "
              f"Upgrade Speed gratis (mode flash) s/d {until_dt.date()}.")
        return True
    except Exception as e:
        print(f"[TrialBoost] gagal proses jatah trial grup={chat_id}: {e}")
        return False


async def backfill_new_group_trial_for_existing_groups(client: Client) -> int:
    """
    Migrasi SEKALI (idempotent, aman dipanggil berkali-kali) untuk grup
    LAMA yang sudah terpasang bot SEBELUM fitur trial 3-hari ini ada —
    supaya mereka juga kebagian jatah trial, bukan cuma grup yang baru
    ditambahkan SETELAH fitur ini rilis.

    Dipanggil sekali di startup (lihat main.py, setelah
    load_active_speed_boosts()). Toggle lewat env
    NEW_GROUP_TRIAL_BACKFILL_ENABLED (default AKTIF — set "0" untuk
    mematikan kalau owner tidak ingin grup lama ikut kebagian).

    Reuse _maybe_grant_new_group_trial() — satu-satunya sumber logic
    grant + idempotent-check (grup yang sudah pernah tercatat di
    group_trial_history otomatis dilewati, tidak ada risiko dobel-grant).
    """
    enabled = os.environ.get(
        "NEW_GROUP_TRIAL_BACKFILL_ENABLED", "1"
    ).strip().lower() in ("1", "true", "yes")
    if not enabled:
        print("[TrialBoost] ℹ️  Backfill trial grup lama dimatikan "
              "(NEW_GROUP_TRIAL_BACKFILL_ENABLED=0).")
        return 0

    from database import nexus_get_all_grup
    try:
        groups = await nexus_get_all_grup()
    except Exception as e:
        print(f"[TrialBoost] gagal ambil daftar grup untuk backfill: {e}")
        return 0

    granted = 0
    for g in groups:
        try:
            chat_id = int(g["chat_id"])
        except (KeyError, TypeError, ValueError):
            continue
        try:
            if await _maybe_grant_new_group_trial(client, chat_id):
                granted += 1
        except Exception as e:
            print(f"[TrialBoost] gagal backfill grup={chat_id}: {e}")
        # Jeda kecil — grup lama bisa jumlahnya banyak sekaligus, hindari
        # burst write ke speed_boost_db (cluster config) dalam 1 tarikan.
        await asyncio.sleep(0.05)

    print(f"[TrialBoost] ✅ Backfill trial grup lama selesai: {granted} grup "
          f"dapat jatah 3 hari baru dari {len(groups)} grup terdaftar.")
    return granted


async def _maybe_bootstrap_admin_roster(client: Client, chat_id: int) -> None:
    """
    Isi group_admin_roster untuk grup ini kalau belum pernah dibuat sama
    sekali. Dipanggil setiap kali bot "dikenali" masuk/berstatus di grup ini
    (nexus_tracking_grup) — tapi hanya benar-benar scan sekali; panggilan
    berikutnya untuk grup yang sama akan langsung skip karena roster != None.
    Dijalankan sebagai background task agar tidak menunda handler utama.
    """
    if chat_id in _admin_roster_bootstrap_in_progress:
        return
    _admin_roster_bootstrap_in_progress.add(chat_id)
    try:
        existing = await get_admin_roster(chat_id)
        if existing is None:
            await bootstrap_admin_roster(client, chat_id)
    except Exception as e:
        print(f"[AdminRoster] Bootstrap awal gagal grup {chat_id}: {e}")
    finally:
        _admin_roster_bootstrap_in_progress.discard(chat_id)


@Client.on_chat_member_updated(group=9)
async def nexus_track_admin_demotion(client: Client, update: ChatMemberUpdated):
    """
    Deteksi perubahan status admin (promote/demote) untuk 2 tujuan:
      1. Demote → cabut sesi DM panel admin (perilaku lama, tidak diubah).
      2. Promote/demote apapun → sinkronkan ke group_admin_roster (persisten
         di DB) secara incremental, supaya userbot Security OS bisa baca
         daftar admin dari DB tanpa perlu panggil Telegram API sendiri.
    group=9 — jalan setelah nexus_tracking_grup (group=8).
    """
    try:
        if not update.old_chat_member or not update.new_chat_member:
            return

        old_status = update.old_chat_member.status
        new_status = update.new_chat_member.status

        was_admin = old_status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
        now_admin = new_status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)

        if was_admin == now_admin:
            return  # tidak ada perubahan status admin — tidak ada yang perlu disinkronkan

        target_uid = update.new_chat_member.user.id
        chat_id    = update.chat.id

        # Sinkronkan roster persisten (aman dipanggil walau roster grup ini
        # belum pernah di-bootstrap — sekarang langsung bootstrap penuh saat
        # itu juga kalau ternyata None, lihat admin_roster_upsert_user()).
        await admin_roster_upsert_user(chat_id, target_uid, now_admin, client=client)

        # Roster berubah → cache get_my_admin_groups(target_uid) jadi basi.
        # Buang cache-nya lalu langsung set ulang menu DM (tombol "Menu"
        # pojok kiri-bawah) user ini, tanpa perlu nunggu dia /start lagi.
        try:
            from database import invalidate_admin_groups_cache
            invalidate_admin_groups_cache(target_uid)
        except Exception:
            pass

        try:
            from core.dm_menu import set_dm_menu_for_user
            asyncio.create_task(set_dm_menu_for_user(client, target_uid))
        except Exception as e:
            print(f"[nexus_track_admin_demotion] ⚠️  Gagal refresh menu DM: {e}")

        if was_admin and not now_admin:
            _adm_sess.on_admin_demoted(target_uid, chat_id)
    except Exception as e:
        print(f"[nexus_track_admin_demotion] {e}")
