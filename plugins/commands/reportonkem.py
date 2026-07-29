"""
plugins/commands/reportonkem.py
──────────────────────────────────
Perintah /reportonkem — member memicu inspeksi VC dadakan untuk Inspeksi
Onkem (fitur mute mic user yang ketahuan menyalakan kamera di obrolan suara).

SIAPA BOLEH PAKAI:
  Semua member grup — tidak perlu admin. Tujuannya supaya member yang lagi
  di VC dan melihat ada yang onkem bisa langsung "lapor" tanpa menunggu
  giliran siklus rutin 30 menit.

SYARAT SEBELUM DIPROSES (diabaikan senyap kalau salah satu gagal, KECUALI
disebutkan lain di bawah):
  1. Command dihapus segera dari grup (kebersihan chat).
  2. Inspeksi Onkem harus AKTIF untuk grup ini — kalau belum, beri tahu
     singkat (auto-delete) supaya member tahu harus minta admin
     mengaktifkan dulu lewat panel Security OS, BUKAN diam total (ini
     bukan spam-cooldown, jadi wajar dikasih tahu).
  3. Userbot harus online.
  4. Jeda 1 JAM per grup (BUKAN per user) — siapa pun yang kirim
     /reportonkem di grup yang sama selama jeda ini diabaikan SENYAP
     (tanpa balasan) supaya tidak membanjiri panel dengan notifikasi
     penolakan. Jeda ini sengaja lebih besar dari siklus rutin 30 menit
     (jadi tidak akan pernah lebih sering dari 2× siklus rutin), karena
     tujuan command ini hanya untuk kasus mendesak, bukan pengganti
     siklus rutin.

CARA KERJA SETELAH LOLOS SEMUA SYARAT:
  Sama seperti /unmutemic — TIDAK memanggil _vc_scan_and_enforce_impl
  langsung, melainkan _enqueue_vc_scan() supaya masuk ke antrean worker
  global yang sama dengan siklus rutin & permintaan reaktif lain (lihat
  "VC JOIN/LEAVE WORKER" di security_os/video_call.py). Worker itulah yang
  menjamin userbot tidak pernah disuruh naik VC di 2 grup berbeda secara
  bersamaan, dan tetap membatasi 20 detik per grup sebelum lanjut ke
  antrean grup lain — jadi /reportonkem TIDAK butuh mekanisme durasi/
  antrean baru sama sekali, cukup menumpang yang sudah ada.

  Begitu userbot naik VC grup ini (lihat _vc_scan_and_enforce_impl), SEMUA
  peserta yang sedang onkem akan otomatis kena mute — bukan cuma user yang
  "dilaporkan", karena command ini tidak menunjuk siapa yang diminta
  namanya reply pesan (tidak seperti /reportsticker) — cukup memicu
  inspeksi menyeluruh ke VC grup itu, cocok untuk kasus "banyak yang onkem
  sekaligus" tanpa member harus reply satu-satu.
"""

import time

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ParseMode

from database import auto_delete_reply
import asyncio

# ── Jeda 1 jam per grup — lapis tunggal, cukup karena siklus rutin sudah
# ada tiap 30 menit; command ini murni untuk kasus mendesak ────────────────
_REPORTONKEM_COOLDOWN_SECS = 60 * 60   # 1 jam
_last_reportonkem: dict[int, float] = {}   # {chat_id: time.time()}


@Client.on_message(filters.command("reportonkem") & (filters.group | filters.forum))
async def cmd_reportonkem(client: Client, message: Message):
    cid = message.chat.id
    uid = message.from_user.id if message.from_user else None
    if not uid:
        try:
            await message.delete()
        except Exception:
            pass
        return

    # ── Hapus pesan perintah segera ─────────────────────────────────────────
    try:
        await message.delete()
    except Exception:
        pass

    try:
        from video_call import (
            _sec_os_get, is_userbot_ready, _enqueue_vc_scan,
        )
    except ImportError as e:
        print(f"[ReportOnkem] Import error dari video_call: {e}")
        return

    # ── Custom Userbot grup ini sudah stay permanen di VC — tidak perlu
    #     perintah paksaan lewat antrean worker global (menghindari tabrakan
    #     dengan enforcement realtime yang sudah berjalan terus-menerus di
    #     akun pribadi admin tersebut). Beri tahu member supaya tidak bingung
    #     kenapa command-nya "tidak melakukan apa-apa".
    try:
        from security_os import custom_userbot as _cub
        if await _cub.is_active(cid):
            print(f"[ReportOnkem] grup={cid}: Custom Userbot aktif (stay permanen) — abaikan, tidak perlu antri.")
            try:
                info = await message.reply(
                    "ℹ️ Grup ini sudah dijaga <b>Custom Userbot</b> yang stay permanen di "
                    "VC — inspeksi onkem sudah berjalan otomatis real-time, tidak perlu "
                    "<code>/reportonkem</code> lagi.",
                    parse_mode=ParseMode.HTML,
                )
                asyncio.create_task(auto_delete_reply([info], delay=8))
            except Exception:
                pass
            return
    except Exception as e:
        print(f"[ReportOnkem] Gagal cek Custom Userbot grup={cid}: {e}")

    # ── Syarat: Inspeksi Onkem harus aktif di grup ini ──────────────────────
    sec_doc = await _sec_os_get(cid)
    if not sec_doc.get("onkem_enabled"):
        try:
            info = await message.reply(
                "ℹ️ <b>Inspeksi Onkem belum aktif</b> di grup ini.\n"
                "Minta admin mengaktifkannya lewat panel <b>Security OS</b>.",
                parse_mode=ParseMode.HTML,
            )
            asyncio.create_task(auto_delete_reply([info], delay=8))
        except Exception:
            pass
        return

    # ── Syarat: userbot online ───────────────────────────────────────────────
    if not is_userbot_ready():
        return   # senyap — bukan salah member, tidak perlu di-spam notif

    # ── Jeda 1 jam per grup ──────────────────────────────────────────────────
    now = time.time()
    last = _last_reportonkem.get(cid, 0.0)
    if now - last < _REPORTONKEM_COOLDOWN_SECS:
        return   # masih jeda → diabaikan senyap (cegah spam beramai-ramai)

    _last_reportonkem[cid] = now

    # ── Antri inspeksi VC dadakan ke worker global ──────────────────────────
    print(f"[ReportOnkem] uid={uid} grup={cid}: memicu inspeksi onkem dadakan.")
    _enqueue_vc_scan(cid)

    try:
        confirm = await message.reply(
            "🎥 <b>Inspeksi onkem dijalankan.</b>\n"
            "Userbot akan naik ke obrolan suara sebentar untuk memeriksa.",
            parse_mode=ParseMode.HTML,
        )
        asyncio.create_task(auto_delete_reply([confirm], delay=10))
    except Exception:
        pass
