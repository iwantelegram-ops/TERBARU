"""
plugins/commands/ram_speed.py
───────────────────────────────
Perintah owner (DM saja) — kelola boost "Upgrade Speed" (mode flash/slow)
per grup.

CATATAN: boost ini HANYA mempengaruhi kecepatan HAPUS pesan spam yang
sudah kedeteksi (lihat database.py::delete_worker, DELETE_INTERVAL_FLASH
vs DELETE_INTERVAL_SLOW). Worker DETEKSI (core/antispam_queue.py) SELALU
jalan di speed maksimal untuk semua grup, terlepas dari status boost.

  /ram                              → bantuan format
  /ram <chat_id> up <DD-MM-YYYY>    → buka mode flash grup itu (hapus pesan
                                       spam pakai DELETE_INTERVAL_FLASH,
                                       lebih cepat) sampai AKHIR HARI tanggal
                                       tsb. Contoh: /ram -100123456789 up 30-8-2026
  /ram <chat_id> off                → cabut boost SEKARANG JUGA, paksa balik
                                       ke mode slow (manual, tanpa nunggu
                                       tanggal habis)
  /ram <chat_id> status             → cek status boost grup itu saat ini

Lihat core/speed_boost.py untuk detail mekanisme (penguncian default,
persistensi lintas redeploy, notifikasi expiry).
"""

import os
import time

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ParseMode

from core.speed_boost import (
    parse_ddmmyyyy, set_boost, clear_boost, get_boost_status,
)

_OWNER_ID = int(os.environ.get("OWNER_ID", 0))


from datetime import datetime as _dt_ram
from database import TZ_WIB as _TZ_WIB_RAM


def _fmt_epoch(epoch: float) -> str:
    # FIX BUG TIMEZONE: `time.localtime(epoch)` memakai timezone LOKAL
    # SERVER (bisa UTC di kebanyakan panel/VPS), bukan WIB — jadi tampilan
    # "Berakhir" ini dulu bisa berbeda beberapa jam dari maksud owner (yang
    # selalu berpikir dalam WIB), dan tidak konsisten dengan cara
    # core/speed_boost.py menghitung until_epoch (sekarang eksplisit WIB).
    try:
        return _dt_ram.fromtimestamp(epoch, tz=_TZ_WIB_RAM).strftime("%d-%m-%Y %H:%M") + " WIB"
    except Exception:
        return "-"


_HELP_TEXT = (
    "⚡ <b>KELOLA UPGRADE SPEED — /ram</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "<b>Aktifkan mode flash grup sampai tanggal tertentu:</b>\n"
    "<code>/ram &lt;chat_id&gt; up &lt;DD-MM-YYYY&gt;</code>\n"
    "Contoh: <code>/ram -100123456789 up 30-8-2026</code>\n\n"
    "<b>Cabut boost sekarang (paksa balik ke mode slow):</b>\n"
    "<code>/ram &lt;chat_id&gt; off</code>\n\n"
    "<b>Cek status boost grup:</b>\n"
    "<code>/ram &lt;chat_id&gt; status</code>\n\n"
    "<i>Ini cuma mengubah kecepatan HAPUS pesan spam (mode flash = lebih "
    "cepat, mode slow = default). Kecepatan DETEKSI tidak terpengaruh — "
    "selalu maksimal untuk semua grup.</i>"
)


@Client.on_message(filters.command("ram") & filters.private & filters.user(_OWNER_ID))
async def cmd_ram(client: Client, message: Message):
    if not _OWNER_ID:
        return

    args = message.command[1:]
    if not args:
        return await message.reply(_HELP_TEXT, parse_mode=ParseMode.HTML)

    if len(args) < 2:
        return await message.reply(
            "❌ <b>Format kurang lengkap.</b>\n\n" + _HELP_TEXT,
            parse_mode=ParseMode.HTML,
        )

    try:
        chat_id = int(args[0])
    except ValueError:
        return await message.reply(
            "❌ <b>chat_id tidak valid.</b> Harus angka (contoh: -100123456789).",
            parse_mode=ParseMode.HTML,
        )

    action = args[1].lower()

    # ── /ram <chat_id> status ────────────────────────────────────────────────
    if action == "status":
        import core.antispam_queue as _aq
        admin_flash_note = (
            "\n\n⚡ <b>Catatan:</b> grup ini JUGA di-force mode flash tanpa batas "
            "waktu lewat Admin-Flash override (deteksi admin khusus) — status "
            "efektifnya tetap flash walau boost donasi/trial di atas nonaktif."
            if _aq.is_admin_flash(chat_id) else ""
        )
        st = await get_boost_status(chat_id)
        if st["active"]:
            src = st.get("source") or "donation"
            src_label = "🎁 Trial gratis (grup baru)" if src == "trial" else "💳 Donasi (manual /ram)"
            return await message.reply(
                f"⚡ <b>Status Upgrade Speed</b>\n"
                f"<code>Grup: {chat_id}</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🚀 <b>Aktif</b> — mode flash, hapus pesan spam lebih cepat.\n"
                f"🏷️ Sumber: {src_label}\n"
                f"⏳ Berakhir: <code>{_fmt_epoch(st['until'])}</code>"
                f"{admin_flash_note}",
                parse_mode=ParseMode.HTML,
            )
        return await message.reply(
            f"⚡ <b>Status Upgrade Speed</b>\n"
            f"<code>Grup: {chat_id}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🔒 <b>Mode slow</b> (tidak sedang di-boost)."
            f"{admin_flash_note}",
            parse_mode=ParseMode.HTML,
        )

    # ── /ram <chat_id> off ───────────────────────────────────────────────────
    if action == "off":
        await clear_boost(chat_id, notified=True)
        return await message.reply(
            f"🔒 <b>Boost dicabut.</b>\n"
            f"<code>Grup: {chat_id}</code> sekarang kembali ke mode slow (hapus pesan spam di kecepatan dasar).",
            parse_mode=ParseMode.HTML,
        )

    # ── /ram <chat_id> up <DD-MM-YYYY> ───────────────────────────────────────
    if action == "up":
        if len(args) < 3:
            return await message.reply(
                "❌ <b>Tanggal belum diisi.</b>\n\n"
                "Format: <code>/ram &lt;chat_id&gt; up &lt;DD-MM-YYYY&gt;</code>\n"
                "Contoh: <code>/ram -100123456789 up 30-8-2026</code>",
                parse_mode=ParseMode.HTML,
            )
        dt = parse_ddmmyyyy(args[2])
        if dt is None:
            return await message.reply(
                "❌ <b>Format tanggal tidak dikenali.</b>\n\n"
                "Gunakan format <code>DD-MM-YYYY</code>, contoh: <code>30-8-2026</code>.",
                parse_mode=ParseMode.HTML,
            )
        # Bandingkan pakai akhir-hari WIB (konsisten dengan set_boost), bukan
        # tengah malam naive di timezone server — supaya "hari ini" (WIB)
        # tidak ditolak keliru sebagai "sudah lewat" oleh server ber-timezone UTC.
        end_of_day_wib = dt.replace(
            hour=23, minute=59, second=59, microsecond=0, tzinfo=_TZ_WIB_RAM,
        )
        if end_of_day_wib.timestamp() <= time.time():
            return await message.reply(
                "❌ <b>Tanggal sudah lewat.</b> Pakai tanggal di masa depan.",
                parse_mode=ParseMode.HTML,
            )

        until_epoch = await set_boost(chat_id, dt, set_by=message.from_user.id)
        return await message.reply(
            f"🚀 <b>Speed grup dibuka!</b>\n"
            f"<code>Grup: {chat_id}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Grup ini sekarang mode flash — pesan spam yang kedeteksi "
            f"kehapus lebih cepat, sampai <code>{_fmt_epoch(until_epoch)}</code>.\n\n"
            f"Setelah itu, otomatis kembali ke mode slow + admin/donor "
            f"grup ini akan dapat notif DM.",
            parse_mode=ParseMode.HTML,
        )

    return await message.reply(
        "❌ <b>Aksi tidak dikenali.</b>\n\n" + _HELP_TEXT,
        parse_mode=ParseMode.HTML,
    )
