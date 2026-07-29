"""
plugins/commands/pxram.py
───────────────────────────────
Perintah owner (DM saja) — kelola akses "💎 Upgrade Akun" Promo Userbot
secara manual. Fallback yang SELALU tersedia di samping jalur donasi QRIS
otomatis (pxocr_confirm_ di handlers_promo_userbot.py) — sama filosofi
dengan /ram untuk Upgrade Speed (core/speed_boost.py): OCR/tombol confirm
cuma shortcut, verifikasi manual owner via command tetap wajib ada sebagai
jalan keluar kalau OCR salah baca atau saran sudah kedaluwarsa.

  /pxram                                  → bantuan format
  /pxram <user_id> up <DD-MM-YYYY>        → aktifkan/perpanjang akses akun
                                             Promo Userbot user itu sampai
                                             AKHIR HARI tanggal tsb.
                                             Contoh: /pxram 123456789 up 30-8-2026
  /pxram <user_id> off                    → cabut akses SEKARANG JUGA
                                             (teardown client, balik ke
                                             pending_approval)
  /pxram <user_id> status                 → cek status akses akun itu

Lihat security_os/promo_userbot.py bagian "AKSES BERBASIS DONASI" untuk
detail mekanisme (approve-atau-perpanjang, expiry watchdog, dsb).
"""

import os
import time
from datetime import datetime as _dt_pxram

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ParseMode

from core.speed_boost import parse_ddmmyyyy
from security_os import promo_userbot as _pub
from database import TZ_WIB as _TZ_WIB_PXRAM

_OWNER_ID = int(os.environ.get("OWNER_ID", 0))


def _fmt_epoch(epoch: float) -> str:
    try:
        return _dt_pxram.fromtimestamp(epoch, tz=_TZ_WIB_PXRAM).strftime("%d-%m-%Y %H:%M") + " WIB"
    except Exception:
        return "-"


_HELP_TEXT = (
    "💎 <b>KELOLA UPGRADE AKUN — /pxram</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "<b>Aktifkan/perpanjang akses akun sampai tanggal tertentu:</b>\n"
    "<code>/pxram &lt;user_id&gt; up &lt;DD-MM-YYYY&gt;</code>\n"
    "Contoh: <code>/pxram 123456789 up 30-8-2026</code>\n\n"
    "<b>Cabut akses sekarang (paksa balik pending_approval):</b>\n"
    "<code>/pxram &lt;user_id&gt; off</code>\n\n"
    "<b>Cek status akses akun:</b>\n"
    "<code>/pxram &lt;user_id&gt; status</code>\n\n"
    "<i>Akun 'pending_approval' langsung diaktifkan; akun 'active' "
    "diperpanjang dari sisa akses aktifnya (bukan dihitung ulang dari "
    "sekarang, kalau belum habis).</i>"
)


@Client.on_message(filters.command("pxram") & filters.private & filters.user(_OWNER_ID))
async def cmd_pxram(client: Client, message: Message):
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
        owner_uid = int(args[0])
    except ValueError:
        return await message.reply(
            "❌ <b>user_id tidak valid.</b> Harus angka.",
            parse_mode=ParseMode.HTML,
        )

    action = args[1].lower()

    # ── /pxram <user_id> status ──────────────────────────────────────────────
    if action == "status":
        st = await _pub.get_access_status(owner_uid)
        if st["status"] is None:
            return await message.reply(
                f"💎 <b>Status Upgrade Akun</b>\n<code>User: {owner_uid}</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"⚠️ Akun tidak ditemukan (belum pernah login Promo Userbot).",
                parse_mode=ParseMode.HTML,
            )
        if st["active"]:
            durasi = (
                f"⏳ Berakhir: <code>{_fmt_epoch(st['until'])}</code>"
                if st["until"] else "♾️ <b>Permanen</b> (di-approve manual Owner)"
            )
            return await message.reply(
                f"💎 <b>Status Upgrade Akun</b>\n<code>User: {owner_uid}</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🟢 <b>Aktif</b>.\n{durasi}",
                parse_mode=ParseMode.HTML,
            )
        return await message.reply(
            f"💎 <b>Status Upgrade Akun</b>\n<code>User: {owner_uid}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🔴 <b>Tidak aktif</b> — status akun saat ini: <code>{st['status']}</code>.",
            parse_mode=ParseMode.HTML,
        )

    # ── /pxram <user_id> off ─────────────────────────────────────────────────
    if action == "off":
        st = await _pub.get_access_status(owner_uid)
        if st["status"] != "active":
            return await message.reply(
                f"⚠️ Akun ini status-nya '<code>{st['status']}</code>', bukan 'active' — tidak ada apa-apa untuk dicabut.",
                parse_mode=ParseMode.HTML,
            )
        await _pub.suspend_expired_access(owner_uid, client)
        return await message.reply(
            f"🔒 <b>Akses dicabut.</b>\n<code>User: {owner_uid}</code> sekarang "
            f"balik ke pending_approval (client di-teardown).",
            parse_mode=ParseMode.HTML,
        )

    # ── /pxram <user_id> up <DD-MM-YYYY> ─────────────────────────────────────
    if action == "up":
        if len(args) < 3:
            return await message.reply(
                "❌ <b>Tanggal belum diisi.</b>\n\n"
                "Format: <code>/pxram &lt;user_id&gt; up &lt;DD-MM-YYYY&gt;</code>\n"
                "Contoh: <code>/pxram 123456789 up 30-8-2026</code>",
                parse_mode=ParseMode.HTML,
            )
        dt = parse_ddmmyyyy(args[2])
        if dt is None:
            return await message.reply(
                "❌ <b>Format tanggal tidak dikenali.</b>\n\n"
                "Gunakan format <code>DD-MM-YYYY</code>, contoh: <code>30-8-2026</code>.",
                parse_mode=ParseMode.HTML,
            )
        end_of_day_wib = dt.replace(
            hour=23, minute=59, second=59, microsecond=0, tzinfo=_TZ_WIB_PXRAM,
        )
        if end_of_day_wib.timestamp() <= time.time():
            return await message.reply(
                "❌ <b>Tanggal sudah lewat.</b> Pakai tanggal di masa depan.",
                parse_mode=ParseMode.HTML,
            )

        ok, msg, until_epoch = await _pub.activate_or_extend_access(
            owner_uid, dt, approver_id=message.from_user.id, main_bot=client,
        )
        if not ok:
            return await message.reply(f"❌ {msg}", parse_mode=ParseMode.HTML)
        return await message.reply(
            f"{msg}\n<code>User: {owner_uid}</code> — akses s/d "
            f"<code>{_fmt_epoch(until_epoch)}</code>.\n\n"
            f"Setelah itu, otomatis di-teardown + balik pending_approval, dan "
            f"user dapat notif DM.",
            parse_mode=ParseMode.HTML,
        )

    return await message.reply(
        "❌ <b>Aksi tidak dikenali.</b>\n\n" + _HELP_TEXT,
        parse_mode=ParseMode.HTML,
    )
