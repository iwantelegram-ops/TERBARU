"""
plugins/commands/mention_reset_owner.py
─────────────────────────────────────────
Perintah /resetmention — khusus owner, hanya via DM bot.

Menghapus TOTAL seluruh data cache mention di SEMUA grup sekaligus:
  - mention_member_cache      (status member per grup)
  - mention_global_cache      (non_akun / channel / grup, lintas semua grup)
  - mention_pending_resolve   (antrian resolusi background)
  - mention_bio_scan_cache    (hasil scan bio: flagged=True/False, global)
  - mention_bio_scan_pending  (antrian scan bio yang belum dikerjakan)

Dipakai untuk memperbaiki data lama yang sempat salah tersimpan sebagai
"non_akun" akibat bug lama (akun asli tapi belum pernah "berhubungan"
langsung dengan bot, sehingga get_chat() gagal duluan dan salah divonis
tidak valid). Setelah reset ini, SEMUA @username di SEMUA grup akan
di-resolve ulang dari API Telegram dari nol, pakai logika yang sudah
diperbaiki (get_chat_member dulu, get_chat cuma fallback).

Beda dengan /resetmentioncache (plugins/commands/mention_cache_reset.py):
  - /resetmentioncache -> admin grup, DI GRUP, cuma 1 grup, ada cooldown 6 jam,
    tidak menyentuh mention_global_cache.
  - /resetmention      -> OWNER, via DM, SEMUA grup + SEMUA cache sekaligus,
    tidak ada cooldown (dibatasi lewat konfirmasi manual + owner-only).

Contoh: /resetmention
"""

import os
import html

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode

from database import mention_reset_all

_OWNER_ID = int(os.environ.get("OWNER_ID", 0))

# FSM sederhana: siapa saja yang sedang menunggu konfirmasi
_pending_confirm: set[int] = set()


@Client.on_message(
    filters.command("resetmention") & filters.private & filters.user(_OWNER_ID)
)
async def cmd_reset_mention(client: Client, message: Message):
    """/resetmention — hapus SEMUA data cache mention (semua grup)."""
    if not _OWNER_ID:
        return

    _pending_confirm.add(message.from_user.id)

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("⚠️ YA, HAPUS SEMUA", callback_data="resetmention_yes"),
            InlineKeyboardButton("❌ Batal", callback_data="resetmention_no"),
        ]
    ])

    await message.reply(
        "⚠️ <b>KONFIRMASI RESET DATA MENTION</b>\n\n"
        "Perintah ini akan menghapus <b>SEMUA</b> data cache fitur "
        "\"Batasi Tag Akun / Channel / Grup\" di <b>SELURUH grup</b> "
        "sekaligus:\n\n"
        "  ◈ Cache status member per grup (siapa akun, siapa bukan member)\n"
        "  ◈ Cache global non-akun / channel / grup (lintas semua grup)\n"
        "  ◈ Antrian resolusi background yang belum selesai\n"
        "  ◈ Cache hasil scan bio (flagged / bersih, lintas semua grup)\n"
        "  ◈ Antrian scan bio yang belum dikerjakan\n\n"
        "Dipakai untuk membersihkan data lama yang sempat salah tersimpan "
        "sebagai \"tidak valid\" akibat bug lama, supaya semua username "
        "di-cek ulang dari awal dengan logika yang sudah diperbaiki.\n\n"
        "<b>Efek samping:</b> untuk sementara (sampai cache terisi lagi "
        "secara alami), setiap mention baru akan memicu pengecekan API "
        "lebih sering di semua grup, sampai cache-nya terbentuk ulang.\n\n"
        "<b>Ini tidak bisa dibatalkan!</b>",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
    )


@Client.on_callback_query(
    filters.regex(r"^resetmention_yes$") & filters.user(_OWNER_ID)
)
async def cb_reset_mention_confirm(client: Client, cb):
    await cb.answer("⏳ Menghapus semua data mention...")

    if cb.from_user.id not in _pending_confirm:
        return await cb.message.edit(
            "❌ Sesi konfirmasi sudah kedaluwarsa. Ulangi perintah /resetmention.",
            parse_mode=ParseMode.HTML,
        )

    _pending_confirm.discard(cb.from_user.id)

    await cb.message.edit(
        "⏳ <b>Menghapus semua data mention...</b>\n<i>Mohon tunggu...</i>",
        parse_mode=ParseMode.HTML,
    )

    try:
        result = await mention_reset_all()
    except Exception as e:
        return await cb.message.edit(
            f"❌ <b>Error saat reset:</b>\n<code>{html.escape(str(e))}</code>",
            parse_mode=ParseMode.HTML,
        )

    total = sum(result.values())
    detail = "\n".join(f"  ◈ {html.escape(k)}: <code>{v}</code>" for k, v in result.items())

    await cb.message.edit(
        "✅ <b>RESET MENTION SELESAI</b>\n\n"
        f"<b>Total dihapus:</b> <code>{total} dokumen</code>\n\n"
        f"<b>Rincian:</b>\n{detail}\n\n"
        "<i>Semua @username di semua grup akan di-resolve ulang dari API "
        "Telegram mulai sekarang, pakai logika yang sudah diperbaiki.</i>",
        parse_mode=ParseMode.HTML,
    )


@Client.on_callback_query(
    filters.regex(r"^resetmention_no$") & filters.user(_OWNER_ID)
)
async def cb_reset_mention_cancel(client: Client, cb):
    await cb.answer("Dibatalkan.")
    _pending_confirm.discard(cb.from_user.id)
    await cb.message.edit(
        "✅ <b>Reset dibatalkan.</b>\n\n<i>Data mention tetap aman.</i>",
        parse_mode=ParseMode.HTML,
    )
