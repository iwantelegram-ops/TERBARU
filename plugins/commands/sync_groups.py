"""
plugins/commands/sync_groups.py
──────────────────────────────────
Perintah /syncgrup — khusus owner via DM. Pemulihan MANUAL registry grup
(config_db) dari daftar dialog Telegram, tanpa perlu restart/redeploy bot.

KAPAN DIPAKAI:
  Bot sudah otomatis menjalankan pemulihan ini SEKALI setiap kali start
  (lihat bootstrap_groups_from_dialogs di database.py, dipanggil dari
  main.py). Command ini untuk kasus:
    • Baru saja kena masalah ini (config_db kosong/sebagian) dan owner
      tidak mau nunggu restart/redeploy lagi untuk memulihkannya.
    • Pemulihan otomatis saat startup sempat kena FloodWait di tengah
      jalan (grup banyak) — jalankan lagi untuk melanjutkan sisanya.

APA YANG DIPULIHKAN:
  Hanya config_db (dipakai /list, panel "Grup Terdaftar", & daftar "grup
  saya" di panel admin) — diisi ulang dari client.get_dialogs() (sumber
  yang SELALU akurat, tidak tergantung DB sama sekali). Pengaturan
  detail tiap grup (bio_check, anti_mention, dst) TIDAK ikut hilang kalau
  memang belum di-reset — fungsi ini hanya upsert field {chat_id, title,
  username, chat_type}, tidak menimpa toggle yang sudah ada.

  TIDAK memulihkan: Security OS (butuh setup bot pemantau ulang secara
  manual per grup — data itu memang tidak bisa direkonstruksi otomatis
  dari Telegram), daftar filter kata custom, dsb — hanya "grup ini pakai
  bot" yang bisa dipulihkan murni dari keanggotaan Telegram.
"""

import os
import time

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ParseMode

from database import bootstrap_groups_from_dialogs

_OWNER_ID = int(os.environ.get("OWNER_ID", 0))

# Cooldown 60 detik — get_dialogs() bisa berat kalau bot ada di ratusan grup,
# jangan sampai owner spam-klik dan bikin beberapa scan tumpang tindih.
_COOLDOWN_SECS = 60
_last_sync = 0.0


@Client.on_message(
    filters.command(["syncgrup", "resyncgrup"]) & filters.private & filters.user(_OWNER_ID)
)
async def cmd_sync_groups(client: Client, message: Message):
    global _last_sync

    if not _OWNER_ID:
        return

    now = time.time()
    if now - _last_sync < _COOLDOWN_SECS:
        sisa = int(_COOLDOWN_SECS - (now - _last_sync))
        return await message.reply(
            f"⏳ Baru saja dijalankan — coba lagi {sisa} detik lagi.",
            parse_mode=ParseMode.HTML,
        )
    _last_sync = now

    status = await message.reply(
        "🔄 <b>Memulihkan registry grup (lapis 1 — via userbot)...</b>\n"
        "Catatan: hanya grup yang juga punya userbot Security OS yang bisa "
        "dipulihkan instan lewat cara ini — grup lain pulih otomatis begitu "
        "ada pesan masuk (lihat penjelasan di bawah).",
        parse_mode=ParseMode.HTML,
    )

    try:
        registered, skipped_channel = await bootstrap_groups_from_dialogs()
    except Exception as e:
        return await status.edit(
            f"❌ Gagal memulihkan registry grup: <code>{e}</code>",
            parse_mode=ParseMode.HTML,
        )

    await status.edit(
        f"✅ <b>Pemulihan lapis 1 selesai.</b>\n\n"
        f"◈ Grup didaftarkan/disegarkan (via userbot): <b>{registered}</b>\n"
        f"◈ Channel dilewati: <b>{skipped_channel}</b>\n\n"
        f"Cek lagi <code>/list</code> atau panel <b>Grup Terdaftar</b>.\n\n"
        f"<b>Kenapa cuma segini?</b> Telegram TIDAK menyediakan API bagi akun "
        f"bot untuk menanyakan \"aku ada di grup mana saja\" — jadi lapis ini "
        f"cuma bisa menjangkau grup yang PERNAH memakai userbot (Security Os/"
        f"Inspeksi Onkem). Grup lain yang bot-nya polos tanpa userbot akan "
        f"terdaftar OTOMATIS dengan sendirinya begitu ada 1 pesan apa pun "
        f"masuk dari grup itu (biasanya dalam hitungan menit untuk grup "
        f"aktif) — tidak perlu tindakan tambahan apa pun dari kamu.\n\n"
        f"<i>Catatan: pengaturan detail per grup (Security OS, filter kata, dst) "
        f"yang memang ikut ter-reset TIDAK dipulihkan otomatis — hanya status "
        f"'grup ini pakai bot' yang direkonstruksi.</i>",
        parse_mode=ParseMode.HTML,
    )
