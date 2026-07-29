"""
plugins/commands/testprivmode.py
─────────────────────────────────
Perintah /testprivmode — admin grup bisa tes status Privacy Mode bot
pemantau KAPAN PUN langsung dari grup, tanpa harus buka panel DM.

KENAPA PERLU INI (selain tombol "Verifikasi Sekarang" di panel DM):
  Tombol di panel DM cuma bisa dipicu dari DM. /testprivmode memindahkan
  cara memicu itu ke grup itu sendiri — lebih cepat diakses admin yang
  memang lagi ada di grup, dan bisa diulang kapan saja (mis. setelah admin
  matikan Privacy Mode di BotFather, admin bisa langsung tes ulang di
  tempat tanpa bolak-balik ke DM).

CARA KERJA (PENTING — command TIDAK BISA jadi bukti privacy OFF):
  Command (pesan berawalan "/") SELALU tembus ke semua bot di grup,
  privacy ON ataupun OFF — jadi /testprivmode sendiri bukan sinyal yang
  valid. Makanya alurnya 2 langkah:
    1. Admin ketik /testprivmode → bot utama "arm" jendela tes singkat
       (lihat arm_priv_test di monitor_bot_reference.py).
    2. Selama jendela itu, SIAPA PUN yang kirim 1 pesan BIASA (bukan
       command) di grup ini akan otomatis melengkapi tes (lihat
       _confirm_priv_test yang dipanggil dari _on_message MonitorInstance).
       Kalau grup sedang aktif, ini sering langsung kejadian sendiri tanpa
       admin perlu ngetik apa-apa lagi.
  Bot utama menunggu window itu habis, lalu balas status final + panduan
  kalau masih gagal.

TIDAK reset status OK yang sudah pernah terverifikasi sebelumnya kalau tes
kali ini gagal (mis. karena grup lagi sepi, bukan karena privacy nyala
lagi) — supaya fitur yang bergantung padanya tidak tiba-tiba terkunci
gara-gara false negative dari grup sepi.
"""

import asyncio

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ParseMode

from database import is_admin, auto_delete_reply

_TEST_WINDOW_SECS = 20


@Client.on_message(filters.command("testprivmode") & (filters.group | filters.forum))
async def cmd_testprivmode(client: Client, message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id if message.from_user else None

    if not user_id or not await is_admin(client, chat_id, user_id):
        warn = await message.reply(
            "🚫 Perintah ini hanya untuk admin grup.",
            parse_mode=ParseMode.HTML,
        )
        asyncio.create_task(auto_delete_reply([warn, message], delay=5))
        return

    # Hapus command dari grup (kebersihan chat) — status dibalas sebagai
    # pesan baru, bukan reply, supaya tetap kebaca walau command-nya hilang.
    try:
        await message.delete()
    except Exception:
        pass

    from video_call import _sec_os_get, sec_os_mark_privacy_ok
    from monitor_bot_reference import arm_priv_test, pop_priv_test_result

    sec_doc  = await _sec_os_get(chat_id)
    has_mon  = bool(sec_doc.get("monitor_bot_id", 0))
    old_ok   = bool(sec_doc.get("monitor_privacy_ok", False))

    if not has_mon:
        await client.send_message(
            chat_id,
            "⚠️ <b>Belum ada bot pemantau terpasang di grup ini.</b>\n"
            "Admin perlu pasang dulu lewat panel Security OS (DM bot ini) "
            "sebelum bisa tes Privacy Mode.",
            parse_mode=ParseMode.HTML,
        )
        return

    status_msg = await client.send_message(
        chat_id,
        f"🔍 <b>Testing Privacy Mode bot pemantau...</b>\n\n"
        f"Kalau grup ini sedang sepi, kirim <b>1 pesan apa saja</b> "
        f"(bukan command) SEKARANG — dari akun manapun, nggak harus kamu. "
        f"Kalau baru saja ada yang ngetik, cukup tunggu.\n\n"
        f"Hasil otomatis dalam {_TEST_WINDOW_SECS} detik.",
        parse_mode=ParseMode.HTML,
    )

    arm_priv_test(chat_id, _TEST_WINDOW_SECS)
    await asyncio.sleep(_TEST_WINDOW_SECS)
    fresh_confirmed = pop_priv_test_result(chat_id)

    if fresh_confirmed:
        await sec_os_mark_privacy_ok(chat_id)
        result_text = (
            "✅ <b>Privacy Mode sudah OFF.</b>\n"
            "Bot pemantau berhasil menerima pesan biasa barusan — semua "
            "fitur yang bergantung padanya (Inspeksi Bio Link, dll) aman "
            "dipakai."
        )
    elif old_ok:
        result_text = (
            "✅ <b>Sudah terverifikasi OK sebelumnya.</b>\n"
            "Tidak ada pesan baru terdeteksi selama tes barusan (kemungkinan "
            "grup sedang sepi) — status Privacy Mode tetap dianggap OFF "
            "berdasarkan verifikasi sebelumnya."
        )
    else:
        result_text = (
            "❌ <b>Privacy Mode bot pemantau masih ON.</b>\n"
            "Bot pemantau belum terbukti bisa menerima pesan biasa dari "
            "grup ini.\n\n"
            "<b>Cara benerin:</b>\n"
            "1️⃣ Pemilik bot pemantau: buka @BotFather → <code>/setprivacy</code> "
            "→ pilih bot pemantau ini → <b>Disable</b>.\n"
            "2️⃣ Keluarkan bot pemantau dari grup ini, lalu masukkan lagi "
            "secara manual (supaya statusnya di-refresh Telegram).\n"
            "3️⃣ Ketik <code>/testprivmode</code> lagi di sini untuk tes ulang."
        )

    try:
        await status_msg.edit_text(result_text, parse_mode=ParseMode.HTML)
    except Exception:
        await client.send_message(chat_id, result_text, parse_mode=ParseMode.HTML)
