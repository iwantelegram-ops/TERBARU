"""
plugins/commands/cast.py
──────────────────────────
Perintah OWNER: /cast <teks> — broadcast ke SEMUA user yang PERNAH DM bot
ini (database.get_all_dm_users(), diisi otomatis oleh database.register_dm_user()
tiap kali ada user chat ke bot — lihat main.py::_dm_peer_collector_handler).

BUKAN broadcast ke GRUP — itu urusan fitur lain (Anti-GCast Global, dll,
plugins/commands/antigcast_group.py). /cast ini KHUSUS japri/DM personal
ke user satu-satu.

ALUR:
  1. Owner ketik /cast <teks> (boleh pakai format Bold/Italic/dll seperti
     biasa di Telegram — diambil dari message.text.html, BUKAN
     message.command yang sudah kehilangan semua formatting).
  2. Bot tampilkan KONFIRMASI dulu (preview teks + jumlah target) dengan
     tombol ✅ Kirim / 🚫 Batal — supaya tidak ada broadcast massal ke-klik
     tidak sengaja tanpa sadar.
  3. Setelah dikonfirmasi, broadcast jalan DICICIL PELAN (jeda antar-pesan,
     lihat CAST_DELAY_SECS) dengan penanganan FloodWait (tunggu durasinya,
     retry 1x, lanjut) — SENGAJA pelan, keamanan lebih penting dari
     kecepatan buat fitur yang menyentuh banyak user sekaligus.
  4. Progress diedit berkala di pesan yang sama (bukan spam pesan baru),
     lalu ringkasan akhir (sukses/gagal/block).

CATATAN: user yang sudah block bot / privasi DM off akan gagal terkirim —
ini NORMAL & TIDAK fatal, cuma dihitung sebagai gagal dan lanjut ke user
berikutnya, tidak menghentikan broadcast.
"""

import os
import time
import asyncio

from pyrogram import Client, filters
from pyrogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait

from database import get_all_dm_users

OWNER_ID = int(os.environ.get("OWNER_ID", 0))

# Jeda antar-DM (detik) — SENGAJA konservatif ("cicil pelan") sesuai
# permintaan, bukan dikejar tercepat. Broadcast ke ratusan/ribuan user bisa
# makan waktu beberapa menit — itu wajar & memang tujuannya (aman > cepat).
CAST_DELAY_SECS = float(os.environ.get("CAST_DELAY_SECS", 0.6))

# Update progress tiap N pesan terkirim — supaya owner tidak menunggu buta
# tanpa kabar kalau daftar user banyak, tapi juga tidak spam-edit tiap 1 pesan.
CAST_PROGRESS_EVERY = int(os.environ.get("CAST_PROGRESS_EVERY", 20))

# key: owner_id → HTML teks yang sedang menunggu konfirmasi kirim.
# Single-owner bot, jadi cukup 1 slot — kalau owner kirim /cast baru
# sebelum konfirmasi yang lama, yang lama otomatis ketimpa (tidak ada
# ambiguitas "yang mana yang dikonfirmasi").
_pending_cast: dict[int, str] = {}

# Guard sederhana — cegah 2 broadcast jalan bersamaan (mis. owner double-tap
# tombol konfirmasi, atau lupa broadcast pertama masih jalan).
_cast_running = False


@Client.on_message(filters.command("cast") & filters.private & filters.user(OWNER_ID))
async def cmd_cast(client: Client, message: Message):
    if len(message.command) < 2:
        return await message.reply(
            "Gunakan: <code>/cast [teks]</code>\n\n"
            "Broadcast ke SEMUA user yang pernah DM bot ini (bukan ke grup). "
            "Boleh pakai format <b>Bold</b>/<i>Italic</i>/dll seperti biasa.",
            parse_mode=ParseMode.HTML,
        )

    if _cast_running:
        return await message.reply(
            "⏳ Ada proses <code>/cast</code> lain yang masih berjalan. "
            "Tunggu sampai selesai dulu sebelum mulai yang baru.",
            parse_mode=ParseMode.HTML,
        )

    # Ambil teks ASLI dengan formatting (HTML) — message.command sudah
    # dipecah per-spasi & kehilangan semua entity Bold/Italic/link/dll.
    cast_html = message.text.html.split(None, 1)[1].strip() if message.text else ""
    if not cast_html:
        return await message.reply("❌ Teks broadcast kosong.")

    user_ids = await get_all_dm_users()
    if not user_ids:
        return await message.reply("❌ Belum ada user yang tercatat pernah DM bot ini.")

    _pending_cast[message.from_user.id] = cast_html
    total = len(user_ids)

    await message.reply(
        f"📢 <b>Konfirmasi Broadcast</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Target: <code>{total} user</code> (yang pernah DM bot ini)\n\n"
        f"<b>Preview pesan:</b>\n{cast_html}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Yakin kirim ke semua target di atas?",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Ya, Kirim", callback_data="cast_confirm"),
            InlineKeyboardButton("🚫 Batal", callback_data="cast_cancel"),
        ]]),
    )


@Client.on_callback_query(filters.regex(r"^cast_cancel$"))
async def cb_cast_cancel(client: Client, cb: CallbackQuery):
    if not OWNER_ID or cb.from_user.id != OWNER_ID:
        return await cb.answer("🚫 Hanya owner yang bisa pakai ini.", show_alert=True)
    _pending_cast.pop(cb.from_user.id, None)
    await cb.answer("Broadcast dibatalkan.")
    try:
        await cb.message.edit_text("🚫 <b>Broadcast dibatalkan.</b>", parse_mode=ParseMode.HTML)
    except Exception:
        pass


@Client.on_callback_query(filters.regex(r"^cast_confirm$"))
async def cb_cast_confirm(client: Client, cb: CallbackQuery):
    global _cast_running

    if not OWNER_ID or cb.from_user.id != OWNER_ID:
        return await cb.answer("🚫 Hanya owner yang bisa pakai ini.", show_alert=True)

    cast_html = _pending_cast.pop(cb.from_user.id, None)
    if not cast_html:
        return await cb.answer(
            "⚠️ Sesi konfirmasi ini sudah kedaluwarsa (mis. bot sempat "
            "redeploy). Ketik ulang /cast [teks].",
            show_alert=True,
        )
    if _cast_running:
        return await cb.answer(
            "⏳ Ada broadcast lain yang masih berjalan, tunggu dulu.",
            show_alert=True,
        )

    user_ids = await get_all_dm_users()
    if not user_ids:
        await cb.answer("❌ Tidak ada target (kosong).", show_alert=True)
        return

    await cb.answer("🚀 Broadcast dimulai!")
    total = len(user_ids)
    try:
        await cb.message.edit_text(
            f"📢 <b>Broadcast dimulai...</b>\n\n"
            f"Target: <code>{total}</code> user\n"
            f"Jeda antar-pesan: <code>{CAST_DELAY_SECS}s</code> (dicicil, anti FloodWait)\n\n"
            f"Terkirim: <code>0/{total}</code>",
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass

    _cast_running = True
    ok = fail = 0
    t_start = time.time()
    try:
        for i, uid in enumerate(user_ids, start=1):
            try:
                await client.send_message(uid, cast_html, parse_mode=ParseMode.HTML)
                ok += 1
            except FloodWait as fw:
                # Tunggu durasi yang Telegram minta, lalu retry SEKALI —
                # supaya user ini tidak terlewat cuma karena kena rate
                # limit sesaat (bukan berarti akunnya bermasalah).
                print(f"[cast] ⏳ FloodWait {fw.value}s saat kirim ke {uid} — menunggu lalu retry 1x...")
                await asyncio.sleep(fw.value + 1)
                try:
                    await client.send_message(uid, cast_html, parse_mode=ParseMode.HTML)
                    ok += 1
                except Exception as e2:
                    fail += 1
                    print(f"[cast] gagal retry kirim ke {uid}: {e2}")
            except Exception as e:
                # User block bot / akun dihapus / privasi DM off, dll — TIDAK
                # fatal, hitung gagal & lanjut ke user berikutnya.
                fail += 1
                print(f"[cast] gagal kirim ke {uid}: {e}")

            if i % CAST_PROGRESS_EVERY == 0 or i == total:
                try:
                    await cb.message.edit_text(
                        f"📢 <b>Broadcast berjalan...</b>\n\n"
                        f"Terkirim: <code>{i}/{total}</code>\n"
                        f"✅ Sukses: <code>{ok}</code>  •  🚫 Gagal: <code>{fail}</code>",
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass

            await asyncio.sleep(CAST_DELAY_SECS)
    finally:
        _cast_running = False

    durasi = int(time.time() - t_start)
    try:
        await cb.message.edit_text(
            f"✅ <b>Broadcast selesai!</b>  (durasi ~{durasi} detik)\n\n"
            f"Total target : <code>{total}</code>\n"
            f"✅ Sukses    : <code>{ok}</code>\n"
            f"🚫 Gagal/Block: <code>{fail}</code>",
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass
