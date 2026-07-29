"""
plugins/commands/mention_cache_reset.py
─────────────────────────────────────────
Perintah admin grup: /resetmentioncache

Membersihkan PAKSA seluruh cache "Batasi Tag Akun" (mention_member_cache)
untuk grup tempat command dijalankan — mencakup entri yang sudah
terverifikasi MEMBER maupun NON-MEMBER, dua-duanya, bukan cuma salah satu.
Setelah di-flush, setiap @username yang di-tag lagi akan di-resolve ULANG
dari API Telegram (bukan dari cache lama).

Dibatasi COOLDOWN per grup (default 6 jam) supaya tidak bisa dipakai
berulang-ulang oleh admin (mis. untuk trial-and-error atau disalahgunakan
buat memaksa banyak API call berturut-turut). Cooldown disimpan persisten
via bot_config (save_bot_config/get_bot_config) — bertahan lintas restart.

Tidak menyentuh mention_global_cache (non_akun/channel/grup lintas semua
grup) — itu memang dirancang lintas grup dan TTL-nya sendiri sudah wajar.
"""

import asyncio
import time

from pyrogram import Client, filters
from pyrogram.enums import ParseMode

from database import (
    is_admin,
    mention_cache_flush_group,
    insert_group_action_log,
    auto_delete_reply,
    save_bot_config,
    get_bot_config,
)

DELAY_NOTIF = 10
COOLDOWN_SECS = 6 * 3600  # 6 jam sekali per grup


def _fmt_sisa(detik: float) -> str:
    detik = int(detik)
    jam, sisa = divmod(detik, 3600)
    menit = sisa // 60
    if jam > 0:
        return f"{jam} jam {menit} menit"
    return f"{menit} menit"


@Client.on_message(filters.command("resetmentioncache") & (filters.group | filters.forum))
async def cmd_reset_mention_cache(client: Client, message):
    cid = message.chat.id
    uid = message.from_user.id if message.from_user else None

    if not await is_admin(client, cid, uid):
        return

    cd_key = f"mention_flush_cooldown_{cid}"
    last_ts = await get_bot_config(cd_key, 0.0)
    now = time.time()
    elapsed = now - float(last_ts or 0.0)

    if elapsed < COOLDOWN_SECS:
        sisa = COOLDOWN_SECS - elapsed
        res = await message.reply(
            "⏳ <b>Belum bisa dipakai lagi.</b>\n\n"
            f"Perintah ini dibatasi maksimal sekali per {COOLDOWN_SECS // 3600} jam "
            "per grup, supaya tidak jadi celah buat memaksa banyak API call "
            "berturut-turut.\n\n"
            f"Coba lagi dalam <b>{_fmt_sisa(sisa)}</b>.",
            parse_mode=ParseMode.HTML,
        )
        asyncio.create_task(auto_delete_reply([res, message], delay=DELAY_NOTIF))
        return

    deleted = await mention_cache_flush_group(cid)
    await save_bot_config(cd_key, now)

    asyncio.create_task(insert_group_action_log(
        chat_id=cid,
        aksi="RESET-CACHE",
        alasan=f"Admin flush mention cache ({deleted} entri dihapus)",
        user_id=uid or 0,
        user_name=message.from_user.first_name if message.from_user else "-",
    ))

    res = await message.reply(
        "🧹 <b>Cache Tag Akun grup ini dibersihkan.</b>\n\n"
        f"• {deleted} entri dihapus (member ✅ & non-member ❌ sekaligus)\n"
        "• @username yang di-tag lagi setelah ini akan dicek ulang dari API, "
        "bukan dari cache lama.\n\n"
        f"⏳ Perintah ini bisa dipakai lagi dalam {COOLDOWN_SECS // 3600} jam.",
        parse_mode=ParseMode.HTML,
    )
    asyncio.create_task(auto_delete_reply([res, message], delay=DELAY_NOTIF))
