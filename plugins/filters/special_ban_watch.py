"""
plugins/filters/special_ban_watch.py
───────────────────────────────────────────────────────────────────────────────
Pasang listener event Telegram real-time: begitu status member berubah jadi
BANNED di grup manapun yang dikenal bot, cek apakah user yang di-ban itu ada
di daftar SPECIAL_BAN_IDS (.env). Kalau ya → bot auto-leave grup itu.

Logika inti ada di core/special_ban_watchdog.py (biar konsisten dengan pola
core/perm_watchdog.py + plugins/filters/cas.py::handle_bot_status_change).
Fungsi di sini sengaja tipis — cuma filter + delegasi via asyncio.create_task,
supaya dispatcher event tidak nge-block menunggu leave_chat/remove_group_data
selesai.

Tidak perlu diaktifkan/dinonaktifkan manual — kalau SPECIAL_BAN_IDS kosong di
.env, is_watched_id() akan selalu False sehingga handler ini efektif no-op.
"""

import asyncio

from pyrogram import Client
from pyrogram.enums import ChatMemberStatus

from core.special_ban_watchdog import is_watched_id, handle_watched_ban


@Client.on_chat_member_updated(group=7)
async def handle_special_ban_watch(client: Client, update):
    try:
        new_member = update.new_chat_member
        if not new_member or new_member.status != ChatMemberStatus.BANNED:
            return

        banned_user = getattr(new_member, "user", None)
        if not banned_user:
            return

        banned_user_id = banned_user.id
        if not is_watched_id(banned_user_id):
            return

        chat_id = update.chat.id
        asyncio.create_task(handle_watched_ban(client, chat_id, banned_user_id))

    except Exception as e:
        print(f"[SpecialBanWatch] ❌ Error tak terduga: {e}")
