"""
plugins/commands/welcome.py
─────────────────────────────
Welcome member baru — dikirim oleh BOT PEMBANTU (bot pemantau Security OS
per-grup, monitor_bot_reference.py), BUKAN bot utama, BUKAN admin.

ALUR:
  1. Bot utama (app) terima ChatMemberUpdated (join) — plugin ini HANYA
     jalan di client bot utama karena MonitorInstance.client dibuat TANPA
     plugins=dict(root="plugins") (lihat security_os/monitor_bot_reference.py),
     jadi tidak ada risiko handler ini terpicu dobel di client bot pembantu.
  2. Kalau grup itu welcome_enabled=True DAN punya bot pembantu aktif
     (_active_instances[chat_id]) → bot utama MEMERINTAHKAN client bot
     pembantu itu langsung (panggil fungsi in-process, bukan lewat API
     HTTP terpisah — keduanya jalan di 1 proses yang sama) untuk kirim
     welcome.
  3. Kalau grup BELUM punya bot pembantu aktif → welcome TIDAK dikirim
     sama sekali (bukan fallback ke bot utama). Ini keputusan sadar: bot
     utama sering TIDAK admin di banyak grup, dan mencampur tanggung jawab
     kirim-pesan-publik ke bot utama membuka risiko flood yang lebih luas
     (bot utama dipakai semua grup sekaligus). /setwelcome akan menolak
     ON kalau bot pembantu belum ada.
  4. Hapus otomatis pesan welcome dijadwalkan ke DB (schedule_welcome_delete)
     lalu dieksekusi oleh welcome_delete_sweep_loop — TIDAK pakai
     asyncio.sleep in-memory murni, supaya tahan restart/redeploy Railway.
     Bot pembantu hapus PESANNYA SENDIRI — tidak butuh hak admin apapun.

PROTEKSI FLOOD:
  - Cooldown ringan per grup (_WELCOME_COOLDOWN) — kalau banyak member join
    beruntun cepat (raid/add massal), welcome ke-2/3/dst dalam window itu
    di-skip, bukan diantre. Mencegah burst send_message ke 1 peer yang sama.
  - FloodWait pendek (<= 5 detik) di-retry sekali; FloodWait panjang di-skip
    (tidak menahan handler join lainnya).
"""

import time
import asyncio

from pyrogram import Client, filters
from pyrogram.types import ChatMemberUpdated, Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ChatMemberStatus, ChatType, ParseMode
from pyrogram.errors import FloodWait

from database import (
    get_config,
    update_config,
    is_admin,
    auto_delete_reply,
    schedule_welcome_delete,
    welcome_delete_db,
)

DELAY_NOTIF = 10
MIN_WELCOME_DELAY = 5
MAX_WELCOME_DELAY = 3600
_STALE_AFTER = 86400  # detik — safety net, buang jadwal hapus yg kelewat 24 jam (bot pembantu mati lama)

_WELCOME_COOLDOWN = 2.0  # detik, jeda minimum antar welcome per grup
_last_welcome_ts: dict[int, float] = {}
_welcome_lock = asyncio.Lock()


async def _welcome_allowed(chat_id: int) -> bool:
    now = time.monotonic()
    async with _welcome_lock:
        last = _last_welcome_ts.get(chat_id, 0.0)
        if now - last < _WELCOME_COOLDOWN:
            return False
        _last_welcome_ts[chat_id] = now
        return True


def _default_text(nama: str, mention: str, grup: str) -> str:
    return (
        f"👋 Selamat datang {mention} di <b>{grup}</b>!\n"
        f"Silakan baca aturan grup dan selamat bergabung 🎉"
    )


def _render_text(template: str, nama: str, mention: str, grup: str) -> str:
    if not template:
        return _default_text(nama, mention, grup)
    try:
        return template.format(mention=mention, nama=nama, grup=grup)
    except Exception:
        return _default_text(nama, mention, grup)


def _build_keyboard(buttons_cfg: list) -> InlineKeyboardMarkup | None:
    """1 tombol URL per baris — tombol dibuat lewat panel Welcome Grup, sudah
    divalidasi & dinormalisasi (skema URL) saat disimpan, jadi di sini tinggal
    dirender."""
    if not buttons_cfg:
        return None
    rows = []
    for b in buttons_cfg:
        text = b.get("text")
        url = b.get("url")
        if text and url:
            rows.append([InlineKeyboardButton(text, url=url)])
    return InlineKeyboardMarkup(rows) if rows else None


# ══════════════════════════════════════════════════════════════════════════
# Handler join member baru — group=11, jalan di client bot UTAMA
# ══════════════════════════════════════════════════════════════════════════

@Client.on_chat_member_updated(group=11)
async def welcome_new_member(client: Client, update: ChatMemberUpdated):
    try:
        new_member = update.new_chat_member
        if not new_member or not new_member.user:
            return
        if new_member.user.is_bot:
            return  # jangan welcome bot lain (Bengkel, monitor bot, dsb)

        new_status = new_member.status
        if new_status not in (ChatMemberStatus.MEMBER, ChatMemberStatus.RESTRICTED):
            return

        old_member = update.old_chat_member
        old_status = old_member.status if old_member else None
        # Hanya trigger untuk join BARU: sebelumnya belum pernah jadi member
        # (None) atau sudah keluar/di-ban. Status update lain (mis. admin
        # promosi/demosi member lama) TIDAK boleh trigger welcome ulang.
        if old_status not in (None, ChatMemberStatus.LEFT, ChatMemberStatus.BANNED):
            return

        chat = update.chat
        if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
            return

        chat_id = chat.id
        cfg = await get_config(chat_id)
        if not cfg.get("welcome_enabled"):
            return

        # Cari bot pembantu grup ini (lazy import — hindari circular import
        # saat startup, sama seperti pola di main.py)
        try:
            from monitor_bot_reference import _active_instances
        except Exception:
            return
        inst = _active_instances.get(chat_id)
        if not inst or not getattr(inst.client, "is_connected", False):
            return  # bot pembantu belum/tidak aktif — tidak kirim apapun

        if not await _welcome_allowed(chat_id):
            return

        user = new_member.user
        nama = user.first_name or "Member"
        mention = user.mention(nama)
        grup = chat.title or str(chat_id)
        text = _render_text(cfg.get("welcome_text", ""), nama, mention, grup)
        photo = cfg.get("welcome_photo", "")
        keyboard = _build_keyboard(cfg.get("welcome_buttons") or [])

        async def _do_send(parse_mode, use_photo):
            if use_photo and photo:
                return await inst.client.send_photo(
                    chat_id, photo=photo, caption=text,
                    parse_mode=parse_mode, reply_markup=keyboard,
                )
            return await inst.client.send_message(
                chat_id, text, parse_mode=parse_mode, reply_markup=keyboard,
            )

        try:
            sent = await _do_send(ParseMode.HTML, use_photo=True)
        except FloodWait as fw:
            if fw.value <= 5:
                await asyncio.sleep(fw.value)
                try:
                    sent = await _do_send(ParseMode.HTML, use_photo=True)
                except Exception as e:
                    print(f"[welcome] retry gagal chat={chat_id}: {e}")
                    return
            else:
                print(f"[welcome] FloodWait {fw.value}s chat={chat_id} — di-skip")
                return
        except Exception as e:
            # Tier 2: kemungkinan besar teks custom admin mengandung HTML
            # tidak valid (tag tidak ditutup, dsb). Jangan biarkan 1 template
            # rusak mematikan welcome untuk SEMUA join berikutnya — fallback
            # kirim tanpa parse_mode (plain text), tetap dengan foto/tombol.
            print(f"[welcome] gagal kirim HTML chat={chat_id}: {e} — fallback plain text+foto")
            try:
                sent = await _do_send(None, use_photo=True)
            except Exception as e2:
                # Tier 3: kemungkinan bot pembantu kehilangan izin kirim media
                # di grup ini (mis. admin cabut izin "Kirim Media" setelah
                # foto welcome disimpan) — CHAT_SEND_PHOTOS_FORBIDDEN dkk.
                # Jangan biarkan welcome mati total karena foto gagal;
                # turunkan jadi teks saja (tanpa foto), tetap dengan tombol.
                print(f"[welcome] kirim foto gagal chat={chat_id}: {e2} — fallback teks saja (tanpa foto)")
                try:
                    sent = await _do_send(None, use_photo=False)
                except Exception as e3:
                    print(f"[welcome] fallback teks saja juga gagal chat={chat_id}: {e3}")
                    return

        delay = int(cfg.get("welcome_delay", 30) or 30)
        delay = max(MIN_WELCOME_DELAY, min(MAX_WELCOME_DELAY, delay))
        await schedule_welcome_delete(chat_id, sent.id, delay)

    except Exception as e:
        print(f"[welcome_new_member] {e}")


# ══════════════════════════════════════════════════════════════════════════
# Sweep loop — eksekusi jadwal hapus, lewat client bot pembantu yang sesuai
# (dipanggil sekali sebagai asyncio.create_task di main.py)
# ══════════════════════════════════════════════════════════════════════════

async def welcome_delete_sweep_loop() -> None:
    from monitor_bot_reference import _active_instances

    print("[welcome_delete_sweep_loop] ✅ Siap.", flush=True)
    while True:
        try:
            now = time.time()
            due = await welcome_delete_db.find({"delete_at": {"$lt": now}}).to_list(None)
            if due:
                grouped: dict[int, list[dict]] = {}
                for doc in due:
                    grouped.setdefault(doc["chat_id"], []).append(doc)

                for chat_id, docs in grouped.items():
                    stale = [d for d in docs if now - d.get("created_at", now) > _STALE_AFTER]
                    pending = [d for d in docs if d not in stale]

                    for d in stale:
                        await welcome_delete_db.delete_one(
                            {"chat_id": d["chat_id"], "message_id": d["message_id"]}
                        )

                    if not pending:
                        continue

                    inst = _active_instances.get(chat_id)
                    if not inst or not getattr(inst.client, "is_connected", False):
                        continue  # coba lagi siklus berikutnya, jadwal tetap di DB

                    mids = [d["message_id"] for d in pending]
                    try:
                        await inst.client.delete_messages(chat_id, mids)
                    except Exception as e:
                        print(f"[welcome_delete_sweep_loop] gagal hapus chat={chat_id}: {e}")

                    for d in pending:
                        await welcome_delete_db.delete_one(
                            {"chat_id": d["chat_id"], "message_id": d["message_id"]}
                        )
        except Exception as e:
            print(f"[welcome_delete_sweep_loop] error: {e}")

        await asyncio.sleep(5)


# ══════════════════════════════════════════════════════════════════════════
# Command: /setwelcome on|off, /setwelcomedelay <detik>, /setwelcometext <teks>
# ══════════════════════════════════════════════════════════════════════════

@Client.on_message(
    filters.command(["setwelcome", "setwelcomedelay", "setwelcometext"]) & (filters.group | filters.forum)
)
async def welcome_settings_handler(client: Client, message: Message):
    cid = message.chat.id
    uid = message.from_user.id if message.from_user else None

    if not await is_admin(client, cid, uid):
        return

    cmd = message.command[0].lower()

    if cmd == "setwelcome":
        if len(message.command) < 2 or message.command[1].lower() not in ["on", "off"]:
            res = await message.reply("⚠️ Format salah. Contoh: <code>/setwelcome on</code>", parse_mode=ParseMode.HTML)
            asyncio.create_task(auto_delete_reply([res, message], delay=DELAY_NOTIF))
            return

        val = message.command[1].lower() == "on"

        if val:
            try:
                from monitor_bot_reference import _active_instances
            except Exception:
                _active_instances = {}
            inst = _active_instances.get(cid)
            if not inst or not getattr(inst.client, "is_connected", False):
                res = await message.reply(
                    "⚠️ Welcome butuh <b>bot pembantu</b> (bot pemantau Security OS) "
                    "aktif dulu di grup ini. Aktifkan Security OS dulu, baru "
                    "<code>/setwelcome on</code>.",
                    parse_mode=ParseMode.HTML,
                )
                asyncio.create_task(auto_delete_reply([res, message], delay=DELAY_NOTIF))
                return

        await update_config(cid, "welcome_enabled", val)
        icon = "🟢" if val else "🔴"
        res = await message.reply(f"👋 Welcome Member Baru → {icon} <b>{'ON' if val else 'OFF'}</b>", parse_mode=ParseMode.HTML)
        asyncio.create_task(auto_delete_reply([res, message], delay=DELAY_NOTIF))

    elif cmd == "setwelcomedelay":
        if len(message.command) < 2 or not message.command[1].isdigit():
            res = await message.reply("⚠️ Format salah. Contoh: <code>/setwelcomedelay 30</code> (detik)", parse_mode=ParseMode.HTML)
            asyncio.create_task(auto_delete_reply([res, message], delay=DELAY_NOTIF))
            return
        detik = max(MIN_WELCOME_DELAY, min(MAX_WELCOME_DELAY, int(message.command[1])))
        await update_config(cid, "welcome_delay", detik)
        res = await message.reply(f"⏱️ Welcome dihapus otomatis setelah → <code>{detik} detik</code>", parse_mode=ParseMode.HTML)
        asyncio.create_task(auto_delete_reply([res, message], delay=DELAY_NOTIF))

    elif cmd == "setwelcometext":
        if len(message.command) < 2:
            res = await message.reply(
                "⚠️ Format salah. Contoh:\n<code>/setwelcometext Halo {mention}, selamat datang di {grup}!</code>\n\n"
                "Placeholder: <code>{mention}</code> <code>{nama}</code> <code>{grup}</code>\n"
                "Kirim <code>/setwelcometext reset</code> untuk kembali ke default.",
                parse_mode=ParseMode.HTML,
            )
            asyncio.create_task(auto_delete_reply([res, message], delay=DELAY_NOTIF))
            return
        raw = message.text.split(None, 1)[1]
        if raw.strip().lower() == "reset":
            await update_config(cid, "welcome_text", "")
            res = await message.reply("♻️ Teks welcome dikembalikan ke default.", parse_mode=ParseMode.HTML)
        else:
            await update_config(cid, "welcome_text", raw)
            res = await message.reply("✅ Teks welcome custom disimpan.", parse_mode=ParseMode.HTML)
        asyncio.create_task(auto_delete_reply([res, message], delay=DELAY_NOTIF))
