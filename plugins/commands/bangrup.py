"""
plugins/commands/bangrup.py
──────────────────────────────
Perintah owner (DM only): /bangrup, /unbangrup, /listbangrup, /teksbangrup

/teksbangrup <teks>
    - Simpan teks custom yang dikirim ke grup SEBELUM bot keluar via
      /bangrup. Disimpan lewat save_bot_config (key BANGRUP_TEKS_KEY) —
      SATU slot global, tiap dipanggil ulang teks lama otomatis TERTIMPA
      (upsert), bukan ditambah/diarsipkan.
    - Kalau belum pernah diset sama sekali, /bangrup akan skip tahap
      kirim teks dan langsung lanjut ke tahap keluar (fallback aman).

/bangrup <chat_id>
    URUTAN EKSEKUSI (sesuai permintaan owner):
      1. Simpan chat_id ke koleksi banned_groups (bangrup_add) — DI DEPAN,
         supaya enforcement re-add sudah aktif walau langkah di bawah gagal.
      2. Kalau ada teks custom tersimpan (/teksbangrup) DAN bot masih ada
         di grup itu → kirim teks itu ke grup.
      3. Tunggu BANGRUP_LEAVE_DELAY detik (default 5, via .env).
      4. Bot utama leave_chat().
      5. Userbot main (security_os/video_call.py, userbot bawaan owner)
         leave_chat() juga — HANYA kalau dia memang sedang jadi member
         di grup itu.
    - Selama grup itu ada di banned_groups, bot akan otomatis leave_chat()
      lagi setiap kali di-invite ulang (lihat handler
      _bangrup_enforce_on_join_service / _bangrup_enforce_on_join_status di
      bawah — dua jalur redundant, service message + chat_member update,
      supaya menutup grup sebelum handler lain sempat memproses join
      sebagai grup aktif normal). Jalur enforcement ini TIDAK memakai
      teks custom/delay — langsung leave, karena ini reaksi ke percobaan
      re-add, bukan aksi ban yang pertama kali.

/unbangrup <chat_id>
    - Hapus chat_id dari banned_groups. Bot boleh di-add lagi setelah ini.

/listbangrup
    - Tampilkan semua grup yang sedang di-bangrup (ID + judul terakhir
      diketahui + waktu + siapa yang nge-ban).
"""

from __future__ import annotations

import os
import html
import asyncio
from datetime import datetime

from pyrogram import Client, filters
from pyrogram.types import Message, ChatMemberUpdated
from pyrogram.enums import ChatMemberStatus, ParseMode

from database import (
    bangrup_add,
    bangrup_remove,
    bangrup_is_banned,
    bangrup_list,
    save_bot_config,
    get_bot_config,
)

_OWNER_ID   = int(os.environ.get("OWNER_ID", 0))
_TZ_LABEL   = "%d/%m/%Y %H:%M:%S"

BANGRUP_TEKS_KEY   = "bangrup_teks_custom"
BANGRUP_LEAVE_DELAY = int(os.environ.get("BANGRUP_LEAVE_DELAY", 5))  # detik, jeda kirim teks → leave


def _get_main_userbot():
    """
    Ambil instance userbot bawaan owner (bukan Custom Userbot per-grup).
    Import lazy + akses lewat modul (bukan `from ... import userbot`)
    karena variabelnya di-reassign saat start_userbot() jalan — import
    langsung akan ke-bind ke None selamanya kalau di-import di top-level
    sebelum userbot benar-benar konek.
    """
    try:
        import security_os.video_call as _vc
        return _vc.userbot
    except Exception:
        return None


def _parse_chat_id(raw: str) -> int | None:
    raw = raw.strip()
    try:
        return int(raw)
    except ValueError:
        return None


# ── /teksbangrup <teks> ──────────────────────────────────────────────────────
@Client.on_message(
    filters.command("teksbangrup") & filters.private & filters.user(_OWNER_ID)
)
async def cmd_teksbangrup(client: Client, message: Message):
    if not _OWNER_ID:
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        current = await get_bot_config(BANGRUP_TEKS_KEY, default=None)
        if current:
            await message.reply(
                f"📝 <b>Teks bangrup saat ini:</b>\n\n{html.escape(current)}\n\n"
                f"Ganti dengan: <code>/teksbangrup teks baru</code>",
                parse_mode=ParseMode.HTML,
            )
        else:
            await message.reply(
                "⚠️ <b>Format salah.</b>\nContoh: <code>/teksbangrup Grup ini melanggar ketentuan, bot ditarik.</code>\n\n"
                "Belum ada teks tersimpan.",
                parse_mode=ParseMode.HTML,
            )
        return

    teks_baru = parts[1].strip()
    # upsert — teks lama otomatis TERTIMPA, bukan diarsipkan
    await save_bot_config(BANGRUP_TEKS_KEY, teks_baru)

    await message.reply(
        f"✅ <b>Teks bangrup diperbarui.</b>\n\n"
        f"Teks lama sudah digantikan. Isi baru:\n\n{html.escape(teks_baru)}",
        parse_mode=ParseMode.HTML,
    )


# ── /bangrup <chat_id> ──────────────────────────────────────────────────────
@Client.on_message(
    filters.command("bangrup") & filters.private & filters.user(_OWNER_ID)
)
async def cmd_bangrup(client: Client, message: Message):
    if not _OWNER_ID:
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(
            "⚠️ <b>Format salah.</b>\nContoh: <code>/bangrup -1001234567890</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    chat_id = _parse_chat_id(parts[1])
    if chat_id is None:
        await message.reply("⚠️ ID grup tidak valid.")
        return

    title = str(chat_id)
    try:
        chat = await client.get_chat(chat_id)
        title = chat.title or title
    except Exception:
        pass

    # Langkah 1 — catat ban DULU, sebelum apapun lain, supaya enforcement
    # re-add sudah aktif walau langkah kirim-teks/leave di bawah gagal.
    await bangrup_add(chat_id, banned_by=message.from_user.id, title=title)

    await message.reply(
        f"🚫 <b>Proses Bangrup Dimulai</b>\n"
        f"◈ <b>Grup:</b> {html.escape(title)}\n"
        f"◈ <b>ID:</b> <code>{chat_id}</code>\n\n"
        f"Kirim teks custom (jika ada) → tunggu {BANGRUP_LEAVE_DELAY} detik → "
        f"bot utama & userbot main keluar.",
        parse_mode=ParseMode.HTML,
    )

    asyncio.create_task(
        _run_bangrup_sequence(client, chat_id, title, message.from_user.id)
    )


async def _run_bangrup_sequence(client: Client, chat_id: int, title: str, owner_id: int) -> None:
    """
    Urutan aktual: kirim teks custom (kalau ada & bot masih di grup) →
    jeda BANGRUP_LEAVE_DELAY detik → leave bot utama → leave userbot main
    (kalau dia juga member di grup itu). Jalan di background supaya
    command /bangrup tidak nge-block nunggu delay.
    """
    teks_status = "Tidak ada teks custom tersimpan — dilewati."
    teks = await get_bot_config(BANGRUP_TEKS_KEY, default=None)
    if teks:
        try:
            await client.send_message(chat_id, teks)
            teks_status = "Teks custom terkirim ke grup."
        except Exception as e:
            teks_status = f"Gagal kirim teks custom ({e}) — tetap lanjut ke tahap leave."

    await asyncio.sleep(BANGRUP_LEAVE_DELAY)

    # ── FIX: putus dulu sesi VC userbot utama (kalau ada) SEBELUM leave_chat ──
    # Root cause bug: leave_chat() cuma keluar dari GRUP (membership biasa),
    # TIDAK memutus sesi voice chat PyTgCalls yang mungkin sedang aktif lewat
    # security_os/vc_stream_main.py (mode eksperimen MAIN_UB_MULTI_VC=1).
    # Kalau ini dilewati, grup yang sudah di-bangrup (bot sudah leave) masih
    # bisa terus menerima update mentah UpdatedGroupCallParticipant, karena
    # sesi WebRTC-nya sendiri belum pernah di-leave_call() secara eksplisit.
    # leave_main_vc_stream() aman dipanggil walau modul tidak aktif/app belum
    # start (langsung return True tanpa efek) — jadi tidak perlu dibungkus
    # pengecekan MAIN_UB_MULTI_VC di sini.
    vc_status = "Userbot utama tidak sedang streaming VC di grup ini."
    try:
        from security_os.vc_stream_main import is_main_streaming, leave_main_vc_stream
        if is_main_streaming(chat_id):
            await leave_main_vc_stream(chat_id)
            vc_status = "Sesi VC userbot utama berhasil diputus."
    except Exception as e:
        vc_status = f"Gagal cek/putus sesi VC userbot utama ({e}) — tetap lanjut ke tahap leave."

    bot_status = "Bot tidak sedang ada di grup ini."
    try:
        await client.leave_chat(chat_id)
        bot_status = "Bot utama berhasil keluar."
    except Exception as e:
        if "USER_NOT_PARTICIPANT" not in str(e) and "CHAT_ID_INVALID" not in str(e):
            bot_status = f"Bot utama gagal leave ({e}) — tetap akan dipaksa keluar kalau ke-detect member lagi."

    ub_status = "Userbot main tidak sedang ada di grup ini / tidak aktif."
    ub = _get_main_userbot()
    if ub is not None:
        try:
            await ub.leave_chat(chat_id)
            ub_status = "Userbot main berhasil keluar."
        except Exception as e:
            if "USER_NOT_PARTICIPANT" not in str(e) and "CHAT_ID_INVALID" not in str(e):
                ub_status = f"Userbot main gagal leave ({e})."

    if owner_id:
        try:
            await client.send_message(
                owner_id,
                f"🚫 <b>Bangrup Selesai</b>\n"
                f"◈ <b>Grup:</b> {html.escape(title)}\n"
                f"◈ <b>ID:</b> <code>{chat_id}</code>\n"
                f"◈ <b>Teks custom:</b> {teks_status}\n"
                f"◈ <b>Sesi VC userbot utama:</b> {vc_status}\n"
                f"◈ <b>Bot utama:</b> {bot_status}\n"
                f"◈ <b>Userbot main:</b> {ub_status}\n\n"
                f"Grup ini tidak bisa nambahin bot lagi sampai <code>/unbangrup {chat_id}</code>.",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass


# ── /unbangrup <chat_id> ─────────────────────────────────────────────────────
@Client.on_message(
    filters.command("unbangrup") & filters.private & filters.user(_OWNER_ID)
)
async def cmd_unbangrup(client: Client, message: Message):
    if not _OWNER_ID:
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(
            "⚠️ <b>Format salah.</b>\nContoh: <code>/unbangrup -1001234567890</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    chat_id = _parse_chat_id(parts[1])
    if chat_id is None:
        await message.reply("⚠️ ID grup tidak valid.")
        return

    removed = await bangrup_remove(chat_id)
    if removed:
        await message.reply(
            f"✅ <b>Grup Di-unbangrup</b>\n◈ <b>ID:</b> <code>{chat_id}</code>\n\n"
            f"Bot sudah boleh di-add lagi ke grup ini.",
            parse_mode=ParseMode.HTML,
        )
    else:
        await message.reply(f"ℹ️ ID <code>{chat_id}</code> tidak ada di daftar bangrup.", parse_mode=ParseMode.HTML)


# ── /listbangrup ──────────────────────────────────────────────────────────────
@Client.on_message(
    filters.command("listbangrup") & filters.private & filters.user(_OWNER_ID)
)
async def cmd_listbangrup(client: Client, message: Message):
    if not _OWNER_ID:
        return

    docs = await bangrup_list()
    if not docs:
        await message.reply("📋 Belum ada grup yang di-bangrup.")
        return

    lines = [f"📋 <b>Daftar Grup Di-bangrup ({len(docs)})</b>\n"]
    for doc in docs:
        cid   = doc.get("_id")
        title = html.escape(doc.get("title") or str(cid))
        ts    = doc.get("banned_at", 0)
        by    = doc.get("banned_by", "-")
        waktu = datetime.fromtimestamp(ts).strftime(_TZ_LABEL) if ts else "-"
        lines.append(
            f"◈ <b>{title}</b>\n"
            f"   ID: <code>{cid}</code> | Oleh: <code>{by}</code> | {waktu}"
        )

    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:3990] + "\n… (terpotong)"

    await message.reply(text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)


# ── Enforcement — tolak otomatis kalau di-add ke grup yang sedang di-bangrup ──
#
# FIX: sebelumnya HANYA pakai on_chat_member_updated (event "chat_member").
# Owner melaporkan bot masih bisa nempel di grup terlarang tanpa keluar lagi
# — indikasi event chat_member tidak selalu terkirim reliable. Codebase ini
# SENDIRI sudah punya bukti jalur yang pasti jalan: cas.py → handle_bot_join
# (filters.service + message.new_chat_members, dipakai buat pesan welcome
# yang terbukti selalu muncul). Sekarang dipasang DUA jalur sekaligus,
# redundant, biar apapun event yang benar-benar terkirim tetap ke-tangkep:
#   1. _bangrup_enforce_on_join_service — service message (PALING DIANDALKAN)
#   2. _bangrup_enforce_on_join_status  — chat_member update (cadangan)

async def _bangrup_kick_if_banned(client: Client, chat_id: int, chat_title: str) -> None:
    """Helper bersama: cek banned_groups, leave_chat kalau kena, notif owner."""
    if not await bangrup_is_banned(chat_id):
        return

    # ── Jaring pengaman sama seperti _run_bangrup_sequence: pastikan sesi VC
    # userbot utama (kalau ada, lewat vc_stream_main.py) ikut diputus di
    # setiap kesempatan enforcement, bukan cuma sekali waktu /bangrup awal.
    try:
        from security_os.vc_stream_main import is_main_streaming, leave_main_vc_stream
        if is_main_streaming(chat_id):
            await leave_main_vc_stream(chat_id)
    except Exception as e:
        print(f"[bangrup] gagal cek/putus sesi VC userbot utama {chat_id}: {e}")

    try:
        await client.leave_chat(chat_id)
    except Exception as e:
        print(f"[bangrup] gagal leave grup terlarang {chat_id}: {e}")
        return

    if _OWNER_ID:
        try:
            await client.send_message(
                _OWNER_ID,
                f"🚫 <b>Percobaan add ke grup terlarang</b>\n"
                f"◈ <b>Grup:</b> {html.escape(chat_title or str(chat_id))}\n"
                f"◈ <b>ID:</b> <code>{chat_id}</code>\n"
                f"Bot otomatis keluar lagi (grup ini masih di-bangrup).",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass


# 1) Jalur utama — service message "bot ditambahkan ke grup"
@Client.on_message(filters.service, group=0)
async def _bangrup_enforce_on_join_service(client: Client, message: Message):
    try:
        if not message.new_chat_members:
            return

        me = client.me
        if not any(m.id == me.id for m in message.new_chat_members):
            return  # bukan bot ini yang ditambahkan

        await _bangrup_kick_if_banned(client, message.chat.id, message.chat.title)
    except Exception as e:
        print(f"[bangrup] enforce_on_join_service error: {e}")


# 2) Jalur cadangan — event chat_member (kadang tidak terkirim reliable,
#    tapi tetap dipasang sebagai jaring pengaman kalau ada kasus join yang
#    tidak memicu service message, mis. lewat link invite langsung).
# group=1: jalan awal, sebelum handler tracking/trial/dsb (group=7/8/9)
# sempat memproses join ini sebagai grup aktif biasa.
@Client.on_chat_member_updated(group=1)
async def _bangrup_enforce_on_join_status(client: Client, update: ChatMemberUpdated):
    try:
        me = client.me
        new_member = update.new_chat_member
        if not new_member or not new_member.user or new_member.user.id != me.id:
            return  # bukan update soal bot ini sendiri

        status = new_member.status
        if status not in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR):
            return  # bukan event "bot baru ditambahkan/di-promote"

        await _bangrup_kick_if_banned(client, update.chat.id, update.chat.title)
    except Exception as e:
        print(f"[bangrup] enforce_on_join_status error: {e}")
