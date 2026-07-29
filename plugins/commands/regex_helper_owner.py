"""
plugins/commands/regex_helper_owner.py
────────────────────────────────────────
Perintah khusus OWNER untuk kelola Regex Helper — gerbang penerjemah kata
ambigu yang jalan SEBELUM AI Manual (lihat core/regex_helper.py).

v8.0 — Format disederhanakan total. DULU owner cuma isi kata baku dan
sistem generate puluhan pola mutasi liar (regex kompleks, sulit ditebak).
SEKARANG owner isi langsung daftar persamaan kata secara eksplisit —
literal, bukan mutasi. Cocok buat ISTILAH BARU (bukan typo dari kata baku,
tapi kata yang beda sama sekali dengan arti sama).

  /addregexhelper katabaku=persamaan|persamaan|persamaan
  contoh: /addregexhelper chat=pc|dm|pv|dll

  /delregexhelper katabaku
  /infohelper
"""

import re
import os
import html

from pyrogram import Client, filters
from pyrogram.enums import ParseMode

from database import regex_helper_db
from core.regex_helper import build_helper_pattern_literal, invalidate_helper_cache

OWNER_ID = int(os.environ.get("OWNER_ID", 0))

_USAGE = (
    "❌ **Sintaks Salah!**\n"
    "Gunakan: `/addregexhelper katabaku=persamaan|persamaan|persamaan`\n\n"
    "Contoh: `/addregexhelper chat=pc|dm|pv|dll`\n\n"
    "⚙️ Kata sebelum `=` adalah kata baku (hasil terjemahan). Kata-kata "
    "setelah `=`, dipisah `|`, adalah persamaan/istilah lain yang berarti "
    "sama. Tiap persamaan dicocokkan PERSIS (bukan mutasi/typo otomatis) "
    "— jadi kata apapun di grup yang persis sama salah satu persamaan "
    "akan diterjemahkan jadi kata baku SEBELUM masuk AI Manual."
)


@Client.on_message(
    filters.command(["addregexhelper", "delregexhelper", "infohelper"]) & filters.user(OWNER_ID)
)
async def regex_helper_management(client, message):
    cmd = message.command[0].lower()

    # ── /addregexhelper katabaku=persamaan|persamaan|persamaan ─────────────
    if cmd == "addregexhelper":
        if len(message.command) < 2:
            return await message.reply(_USAGE, parse_mode=ParseMode.MARKDOWN)

        raw = message.text.split(None, 1)[1].strip()

        if "=" not in raw:
            return await message.reply(_USAGE, parse_mode=ParseMode.MARKDOWN)

        kata_baku_raw, _, persamaan_raw = raw.partition("=")
        kata_baku_raw = kata_baku_raw.strip()
        daftar_persamaan = persamaan_raw.split("|")

        try:
            pola, persamaan_bersih = build_helper_pattern_literal(kata_baku_raw, daftar_persamaan)
        except ValueError as e:
            return await message.reply(f"❌ **Gagal Simpan:** {e}")

        kata_key = re.sub(r"[^\w]", "", kata_baku_raw, flags=re.UNICODE).strip().lower()

        await regex_helper_db.update_one(
            {"kata_baku": kata_key},
            {"$set": {
                "kata_baku": kata_key,
                "pola":      pola,
                "persamaan": persamaan_bersih,
                "jumlah_mutasi": len(persamaan_bersih),
            }},
            upsert=True,
        )
        invalidate_helper_cache()

        daftar_str = ", ".join(f"`{html.escape(p)}`" for p in persamaan_bersih)
        await message.reply(
            f"✅ **REGEX HELPER TERSIMPAN!**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📝 **Kata baku:** `{html.escape(kata_key)}`\n"
            f"🔀 **Persamaan ({len(persamaan_bersih)}):** {daftar_str}\n\n"
            f"Kata apapun di grup yang PERSIS sama salah satu persamaan di "
            f"atas akan otomatis diterjemahkan jadi `{html.escape(kata_key)}` "
            f"sebelum masuk AI Manual.",
            parse_mode=ParseMode.MARKDOWN,
        )

    # ── /delregexhelper katabaku ─────────────────────────────────────────────
    elif cmd == "delregexhelper":
        if len(message.command) < 2:
            return await message.reply("Gunakan: `/delregexhelper [katabaku]`")

        kata_key = re.sub(r"[^\w]", "", " ".join(message.command[1:]), flags=re.UNICODE).strip().lower()
        result = await regex_helper_db.delete_one({"kata_baku": kata_key})
        invalidate_helper_cache()

        if result.deleted_count:
            await message.reply(f"✅ Regex Helper `{html.escape(kata_key)}` berhasil dihapus.", parse_mode=ParseMode.HTML)
        else:
            await message.reply(
                f"❌ **Data Not Found!**\n\nKata `{html.escape(kata_key)}` tidak ada di database.\n"
                f"Cek dengan `/infohelper`.",
                parse_mode=ParseMode.MARKDOWN,
            )

    # ── /infohelper ──────────────────────────────────────────────────────────
    elif cmd == "infohelper":
        docs = [doc async for doc in regex_helper_db.find({})]
        if docs:
            lines = "\n".join(
                f"<code>{html.escape(d.get('kata_baku', '—'))}</code> "
                f"— {d.get('jumlah_mutasi', '?')} persamaan"
                for d in docs
            )
            text = (
                "<b>REGEX HELPER — GERBANG PENERJEMAH</b>\n\n"
                f"⚡ Total Entri: <code>{len(docs)}</code>\n\n"
                "<b>KATA BAKU:</b>\n\n"
                f"{lines}\n\n"
                "<b>Cara hapus:</b>\n"
                "<code>/delregexhelper [katabaku]</code>"
            )
        else:
            text = (
                "📭 Regex Helper masih kosong. Tambah dengan "
                "`/addregexhelper katabaku=persamaan|persamaan|persamaan`."
            )
        await message.reply(text, parse_mode=ParseMode.HTML)
