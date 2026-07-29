"""
plugins/nexus/nexus_handlers.py
────────────────────────────────
Panel Nexus AI — /nexus di DM & Direct Injection Command.

v8.0 — Tombol "⚡ Paksa Kalkulasi" & toggle "🔄 Auto-Generate" DIHAPUS
(fitur auto-generate regex dari korpus tengah malam sudah tidak ada,
lihat plugins/nexus/engine.py). Record Data sekarang menyimpan raw teks
TAHAP 1 yang diperiksa Groq TAHAP 2, bukan korpus regex.

STRUKTUR MENU:
  [Menu Utama Nexus]
  ┌─ ➕ AKTIFKAN DI GRUP
  ├─ 📚 BUKU MANUAL AI  │  🧪 LAB UJI SANDBOX
  ├─ 🔮 GLOBAL REGEX    │  👑 OWNER BOT
  └─ 🔙 MENU UTAMA BOT

  [Sub-menu OWNER BOT]
  ┌─ 📊 RECORD DATA     │  📂 GRUP TERDAFTAR
  ├─ 🔄 REFRESH METRIK   │  🧠 LIHAT AI
  ├─ 📋 LOG AKTIVITAS    │  🔬 DEBUG AI (24j)
  ├─ 🗑️ RESET INTEGRASI
  ├─ 📱 GANTI USERBOT
  └─ 🔙 KEMBALI KE NEXUS

  [Sub-menu GLOBAL REGEX]
  ┌─ 🧬 VISUALISASI FILTER
  └─ ⚙️ TRIGGER AI
"""

import re
import asyncio
import unicodedata
from html import escape as _html_escape

from nexus.ai_core.constants import NEXUS_MIN_CONFIDENCE
from nexus.ai_core import nexus_ai_full_reset, get_nexus_ai

import pytz
from pyrogram import Client, filters, ContinuePropagation
from pyrogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton, ForceReply,
)
from pyrogram.enums import ParseMode
from pyrogram.errors import MessageNotModified

from database import (
    nexus_get_kalimat_count,
    nexus_get_regex_count,
    nexus_get_regex_page,
    nexus_get_kalimat_page,
    get_all_groups_with_perm_status,
    refresh_group_public_info,
    get_config,
    nexus_delete_kalimat,
    nexus_delete_kalimat_by_id,
    nexus_delete_regex_by_pola,
    nexus_clear_kalimat,
    nexus_clear_regex,
    nexus_get_all_regex,
    nexus_whitelist_add,
    nexus_whitelist_count,
    nexus_whitelist_page,
    nexus_whitelist_delete_by_id,
    nexus_whitelist_clear,
    nexus_regex_delete_by_id,
    nexus_actlog_get_page,
    nexus_actlog_count,
    nexus_actlog_clear,
    regex_db,
    get_owner_regex_count,
    invalidate_nexus_counts,
)
from plugins.nexus.engine import (
    pipeline_pembersihan,
    generate_kandidat_mutasi_liar,
)
from core.regex_utils import match_with_leet, remove_mentions_for_regex

from plugins.commands.log import log_spam_lokal, log_spam_global, log_sistem
from plugins.nexus.nexus_group import invalidate_nexus_wl_cache
try:
    from nexus.ai_core import nexus_ai_get_full_stats
    _AI_STATS_AVAILABLE = True
except Exception as _e:
    _AI_STATS_AVAILABLE = False
    async def nexus_ai_get_full_stats():
        return {}
    print(f"[nexus_handlers] ai_core import gagal (opsional): {_e}")

try:
    from nexus.ai_core import nexus_ai_trainer_status
    from nexus.ai_core.constants import (
        GROQ_CONFIG_KEY_PROMPT, GROQ_CONFIG_KEY_ENABLED, GROQ_DEFAULT_PROMPT,
        GROQ_FIXED_OUTPUT_FORMAT, OPENROUTER_DEFAULT_MODEL,
    )
    from database import save_bot_config
    _TRAINER_PANEL_AVAILABLE = True
except Exception as _e:
    _TRAINER_PANEL_AVAILABLE = False
    print(f"[nexus_handlers] trainer panel import gagal (opsional): {_e}")

import os
OWNER_ID   = int(os.environ.get("OWNER_ID", 0))
TZ_JAKARTA = pytz.timezone("Asia/Jakarta")

_owner_regex_fsm: dict[int, int] = {}
# FSM "Kategori Kata" — value: (category: str, msg_id: int, page: int)
_catword_fsm: dict[int, tuple] = {}

# Label tampilan + daftar kata DEFAULT (hardcoded, read-only) per kategori —
# ringkasan dari regex di nexus/ai_core/category_detector.py, HANYA untuk
# ditampilkan di panel (bukan sumber pengecekan — pengecekan tetap lewat
# regex asli). Kata kustom owner (bisa ditambah/dihapus) TERPISAH dari daftar
# ini, disimpan di database.py::category_custom_words_db.
CATWORD_LABELS: dict[str, str] = {
    "GROUP_INVITE":      "📢 Group Invite",
    "PORN":              "🔞 Porn",
    "SCAM":              "💰 Scam",
    "PROMO_VIRAL":       "📣 Promo Viral",
    "BIO_PROMO":         "🔗 Bio Promo",
    "JUDI_SLOT":         "🎰 Judi/Slot",
    "INVESTASI_BODONG":  "📈 Investasi Bodong",
    "JUAL_AKUN":         "🛒 Jual Akun",
    "GCAST_SPAM":        "📡 Gcast Spam",
    "PINJOL_JUDOL":      "💸 Pinjol",
    "SHORTLINK_SPAM":    "🔗 Shortlink",
}
CATWORD_DEFAULTS: dict[str, list[str]] = {
    "GROUP_INVITE": [
        "join", "gabung", "masuk", "ikut", "ikutan", "daftar", "register", "subscribe",
        "follow", "kunjungi", "cek", "kepoin", "yuk", "ayo", "mari", "buruan", "gas",
        "grup", "group", "channel", "komunitas", "server", "discord", "forum",
        "kami", "khusus", "vip", "premium", "eksklusif", "private", "rahasia", "official",
    ],
    "PORN": [
        "bokep", "bugil", "telanjang", "nakal", "esek esek", "mesum", "porno", "xxx", "18+",
        "colmek", "ngentot", "ngewe", "memek", "kontol", "pepek", "toket", "ngocok",
        "open bo", "psk", "wts", "sewa tante", "sewa abg",
        "dewasa", "sensual", "erotis", "sexy", "montok", "vc mesra", "teman tidur",
        "kirim foto", "kirim video", "vc yuk", "mau lihat",
    ],
    "SCAM": [
        "transfer dulu", "bayar dp", "modal receh", "cuan besar", "profit besar",
        "kerja santai", "gaji besar", "tanpa modal", "pasti untung", "dijamin untung",
        "skema", "piramid", "MLM", "downline", "upline", "bonus referral",
        "terbatas", "stok habis", "kesempatan emas", "hari ini saja", "claim sekarang",
        "selamat", "menang", "terpilih", "hadiah",
    ],
    "PROMO_VIRAL": [
        "broadcast", "bc", "forward", "sebarkan", "viralkan", "share ke", "copy paste",
        "giveaway", "GA", "berhadiah", "undian", "doorprize", "gratis untuk", "bagi-bagi",
    ],
    "BIO_PROMO": [
        "cek bio", "lihat profil", "kunjungi bio", "info di bio", "link di bio",
        "ada di bio", "di bio aku", "di bio saya", "di profil kami",
    ],
    "JUDI_SLOT": [
        "togel", "slot", "judi", "gacor", "rtp", "jackpot", "maxwin",
        "scatter", "pragmatic", "zeus", "mahjong", "spaceman", "live rtp",
        "bocoran rtp", "jam gacor", "jp maxwin", "freespin", "anti rungkad",
    ],
    "INVESTASI_BODONG": [
        "investasi", "profit", "passive income", "modal kecil", "tanpa modal",
        "binary", "forex", "trading", "crypto", "bitcoin", "mining", "airdrop",
        "roi", "bunga harian", "downline", "join sekarang", "daftar gratis",
        "penghasilan pasif", "kerja dari rumah", "bisnis online",
    ],
    "JUAL_AKUN": [
        "jual akun", "beli akun", "akun sultan", "akun premium",
        "saldo dana", "saldo ovo", "saldo gopay", "akun verified",
        "harga murah", "stok terbatas", "fast respon", "amanah", "trusted",
        "jual saldo", "top up murah", "reseller",
    ],
    "GCAST_SPAM": [
        "broadcast", "forward", "sebarkan", "share ke grup", "kirim ke semua",
        "teruskan pesan", "sebar", "viralkan", "copy paste", "share this",
    ],
    "PINJOL_JUDOL": [
        "pinjaman online", "pinjol", "pinjam uang", "kredit instan",
        "cair cepat", "tanpa jaminan", "bunga rendah", "koperasi",
        "kta kilat", "limit tinggi", "dana darurat",
    ],
    "SHORTLINK_SPAM": [
        "wa.me", "whatsapp.com/", "t.me/", "bit.ly", "tinyurl",
        "s.id/", "linktr.ee", "cutt.ly", "rebrand.ly", "gg.gg", "rb.gy",
    ],
}
_whitelist_fsm:   dict[int, int] = {}   # FSM untuk input whitelist regex
_trainer_prompt_fsm: dict[int, int] = {}   # FSM untuk input prompt Groq


async def sync_category_defaults_to_bayes() -> None:
    """
    Sinkron SEKALI SEUMUR HIDUP MODEL ke Bayes (is_spam=True):
      1. Semua kata DEFAULT (CATWORD_DEFAULTS, 11 kategori, hardcoded di
         category_detector.py — mis. "memek" di _PORN_HARD).
      2. Semua kata KUSTOM yang SUDAH TERSIMPAN di DB dari sebelum fitur
         auto-train ini ada (category_custom_words_db) — supaya kata yang
         owner tambah sebelum update ini juga ikut ke-training, bukan cuma
         yang ditambah SETELAH update.

    Auto-train real-time (di handler tambah/hapus kata) cuma nutup kata yang
    ditambah/dihapus SETELAH fungsi ini ada — kata lama tetap butuh sync
    manual sekali ini.

    Dipanggil sekali di startup (main.py). Idempotent lewat flag
    `ai.defaults_synced` yang persist di MongoDB (nexus_ai_model) — restart
    bot berkali-kali TIDAK akan nge-train ulang kata yang sama berkali-kali.
    """
    try:
        from nexus.ai_core.bridge import get_nexus_ai
        from database import get_all_category_words
        ai = get_nexus_ai()
        if not ai._loaded:
            await ai.load()

        if ai.defaults_synced:
            return  # sudah pernah disinkron, skip

        total = 0

        # 1. Kata default hardcoded
        for cat, words in CATWORD_DEFAULTS.items():
            for w in words:
                ai.bayes.train(w, is_spam=True)
                total += 1

        # 2. Kata kustom yang sudah ada di DB (ditambah owner sebelum update ini)
        custom_data = await get_all_category_words()
        custom_total = 0
        for cat, words in custom_data.items():
            for w in words:
                ai.bayes.train(w, is_spam=True)
                custom_total += 1
        total += custom_total

        ai.defaults_synced = True
        await ai.save()
        print(
            f"[Startup] 🧠 Sync kata ke Bayes selesai — {total} kata total "
            f"({total - custom_total} default + {custom_total} kustom lama) dari {len(CATWORD_DEFAULTS)} kategori."
        )
    except Exception as e:
        print(f"[Startup] ⚠️  sync_category_defaults_to_bayes gagal (dilanjutkan): {e}")


# Teks panel "PANEL KHUSUS OWNER BOT" — termasuk daftar SEMUA command
# owner-only. Sengaja HANYA muncul di sini (bukan di /antigcast → Panduan)
# karena panel ini sudah digerbang `user_id != OWNER_ID` di setiap handler-nya
# — admin grup & member biasa tidak pernah bisa membuka layar ini.
_OWNER_PANEL_TEXT = (
    "👑 <b>PANEL KHUSUS OWNER BOT</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "Akses kontrol penuh ke dalam core system Nexus AI.\n\n"
    "<blockquote>"
    "<b>🔧 Perintah Owner (DM bot):</b>\n"
    "<code>/addregex [kata1|kata2]</code> — tambah filter kata GLOBAL\n"
    "<code>/delregex [kata]</code> — hapus filter kata global\n"
    "<code>/wlregex [kata1|kata2]</code> — tambah whitelist regex global\n"
    "<code>/infobot</code> — lihat semua filter kata global aktif\n"
    "<code>/delnexus [kalimat/pola]</code> — hapus data dari database Nexus AI\n"
    "<code>/delkalimat [teks]</code> — hapus kalimat dari Record Data Nexus\n"
    "<code>/otp [kode]</code> — kirim kode OTP login userbot\n"
    "<code>/list</code> — lihat daftar semua grup terpasang bot\n"
    "<code>/reset [code_bot]</code> — hapus semua data 1 namespace ⚠️\n"
    "<code>/cekstickerpack</code> — daftar stiker pack yang diblokir\n"
    "<code>/cekreport</code> — daftar stiker pack yang dilaporkan\n"
    "<code>/openstikerpack [SET_NAME]</code> — buka blokir 1 stiker pack\n"
    "<code>/ram &lt;chat_id&gt; up &lt;DD-MM-YYYY&gt;</code> — buka kunci Upgrade Speed grup\n"
    "<code>/ram &lt;chat_id&gt; off</code> — cabut boost, paksa balik speed minimal\n"
    "<code>/ram &lt;chat_id&gt; status</code> — cek status boost Upgrade Speed grup"
    "</blockquote>"
)


# ══════════════════════════════════════════════════════════════════════════════
# UI HELPERS
# ══════════════════════════════════════════════════════════════════════════════

async def _welcome_text() -> str:
    total, antrean = await nexus_get_kalimat_count()
    owner_regex_ct = await get_owner_regex_count()
    wl_ct          = await nexus_whitelist_count()
    return (
        "🤖 <b>NEXUS AI ENGINE</b>\n"
        "<i>Adaptive Regex Engine · Belajar dari Laporan Spam</i>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "<blockquote>"
        "Bukan blacklist statis. Nexus mengurai pola pesan secara kolektif,\n"
        "merakit pertahanan otomatis, dan mengantisipasi manipulasi font,\n"
        "karakter berulang, hingga varian leetspeak secara simultan.\n\n"
        "📊 <b>STATUS ENGINE — LIVE:</b>\n"
        f"├─ 📚 <code>Record Data</code>         : <b>{total} raw teks</b>\n"
        f"├─ ⏳ <code>Antrean TAHAP 2</code>      : <b>{antrean} belum digenerate</b>\n"
        f"├─ ⚙️ <code>Pola Manual (Owner)</code> : <b>{owner_regex_ct} regex</b>\n"
        f"└─ 🛡️ <code>Whitelist Nexus</code>     : <b>{wl_ct} pengecualian</b>"
        "</blockquote>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<i>Engine berjalan otomatis. Pilih panel di bawah:</i>"
    )


def _main_markup(username_bot: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "➕  Aktifkan di Grup",
            callback_data="pasang_bot"
        )],
        [
            InlineKeyboardButton("📚  Manual AI",        callback_data="nx_tutorial"),
            InlineKeyboardButton("🧪  Lab Sandbox",      callback_data="nx_sandbox_hub"),
        ],
        [
            InlineKeyboardButton("🔮  Global Regex",     callback_data="nx_global_regex_menu"),
            InlineKeyboardButton("👑  OWNER BOT",        callback_data="nx_owner_menu"),
        ],
        [InlineKeyboardButton("🎤  Promo Userbot",       callback_data="px_menu")],
        [InlineKeyboardButton("🔙  Menu Utama Bot",      callback_data="nx_back_main")],
    ])


def _back_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🔙 KEMBALI KE MAINFRAME", callback_data="nx_home")
    ]])


def _back_global_regex() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🔙 KEMBALI KE GLOBAL REGEX", callback_data="nx_global_regex_menu")
    ]])


async def _safe_edit(msg, text: str, keyboard=None):
    """
    Edit pesan panel dengan proteksi FloodWait.
    Jika kena FloodWait, tunggu durasi yang diminta lalu retry sekali —
    panel nexus owner-only tapi tetap harus tahan di kondisi API sibuk.
    """
    from pyrogram.errors import FloodWait as _FW
    try:
        await msg.edit(text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
    except MessageNotModified:
        pass
    except _FW as fw:
        wait = min(fw.value, 10)   # tunggu maks 10 detik, tidak lebih
        await asyncio.sleep(wait)
        try:
            await msg.edit(text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
        except Exception as e2:
            print(f"[nexus safe_edit] retry gagal: {e2}")
    except Exception as e:
        print(f"[nexus safe_edit] {e}")


async def build_grup_terdaftar_text(client) -> str:
    """
    Bangun teks "GRUP YANG MEMAKAI BOT" (dipakai panel Nexus > Owner Bot >
    Grup Terdaftar, DAN perintah /list di DM owner — supaya keduanya selalu
    tampil identik, satu sumber kebenaran).

    HTML dipakai di sini (bukan Markdown seperti halaman Nexus lain) —
    judul grup adalah teks BEBAS dari user, dan Markdown legacy Pyrogram
    tidak mendukung backslash-escape (\\* tidak "dimakan" parser seperti
    MarkdownV2 Bot API resmi). Akibatnya 1 karakter delimiter (* _ ` [)
    di judul grup manapun bisa merusak parsing SEMUA baris setelahnya
    (bold hilang, link gagal ter-render, dst — persis kasus yang pernah
    terjadi). html.escape() menetralkan &, <, > secara aman dan total,
    tidak ada delimiter HTML yang bisa "lolos" dari teks bebas.

    SUMBER DATA: config_db (lewat get_all_groups_with_perm_status), BUKAN
    nexus_grup_db — yang isinya hanya grup yang pernah trigger /spam
    atau event member-update, sehingga tidak mewakili SEMUA grup yang
    memakai bot. config_db juga otomatis tidak memuat grup yang sudah
    mati/bot-dikick (dibersihkan oleh perm_watchdog), dan menyertakan
    status izin ban/mute TERKINI per grup — supaya owner tidak melihat
    grup tanpa izin ban seolah-olah berjalan normal.

    STATUS RAM (flash/slow) — per grup, sumber core/speed_boost.py
    (group_speed_boost). Di-fetch SEKALI di awal via query $in untuk semua
    chat_id sekaligus (bukan 1 query per grup di dalam loop) — supaya
    panel/daftar dengan ratusan grup tidak N+1 query ke DB.
    """
    from core.speed_boost import speed_boost_db
    from database import TZ_WIB
    from datetime import datetime
    import time as _time_ram

    grups = await get_all_groups_with_perm_status()

    # ── Batch fetch status RAM semua grup sekaligus ──────────────────────────
    _ram_map: dict[int, dict] = {}
    if grups:
        _now_ram = _time_ram.time()
        _chat_ids = [g["chat_id"] for g in grups]
        async for _doc in speed_boost_db.find({"chat_id": {"$in": _chat_ids}}):
            try:
                _cid_ram = int(_doc.get("chat_id"))
            except (TypeError, ValueError):
                continue
            _until = float(_doc.get("boost_until") or 0)
            _ram_map[_cid_ram] = {
                "active": _until > _now_ram,
                "until":  _until,
                "source": _doc.get("source", "donation"),
            }

    text  = "📂 <b>GRUP YANG MEMAKAI BOT:</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    stop_fallback_fetch = False  # set True kalau kena FloodWait — jangan fetch lagi di loop ini
    if grups:
        for idx, g in enumerate(grups, 1):
            chat_id_g   = g["chat_id"]
            judul       = _html_escape(g["title"])
            username    = g.get("username")
            invite_link = g.get("invite_link")

            # Fallback: grup terdaftar SEBELUM field username/invite_link
            # ada di DB (data lama, atau belum sempat dicek perm_watchdog
            # siklus pertama) — coba ambil langsung dari Telegram sekali,
            # lalu simpan supaya render berikutnya tidak perlu fetch ulang.
            # Sumber utama tetap perm_watchdog (refresh_group_public_info)
            # yang jalan berkala terlepas panel ini dibuka atau tidak.
            if username is None and invite_link is None and not stop_fallback_fetch:
                try:
                    await refresh_group_public_info(client, chat_id_g)
                    _refreshed = await get_config(chat_id_g)
                    username    = _refreshed.get("username")
                    invite_link = _refreshed.get("invite_link")
                except Exception as _fc:
                    if type(_fc).__name__ == "FloodWait":
                        # Telegram rate-limit — stop fallback fetch untuk
                        # sisa grup di loop ini, jangan sampai tombol
                        # nge-hang menunggu FloodWait demi 1 list.
                        stop_fallback_fetch = True
                    # Grup tidak bisa diakses sekarang (privat-tanpa-akses,
                    # FloodWait, dll) — tampilkan apa adanya untuk siklus ini.

            text += f"<b>{idx}.</b> 👥 {judul}\n┗─ ID: <code>{chat_id_g}</code>\n"
            if username:
                uname_safe = _html_escape(username)
                text += f'┗─ 🔗 <a href="https://t.me/{uname_safe}">t.me/{uname_safe}</a>\n'
            elif invite_link:
                text += f'┗─ 🔒 <a href="{_html_escape(invite_link)}">Link Undangan (privat)</a>\n'
            else:
                text += "┗─ 🔒 Grup privat (tanpa link undangan)\n"

            # Status RAM (Upgrade Speed) — flash (boost aktif) vs slow
            # (default/terkunci floor). Selalu tampil (bukan cuma saat
            # bermasalah, beda dari status izin ban/mute di bawah) — owner
            # perlu lihat sekilas grup mana yang lagi flash tanpa buka /ram.
            #
            # Admin-Flash override (core/admin_flash_watch.py) dicek DULU —
            # jalur TERPISAH dari boost donasi/trial, tidak pernah expire
            # sendiri, jadi ditampilkan beda (tanpa tanggal "s/d") dan
            # PRIORITAS di atas status donasi/trial kalau kebetulan dua-
            # duanya aktif bareng di grup yang sama.
            import core.antispam_queue as _aq
            if _aq.is_admin_flash(chat_id_g):
                text += "┗─ ⚡ Mode Flash — tanpa batas waktu (admin override)\n"
            else:
                _ram = _ram_map.get(chat_id_g)
                if _ram and _ram["active"]:
                    _until_str = datetime.fromtimestamp(_ram["until"], tz=TZ_WIB).strftime("%d-%m-%Y")
                    _src_label = "trial gratis" if _ram.get("source") == "trial" else "donasi"
                    text += f"┗─ ⚡ Mode Flash — s/d {_until_str} ({_src_label})\n"
                else:
                    text += "┗─ 🐢 Mode Slow (default)\n"

            # Status izin ban/mute — sumber: perm_watchdog (cek berkala).
            # Tampil HANYA saat bermasalah, supaya daftar tidak penuh noise
            # untuk grup yang memang normal.
            if g.get("forced_off"):
                text += "┗─ ⛔ <b>Izin Ban/Mute Hilang</b> — moderasi dipaksa OFF\n"
            text += "\n"
    else:
        text += "<i>Belum ada grup yang terdaftar.</i>"
    return text


async def _safe_edit_html(msg, text: str, keyboard=None):
    """
    Sama seperti _safe_edit, tapi parse_mode=HTML.

    DIPAKAI KHUSUS untuk halaman yang merender teks bebas dari user/grup
    (mis. judul grup di nx_list_grup) — Markdown legacy Pyrogram TIDAK
    mendukung backslash-escape (\\* tidak "dimakan" parser seperti di
    MarkdownV2 Bot API resmi), sehingga judul yang mengandung karakter
    delimiter (*, _, `, [) selalu berisiko merusak parsing baris setelahnya
    walau sudah di-escape manual. HTML jauh lebih aman karena html.escape()
    benar-benar menetralkan &, <, > — tidak ada lagi delimiter yang lolos.
    """
    from pyrogram.errors import FloodWait as _FW
    try:
        await msg.edit(text, reply_markup=keyboard, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except MessageNotModified:
        pass
    except _FW as fw:
        wait = min(fw.value, 10)
        await asyncio.sleep(wait)
        try:
            await msg.edit(text, reply_markup=keyboard, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        except Exception as e2:
            print(f"[nexus safe_edit_html] retry gagal: {e2}")
    except Exception as e:
        print(f"[nexus safe_edit_html] {e}")


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT & PURGE
# ══════════════════════════════════════════════════════════════════════════════


@Client.on_message(filters.command("delnexus") & filters.user(OWNER_ID))
async def nexus_del_handler(client: Client, message: Message):
    if len(message.command) < 2:
        return await message.reply(
            "❌ **Sintaks Salah!**\n"
            "Gunakan: `/delnexus <kalimat asli atau pola interlock (?=.*)>`",
            parse_mode=ParseMode.MARKDOWN,
        )
    input_target = message.text.split(None, 1)[1].strip()

    if input_target.startswith("(?=.*"):
        deleted = await nexus_delete_regex_by_pola(input_target)
        if deleted:
            await message.reply("🗑️ **PURGE AI REGEX BERHASIL**\nPola interlock dieliminasi dari Core Nexus.", parse_mode=ParseMode.MARKDOWN)
        else:
            await message.reply("❌ Pola interlock tidak ditemukan di database Nexus.")
    else:
        teks_clean = pipeline_pembersihan(input_target)
        deleted    = await nexus_delete_kalimat(input_target)
        if not deleted and teks_clean != input_target:
            deleted = await nexus_delete_kalimat(teks_clean)
        if deleted:
            await message.reply(
                f"🗑️ **PURGE BERHASIL**\n📝 `{input_target}` & seluruh turunannya "
                f"(klaim TAHAP 1 + varian TAHAP 2) dihapus dari Record Data.",
                parse_mode=ParseMode.MARKDOWN,
            )
        else:
            await message.reply("❌ Kalimat tidak ditemukan di Nexus database.")


@Client.on_message(filters.command("resetai") & filters.user(OWNER_ID))
async def nexus_resetai_handler(client: Client, message: Message):
    """
    /resetai — RESET TOTAL AI Manual: dari raw Record Data (TAHAP 1) sampai
    kalimat turunan (TAHAP 2 varian Groq), regex hasil mining, DAN model itu
    sendiri (Bayes vocab + PatternMemory + AdaptiveThreshold) — termasuk
    kata/pola yang sempat masuk paksa ke database sebelum integrasi Groq.

    Tombol "PURGE KALIMAT + AI REGEX" yang sudah ada di panel Owner Bot
    HANYA membersihkan Record Data (nexus_kalimat/variants/regex/claim
    queue) — model AI (nexus_ai_model) tidak ikut ter-reset di sana.
    Command ini menggabungkan keduanya dalam satu langkah, dengan
    konfirmasi karena bersifat destruktif & permanen.
    """
    total, _ = await nexus_get_kalimat_count()
    regex_n  = await nexus_get_regex_count()
    try:
        ai = get_nexus_ai()
        pm = ai.pattern_memory.info()
        model_info = (
            f"• Bayes vocab: `{ai.bayes.vocab_size}` kata "
            f"(spam:{ai.bayes.class_count.get('spam', 0)} "
            f"ham:{ai.bayes.class_count.get('ham', 0)})\n"
            f"• Pattern Memory: `{pm.get('pattern_spam_stored', 0)}` pola spam, "
            f"`{pm.get('pattern_nonspam_stored', 0)}` pola non-spam\n"
            f"• Threshold saat ini: `{ai.adaptive.threshold:.3f}`\n"
        )
    except Exception:
        model_info = "• (gagal membaca statistik model)\n"

    await message.reply(
        "⚠️ **KONFIRMASI RESET TOTAL AI MANUAL**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Yang akan **dihapus permanen**:\n"
        f"• Record Data raw (TAHAP 1): `{total}` kalimat\n"
        "• Semua varian turunan (TAHAP 2 Groq)\n"
        f"• Regex hasil mining: `{regex_n}` pola\n"
        "• Antrean klaim spam (spam_claim_queue)\n"
        + model_info +
        "\nModel akan di-seed ulang dari nol (seed vocabulary bawaan). "
        "Ini termasuk kata/pola yang masuk sebelum integrasi Groq — "
        "**tidak bisa dibatalkan**. Lanjutkan?",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Ya, Reset Total", callback_data="nx_resetai_exec")],
            [InlineKeyboardButton("🚫 Batal",            callback_data="nx_resetai_batal")],
        ]),
    )


@Client.on_message(filters.command("delkalimat") & filters.user(OWNER_ID))
async def nexus_delkalimat_handler(client: Client, message: Message):
    """
    /delkalimat <teks>  — hapus satu kalimat dari Record Data berdasarkan teks asli.
    Alternatif command dari tombol 🗑 di panel Record Data.
    """
    if len(message.command) < 2:
        return await message.reply(
            "❌ **Sintaks Salah!**\n\n"
            "Gunakan: `/delkalimat <teks kalimat spam>`\n\n"
            "💡 _Atau buka panel Record Data → tekan tombol 🗑 di samping kalimat yang ingin dihapus._",
            parse_mode=ParseMode.MARKDOWN,
        )
    teks_target = message.text.split(None, 1)[1].strip()
    teks_clean  = pipeline_pembersihan(teks_target)

    deleted = await nexus_delete_kalimat(teks_target)
    if not deleted and teks_clean and teks_clean != teks_target:
        deleted = await nexus_delete_kalimat(teks_clean)

    if deleted:
        await message.reply(
            f"🗑️ **KALIMAT BERHASIL DIHAPUS**\n\n"
            f"📝 `{teks_target[:200]}`\n\n"
            f"_Kalimat telah dieliminasi dari Record Data Nexus AI._",
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        await message.reply(
            f"❌ **Kalimat Tidak Ditemukan**\n\n"
            f"`{teks_target[:200]}`\n\n"
            f"_Pastikan teks sama persis. Cek daftar via panel: Menu Nexus AI → Owner Bot → Record Data._",
            parse_mode=ParseMode.MARKDOWN,
        )


# ══════════════════════════════════════════════════════════════════════════════
# LAB SANDBOX PROCESSOR (RESTORED LOGIC DUPLIKAT/MULTIPLE TRIGGER & ASLI LOG)
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.private & filters.reply, group=10)
async def nexus_sandbox_processor(client: Client, message: Message):
    if not (
        message.reply_to_message
        and message.reply_to_message.text
        and "🧪 [NEXUS SANDBOX SIMULATION MODE]" in message.reply_to_message.text
    ):
        # Bukan pesan untuk handler ini — pass ke handler berikutnya di group=10
        # (nexus_ai_sandbox_processor). Tanpa ContinuePropagation, Pyrogram
        # berhenti di sini dan AI Manual Sandbox tidak pernah jalan.
        raise ContinuePropagation
    if not message.text:
        raise ContinuePropagation

    # CATATAN: sebelumnya sandbox cuma cek 1 versi teks (pipeline_pembersihan,
    # yang mengubah semua angka jadi huruf leet-speak) — jadi pola yang sengaja
    # menargetkan ANGKA ASLI (mis. blokir literal "666" atau nomor tertentu)
    # tidak pernah kena di sandbox walau di produksi (Gate A, core/antispam_
    # queue.py::_gate_regex) sudah kena karena match_with_leet() mengecek
    # TIGA versi sekaligus: leet (angka→huruf), strip (angka dibuang), raw
    # (angka asli dipertahankan). Sekarang disamakan persis — sandbox HARUS
    # selalu mirror production matching, kalau tidak hasil ujinya menyesatkan.
    regex_safe       = remove_mentions_for_regex(message)
    teks_clean       = pipeline_pembersihan(message.text)

    def _cocok(pola_str: str) -> bool:
        try:
            pat = re.compile(pola_str, re.IGNORECASE)
        except re.error:
            return False
        return bool(match_with_leet(pat, regex_safe) or (teks_clean and pat.search(teks_clean)))

    triggers = []

    # 1. Validasi Pertama: AI GLOBAL REGEX (Full Interlock AI)
    docs = await nexus_get_all_regex()
    for d in docs:
        pola_target = d.get("pola")
        if not pola_target:
            continue
        if _cocok(pola_target):
            triggers.append({
                "tipe": "GLOBAL_AI",
                "pola": pola_target,
                "indikator": d.get("kata_kunci", "[AI_PATTERN]")
            })
            # Tidak di-break agar bisa mendeteksi duplikat pelanggaran (AI + Owner)

    # 2. Validasi Kedua: OWNER GLOBAL REGEX (Full Interlock Manual)
    async for doc in regex_db.find({}):
        pola_target = doc.get("pola") or doc.get("pattern")
        if not pola_target:
            continue
        if _cocok(pola_target):
            triggers.append({
                "tipe": "OWNER_GLOBAL",
                "pola": pola_target,
                "indikator": f"[OWNER] {doc.get('raw', '')}"
            })

    # ── EKSEKUSI PENANGANAN PESAN & LOG (LOGIKA ASLI) ──
    if triggers:
        for trig in triggers:
            if trig["tipe"] == "GLOBAL_AI":
                asyncio.create_task(log_spam_global(client, message, trig["pola"], f"NEXUS_AI: {trig['indikator']}"))
            elif trig["tipe"] == "OWNER_GLOBAL":
                asyncio.create_task(log_spam_lokal(client, message, trig["pola"], f"NEXUS_OWNER: {trig['indikator']}"))

        from pyrogram.enums import ChatType
        if message.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
            try:
                await message.delete()
            except Exception:
                pass

    hasil = (
        "🧪 **HASIL DIAGNOSA SENSOR FILTER (ACUAN FULL INTERLOCK)**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📥 **Input Mentah:** `{message.text}`\n"
        f"🧹 **Hasil Destilasi Core:** `{teks_clean}`\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    )

    if triggers:
        hasil += f"🚨 **STATUS: SENSOR TERPICU ({len(triggers)} DETEKSI)!**\n\n"
        for idx, t in enumerate(triggers, 1):
            hasil += (
                f"**[{idx}] Deteksi: {t['tipe']}**\n"
                f"🔑 **Matriks ID:** `{t['indikator']}`\n"
                f"💥 **Interlock:** `{t['pola'][:50]}...`\n\n"
            )
        hasil += "📢 _Pesan pemicu ditangani & duplikat log diteruskan sesuai porsi masing-masing._"
    else:
        hasil += "✅ **STATUS: AMAN (LOLOS ACUAN FULL INTERLOCK)**"

    await message.reply(
        hasil,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🧪 UJI KEMBALI", callback_data="nx_sandbox_hub")],
            [InlineKeyboardButton("🔙 MENU NEXUS",  callback_data="nx_home")],
        ]),
        parse_mode=ParseMode.MARKDOWN,
    )


# ══════════════════════════════════════════════════════════════════════════════
# SANDBOX AI MANUAL (GATE E) — mirror PERSIS core/antispam_queue.py::_gate_nexus_ai
# ══════════════════════════════════════════════════════════════════════════════
# Beda dari nexus_sandbox_processor di atas (yang cuma cek regex): sandbox ini
# menjalankan model AI Manual yang SAMA persis dipakai produksi — generate
# varian (Regex Helper + Fuzzy Expand), scan tiap varian lewat
# NexusAICore.auto_detect() langsung (bukan wrapper bridge.nexus_ai_auto_detect()),
# lalu tampilkan SpamResult.debug apa adanya — breakdown ini BYTE-IDENTIK
# dengan yang dikirim ke LOG_CHANNEL produksi (lihat core.py::auto_detect,
# termasuk baris "Eksekusi" yang membandingkan ke NEXUS_MIN_CONFIDENCE), jadi
# tidak ada rumus/format kedua yang bisa beda sendiri dari produksi.
# Owner-only karena membuka detail internal cara model berpikir.

@Client.on_message(filters.private & filters.reply & filters.user(OWNER_ID), group=10)
async def nexus_ai_sandbox_processor(client: Client, message: Message):
    if not (
        message.reply_to_message
        and message.reply_to_message.text
        and "🧠 [NEXUS AI MANUAL SANDBOX MODE]" in message.reply_to_message.text
    ):
        return
    if not message.text:
        return

    content = message.text
    status_msg = await message.reply("⏳ _Menjalankan AI Manual (Gate E)..._", parse_mode=ParseMode.MARKDOWN)

    try:
        import asyncio as _asyncio
        from nexus.ai_core.bridge import get_nexus_ai
        from core.regex_helper import translate_variants

        ai = get_nexus_ai()
        if not ai._loaded:
            await ai.load()

        # ── FASE 1: generate varian — SAMA PERSIS dengan Gate E produksi ────
        async def _regex_variants() -> list[str]:
            try:
                return await translate_variants(content)
            except Exception:
                return [content]

        def _fuzzy_variants() -> tuple[list[str], list[str]]:
            try:
                from nexus.ai_core.sentence_expander import expand_sentence
                exp = expand_sentence(content)
                if exp.had_unknowns:
                    return exp.all_sentences, exp.unknown_words
            except Exception:
                pass
            return [], []

        regex_task = _asyncio.create_task(_regex_variants())
        fuzzy_sents, unknown_words = await _asyncio.get_event_loop().run_in_executor(None, _fuzzy_variants)
        regex_sents = await regex_task

        seen: set[str] = set()
        all_variants: list[str] = []
        for s in [content] + regex_sents + fuzzy_sents:
            key = s.strip().lower()
            if key and key not in seen:
                seen.add(key)
                all_variants.append(s)

        # ── FASE 2: scan semua varian LANGSUNG lewat auto_detect() (bukan
        # wrapper nexus_ai_auto_detect()) — supaya SpamResult.debug (breakdown
        # persis sama dengan yang tampil di LOG_CHANNEL produksi, termasuk
        # bobot per-layer & baris "Eksekusi") selalu ke-generate, walau
        # variannya tidak lolos gate/min_confidence sekalipun. Verdict
        # eksekusi tetap dihitung manual pakai rumus SAMA PERSIS dengan
        # bridge.py::nexus_ai_auto_detect() (is_spam AND confidence >= min_confidence).
        metadata = {"chat_id": message.chat.id, "user_id": message.from_user.id}
        best_result  = None
        best_variant = content
        best_fires   = False
        for variant in all_variants:
            res = ai.auto_detect(variant, metadata)
            fires = res.is_spam and res.confidence >= NEXUS_MIN_CONFIDENCE
            # Prioritas: varian yang benar2 "fires" (akan dieksekusi produksi)
            # menang di atas varian yang cuma confidence tinggi tapi ditahan.
            if best_result is None or (fires and not best_fires) or (
                fires == best_fires and res.confidence > best_result.confidence
            ):
                best_result, best_variant, best_fires = res, variant, fires

        # ── FASE 3: breakdown = best_result.debug APA ADANYA (byte-identik
        # dengan yang dikirim ke LOG_CHANNEL produksi, lihat core.py::auto_detect) ──
        explain_text = best_result.debug if (best_result and best_result.debug) else "(tidak ada rincian — teks kosong/gibberish gate)"
        best_conf = best_result.confidence if best_result else -1.0

    except Exception as e:
        await status_msg.edit(f"❌ **Sandbox AI gagal jalan**\n`{e}`")
        return

    if best_fires:
        verdict = (
            f"🚨 **AKAN DIHAPUS** — confidence `{best_conf*100:.0f}%`\n"
            f"Varian pemicu: `{best_variant[:80]}`\n"
            f"Layer: `{best_result.layer}`"
        )
    else:
        verdict = f"✅ **AMAN** — tidak ada varian yang lolos gate + threshold {NEXUS_MIN_CONFIDENCE:.2f}"

    varian_list = "\n".join(f"  {i+1}. `{v[:70]}`" for i, v in enumerate(all_variants[:8]))
    if len(all_variants) > 8:
        varian_list += f"\n  _(+{len(all_variants) - 8} varian lain, dipotong)_"

    hasil = (
        "🧠 **HASIL SIMULASI AI MANUAL (GATE E)**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📥 **Input:** `{content[:100]}`\n\n"
        f"🧬 **Varian diuji ({len(all_variants)}):**\n{varian_list}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{verdict}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔬 **BREAKDOWN (varian terbaik):**\n```\n{explain_text[:2500]}\n```"
    )
    if len(hasil) > 3900:
        hasil = hasil[:3900] + "\n…_(dipotong)_"

    await status_msg.edit(
        hasil,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🧠 UJI KEMBALI", callback_data="nx_ai_sandbox_hub")],
            [InlineKeyboardButton("🔙 KEMBALI",     callback_data="nx_owner_menu")],
        ]),
        parse_mode=ParseMode.MARKDOWN,
    )


# ══════════════════════════════════════════════════════════════════════════════
# BUILDER CORE INTERLOCK SKEMA
# ══════════════════════════════════════════════════════════════════════════════

def _build_owner_interlock(kata_list: list[str]) -> tuple[str, list[tuple[str, list[str]]], str]:
    """
    Bangun interlock regex dari daftar kata.
    Kapital dari owner DIPERTAHANKAN — diteruskan ke generate_kandidat_mutasi_liar
    sebagai penanda posisi wajib. pipeline_pembersihan hanya dipakai untuk
    validasi (cek kosong), BUKAN sebagai sumber kata ke generator.
    """
    import re as _re
    mutasi_display = []
    lookaheads     = []

    for kata in kata_list:
        # Validasi: pastikan kata tidak kosong setelah dibersihkan
        kata_clean = pipeline_pembersihan(kata)
        if not kata_clean:
            continue

        # Bersihkan simbol tapi JAGA KAPITAL — ini yang dikirim ke generator
        # "*" (wildcard) juga dijaga — lihat generate_kandidat_mutasi_liar().
        kata_bersih = _re.sub(r"\(?[×xX]\d+\)?", "", kata)
        kata_bersih = _re.sub(r"[^\w*]", "", kata_bersih).strip()
        kata_token  = kata_bersih.split()[0] if kata_bersih else ""
        if not kata_token:
            continue

        mutasi = generate_kandidat_mutasi_liar(kata_token)

        if mutasi:
            lookaheads.append(f"(?=.*({'|'.join(mutasi)}))")
            # Simpan token lowercase untuk display/key konsistensi
            mutasi_display.append((kata_token.lower(), mutasi))

    pola = "".join(lookaheads) if lookaheads else ""
    return pola, mutasi_display, ""


# ══════════════════════════════════════════════════════════════════════════════
# COMMAND HANDLER — DIRECT ADDREGEX
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.command("addregex") & filters.user(OWNER_ID))
async def nexus_direct_add_regex(client: Client, message: Message):
    if len(message.command) < 2:
        return await message.reply(
            "❌ **Sintaks Salah!**\n"
            "Gunakan: `/addregex kata1|kata2|kata3`\n\n"
            "💡 _Pola lama dimusnahkan. Otomatis menggunakan metode Full Interlock Owner Nexus._",
            parse_mode=ParseMode.MARKDOWN,
        )

    raw_input = message.text.split(None, 1)[1].strip()
    raw_input = unicodedata.normalize("NFKC", raw_input)

    kata_list = [k.strip() for k in raw_input.split("|") if k.strip()]
    if not kata_list:
        return await message.reply("❌ **Gagal:** Input kata tidak valid atau kosong.")

    pola, mutasi_display, _ = _build_owner_interlock(kata_list)

    if not pola:
        return await message.reply("❌ **Gagal Generate:** Kata bersih kosong setelah melewati pipeline destilasi.")

    try:
        re.compile(pola)
    except re.error as e:
        return await message.reply(f"❌ **Regex Error:** Kompilasi interlock gagal.\n`{e}`")

    raw_joined = " | ".join([k for k, _ in mutasi_display])

    await regex_db.update_one(
        {"pola": pola},
        {"$set": {
            "pola":      pola,
            "pattern":   pola,
            "raw":       raw_joined,
            "kata_list": [k for k, _ in mutasi_display],
            "mutasi":    {k: m for k, m in mutasi_display},
        }},
        upsert=True,
    )
    invalidate_nexus_counts()

    hasil_respon = (
        f"✅ **DIRECT INJECTION SUCCESS!**\n"
        f"⚙️ **Metode:** Owner Interlock System (Command Base)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📝 **Koleksi Asli:** `{raw_joined}`\n\n"
        f"🔍 **Probabilitas Lolos Mutasi (>=50%):**\n"
    )
    for kata, mutasi in mutasi_display:
        hasil_respon += f"• `{kata}` ➔ `({'|'.join(mutasi)})`\n"

    hasil_respon += f"\n💥 **Full Interlock (Acuan Utama Locked):**\n`{pola}`"

    await message.reply(hasil_respon, parse_mode=ParseMode.MARKDOWN)


# ══════════════════════════════════════════════════════════════════════════════
# COMMAND HANDLER — DIRECT WLREGEX (WHITELIST)
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.command("wlregex") & filters.user(OWNER_ID))
async def nexus_direct_add_whitelist(client: Client, message: Message):
    """/wlregex kataA | kataB | kataC — generate whitelist regex seperti /addregex."""
    if len(message.command) < 2:
        return await message.reply(
            "❌ **Sintaks Salah!**\n"
            "Gunakan: `/wlregex kata1|kata2|kata3`\n\n"
            "💡 _Regex whitelist melindungi pesan dari penghapusan meski cocok regex spam._",
            parse_mode=ParseMode.MARKDOWN,
        )

    raw_input = message.text.split(None, 1)[1].strip()
    raw_input = unicodedata.normalize("NFKC", raw_input)
    kata_list = [k.strip() for k in raw_input.split("|") if k.strip()]
    if not kata_list:
        return await message.reply("❌ **Gagal:** Input kata tidak valid atau kosong.")

    pola, mutasi_display, _ = _build_owner_interlock(kata_list)
    if not pola:
        return await message.reply("❌ **Gagal Generate:** Kata kosong setelah pipeline destilasi.")

    try:
        re.compile(pola)
    except re.error as e:
        return await message.reply(f"❌ **Regex Error:** `{e}`")

    raw_joined = " | ".join([k for k, _ in mutasi_display])
    await nexus_whitelist_add(
        pola      = pola,
        raw       = raw_joined,
        kata_list = [k for k, _ in mutasi_display],
        mutasi    = {k: m for k, m in mutasi_display},
    )
    invalidate_nexus_wl_cache()

    hasil = (
        f"🛡️ **WHITELIST INJECTED!**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📝 **Koleksi Asli:** `{raw_joined}`\n\n"
        f"🔍 **Probabilitas Mutasi (>=50%):**\n"
    )
    for kata, mutasi in mutasi_display:
        hasil += f"• `{kata}` ➔ `({'|'.join(mutasi)})`\n"
    hasil += f"\n🛡️ **Whitelist Interlock:**\n`{pola}`"
    await message.reply(hasil, parse_mode=ParseMode.MARKDOWN)


# ══════════════════════════════════════════════════════════════════════════════
# FSM — ENGINE INTERLOCK PANEL BUILDER
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.private & filters.text & ~filters.command([""]) & filters.user(OWNER_ID), group=9)
async def nexus_owner_regex_fsm(client: Client, message: Message):
    # Cek FSM "Kategori Kata" dulu (kata kustom CategoryDetector, RAW —
    # bukan regex, lihat panel nx_catword_*)
    if message.from_user.id in _catword_fsm:
        category, msg_id, page = _catword_fsm.pop(message.from_user.id)
        if message.text and message.text.strip() == "/batal":
            await _render_catword_page(client, message.chat.id, msg_id, category, 1)
            try: await message.delete()
            except Exception: pass
            return
        if message.text and message.text.startswith("/"):
            return

        raw_word = message.text.strip() if message.text else ""
        if not raw_word:
            await _render_catword_page(
                client, message.chat.id, msg_id, category, page,
                header="❌ **INPUT KOSONG** — kata tidak disimpan.\n\n",
            )
            try: await message.delete()
            except Exception: pass
            return

        from database import add_category_word
        from nexus.ai_core.category_detector import reload_custom_words
        ok = await add_category_word(category, raw_word, message.from_user.id)
        await reload_custom_words()

        bayes_note = ""
        if ok:
            # Sinkron ke Bayes juga — kata kustom kategori dianggap sinyal
            # spam (is_spam=True) apapun kategorinya, karena model Bayes
            # binary spam/ham, bukan multi-kategori.
            try:
                from nexus.ai_core.bridge import get_nexus_ai
                ai = get_nexus_ai()
                if not ai._loaded:
                    await ai.load()
                ai.bayes.train(raw_word, is_spam=True)
                await ai.save()
                bayes_note = "🧠 Bayes ikut belajar dari kata ini.\n\n"
            except Exception as e:
                bayes_note = f"⚠️ Bayes gagal disinkron: {e}\n\n"

        header = (
            f"✅ Kata **`{raw_word[:60]}`** ditambahkan ke kategori `{category}`.\n\n{bayes_note}"
            if ok else
            f"⚠️ Kata **`{raw_word[:60]}`** sudah ada di kategori `{category}` (tidak disimpan dobel).\n\n"
        )
        await _render_catword_page(client, message.chat.id, msg_id, category, 1, header=header)
        try: await message.delete()
        except Exception: pass
        return

    # Cek Pelatihan AI prompt FSM dulu (v3.2, v11: rename dari Groq)
    if message.from_user.id in _trainer_prompt_fsm:
        msg_id = _trainer_prompt_fsm.pop(message.from_user.id)
        if message.text and message.text.strip() == "/batal":
            await _render_trainer_menu(await client.get_messages(message.chat.id, msg_id))
            try: await message.delete()
            except Exception: pass
            return
        if message.text and message.text.startswith("/"):
            return

        new_prompt = message.text.strip() if message.text else ""
        if not new_prompt or len(new_prompt) < 10:
            try:
                await client.edit_message_text(
                    message.chat.id, msg_id,
                    "❌ **PROMPT TERLALU PENDEK**\n\nMinimal 10 karakter.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Pelatihan AI", callback_data="nx_trainer_menu")]]),
                    parse_mode=ParseMode.MARKDOWN,
                )
            except Exception:
                pass
            try: await message.delete()
            except Exception: pass
            return

        if _TRAINER_PANEL_AVAILABLE:
            await save_bot_config(GROQ_CONFIG_KEY_PROMPT, new_prompt)
        try:
            edited_msg = await client.get_messages(message.chat.id, msg_id)
            await _render_trainer_menu(edited_msg)
        except Exception:
            pass
        try: await message.delete()
        except Exception: pass
        return

    # Cek whitelist FSM dulu
    if message.from_user.id in _whitelist_fsm:
        if message.text and message.text.startswith("/"):
            _whitelist_fsm.pop(message.from_user.id, None)  # Clear FSM agar tidak stuck
            return
        msg_id    = _whitelist_fsm.pop(message.from_user.id)
        raw_input = unicodedata.normalize("NFKC", message.text.strip())
        kata_list = [k.strip() for k in raw_input.split("|") if k.strip()]

        if not kata_list:
            try:
                await client.edit_message_text(
                    message.chat.id, msg_id,
                    "❌ **INPUT KOSONG**\n\nKirim minimal satu kata.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Whitelist", callback_data="nx_whitelist_page_1")]]),
                    parse_mode=ParseMode.MARKDOWN,
                )
            except Exception:
                pass
            try: await message.delete()
            except Exception: pass
            return

        pola, mutasi_display, _ = _build_owner_interlock(kata_list)
        if not pola:
            try:
                await client.edit_message_text(
                    message.chat.id, msg_id,
                    "❌ **GAGAL GENERATE**\n\nSemua kata kosong setelah normalisasi.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Whitelist", callback_data="nx_whitelist_page_1")]]),
                    parse_mode=ParseMode.MARKDOWN,
                )
            except Exception:
                pass
            try: await message.delete()
            except Exception: pass
            return

        try:
            re.compile(pola)
        except re.error as e:
            try:
                await client.edit_message_text(
                    message.chat.id, msg_id,
                    f"❌ **REGEX ERROR**\n\n`{e}`",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Whitelist", callback_data="nx_whitelist_page_1")]]),
                    parse_mode=ParseMode.MARKDOWN,
                )
            except Exception:
                pass
            try: await message.delete()
            except Exception: pass
            return

        raw_joined = " | ".join([k for k, _ in mutasi_display])
        await nexus_whitelist_add(
            pola      = pola,
            raw       = raw_joined,
            kata_list = [k for k, _ in mutasi_display],
            mutasi    = {k: m for k, m in mutasi_display},
        )
        invalidate_nexus_wl_cache()
        header = f"🛡️ **`{raw_joined}`** berhasil dikunci ke Whitelist!\n\n"
        await _render_whitelist_page(client, message.chat.id, msg_id, 1, header=header)
        try: await message.delete()
        except Exception: pass
        return

    if message.from_user.id not in _owner_regex_fsm:
        return
    if message.text and message.text.startswith("/"):
        _owner_regex_fsm.pop(message.from_user.id, None)  # Clear FSM agar tidak stuck
        return

    msg_id   = _owner_regex_fsm.pop(message.from_user.id)
    raw_input = unicodedata.normalize("NFKC", message.text.strip())

    kata_list = [k.strip() for k in raw_input.split("|") if k.strip()]
    if not kata_list:
        try:
            await client.edit_message_text(
                message.chat.id, msg_id,
                "❌ **INPUT KOSONG**\n\nKirim minimal satu kata.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 TRIGGER AI", callback_data="nx_owner_regex_page_1")]]),
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception:
            pass
        try:
            await message.delete()
        except Exception:
            pass
        return

    pola, mutasi_display, _ = _build_owner_interlock(kata_list)

    if not pola:
        try:
            await client.edit_message_text(
                message.chat.id, msg_id,
                "❌ **GAGAL GENERATE**\n\nSemua kata kosong setelah normalisasi.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 TRIGGER AI", callback_data="nx_owner_regex_page_1")]]),
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception:
            pass
        try:
            await message.delete()
        except Exception:
            pass
        return

    try:
        re.compile(pola)
    except re.error as e:
        try:
            await client.edit_message_text(
                message.chat.id, msg_id,
                f"❌ **REGEX ERROR**\n\nError: `{e}`",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 TRIGGER AI", callback_data="nx_owner_regex_page_1")]]),
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception:
            pass
        try:
            await message.delete()
        except Exception:
            pass
        return

    raw_joined = " | ".join([k for k, _ in mutasi_display])

    await regex_db.update_one(
        {"pola": pola},
        {"$set": {
            "pola":      pola,
            "pattern":   pola,
            "raw":       raw_joined,
            "kata_list": [k for k, _ in mutasi_display],
            "mutasi":    {k: m for k, m in mutasi_display},
        }},
        upsert=True,
    )
    invalidate_nexus_counts()

    header = f"✅ **`{raw_joined}`** Full Interlock berhasil dikunci ke Core Database!\n\n"
    await _render_owner_regex_page(client, message.chat.id, msg_id, 1, header=header)
    try:
        await message.delete()
    except Exception:
        pass


async def _render_owner_regex_page(client, chat_id: int, msg_id: int, page: int, header: str = ""):
    limit  = 5
    offset = (page - 1) * limit
    total  = await get_owner_regex_count()
    docs   = [doc async for doc in regex_db.find({}).sort("_id", -1).skip(offset).limit(limit)]
    total_pages = max(1, (total + limit - 1) // limit)

    if docs:
        body = ""
        del_buttons = []
        for local_i, doc in enumerate(docs):
            global_idx = offset + local_i
            raw        = doc.get("raw", "—")
            pola_full  = doc.get("pola", doc.get("pattern", ""))
            kata_list  = doc.get("kata_list", [])
            mutasi_map = doc.get("mutasi", {})

            if not kata_list and raw != "—":
                kata_list = [k.strip() for k in raw.split("|") if k.strip()]

            jalur_tag = f"[OWNER-{global_idx + 1}]"
            body += f"🔑 **ID Jalur:** `{jalur_tag}`\n"
            body += "📝 **Koleksi Asli:** " + ", ".join(f"`{k}`" for k in kata_list) + "\n"

            if mutasi_map:
                body += "🔍 **Probabilitas Lolos Mutasi (>=50%):**\n"
                for kata in kata_list:
                    mutasi = mutasi_map.get(kata, generate_kandidat_mutasi_liar(kata))
                    body  += f"• `{kata}` ➔ `({'|'.join(mutasi)})`\n"
            elif kata_list:
                body += "🔍 **Probabilitas Lolos Mutasi (>=50%):**\n"
                for kata in kata_list:
                    import re as _re2
                    kata_b = _re2.sub(r"[^\w]", "", kata).strip()
                    if kata_b:
                        mutasi = generate_kandidat_mutasi_liar(kata_b)
                        body  += f"• `{kata}` ➔ `({'|'.join(mutasi)})`\n"

            if pola_full:
                body += f"💥 **Full Interlock (Acuan Utama):**\n`{pola_full}`\n"
            body += "──────────────────────────\n"

            doc_id = str(doc["_id"])
            del_buttons.append([InlineKeyboardButton(f"🗑 Hapus: {raw[:40]}", callback_data=f"nx_owner_rgx_del_{doc_id}")])

        content = f"⚡ Total Trigger AI: **{total} pola**\n\n{body}"
    else:
        content    = "📭 **Belum ada Trigger AI.**"
        del_buttons = []

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("⏪ SEBELUMNYA", callback_data=f"nx_owner_regex_page_{page-1}"))
    if (offset + limit) < total:
        nav.append(InlineKeyboardButton("SELANJUTNYA ⏩", callback_data=f"nx_owner_regex_page_{page+1}"))

    rows = del_buttons.copy()
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("➕ Tambah Regex Baru", callback_data=f"nx_owner_rgx_add_{page}")])
    rows.append([InlineKeyboardButton("🔙 KEMBALI", callback_data="nx_global_regex_menu")])

    text = (f"⚙️ **TRIGGER AI — HAL {page}/{total_pages}**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n{header}{content}")
    try:
        await client.edit_message_text(chat_id, msg_id, text[:4000], reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        print(f"[_render_owner_regex_page] {e}")


async def _render_catword_menu(client, chat_id: int, msg_id: int, header: str = ""):
    """Menu utama Kategori Kata — daftar 5 tombol kategori."""
    from database import get_category_word_count, CATEGORY_WORD_LIST
    rows = []
    for cat in CATEGORY_WORD_LIST:
        n_cust = await get_category_word_count(cat)
        n_def  = len(CATWORD_DEFAULTS.get(cat, []))
        label  = f"{CATWORD_LABELS.get(cat, cat)}  ({n_def} default + {n_cust} kustom)"
        rows.append([InlineKeyboardButton(label, callback_data=f"nx_catword_page_{cat}_1")])
    rows.append([InlineKeyboardButton("🔙 KEMBALI", callback_data="nx_owner_menu")])

    text = (
        "🗂️ **KATEGORI KATA (CategoryDetector)**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{header}"
        "Tambah/hapus kata RAW per kategori — bukan generate regex, cuma "
        "substring match sederhana. Kata default (hasil kurasi awal) tidak "
        "bisa dihapus dari sini; kata kustom yang owner tambahkan bisa "
        "dihapus kapan saja.\n\n"
        "Pilih kategori:"
    )
    try:
        await client.edit_message_text(chat_id, msg_id, text, reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        print(f"[_render_catword_menu] {e}")


async def _render_catword_page(client, chat_id: int, msg_id: int, category: str, page: int, header: str = ""):
    """Sub-panel 1 kategori — daftar kata default (read-only) + kata kustom (hapus-able) + tambah."""
    from database import get_category_words_page

    limit = 8
    docs, total_cust = await get_category_words_page(category, page, limit)
    total_pages = max(1, (total_cust + limit - 1) // limit)

    defaults = CATWORD_DEFAULTS.get(category, [])
    body = "📝 **Kata Default:**\n" + (", ".join(f"`{w}`" for w in defaults) if defaults else "_(kosong)_") + "\n\n"

    del_buttons = []
    if docs:
        body += f"➕ **Kata Kustom Owner** ({total_cust} total, hal {page}/{total_pages}):\n"
        for doc in docs:
            raw = doc.get("raw", "—")
            body += f"• `{raw}`\n"
            doc_id = str(doc["_id"])
            del_buttons.append([InlineKeyboardButton(f"🗑 Hapus: {raw[:40]}", callback_data=f"nx_catword_del_{category}_{doc_id}_{page}")])
    else:
        body += "➕ **Kata Kustom Owner:** _(belum ada, tambahkan di bawah)_\n"

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("⏪ SEBELUMNYA", callback_data=f"nx_catword_page_{category}_{page-1}"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("SELANJUTNYA ⏩", callback_data=f"nx_catword_page_{category}_{page+1}"))

    rows = del_buttons.copy()
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("➕ Tambah Kata Baru", callback_data=f"nx_catword_add_{category}_{page}")])
    rows.append([InlineKeyboardButton("🔙 KEMBALI KE KATEGORI", callback_data="nx_catword_menu")])

    label = CATWORD_LABELS.get(category, category)
    text  = f"{label} — **HAL {page}/{total_pages}**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n{header}{body}"
    try:
        await client.edit_message_text(chat_id, msg_id, text[:4000], reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        print(f"[_render_catword_page] {e}")


async def _render_trainer_menu(msg):
    """Render panel status "Pelatihan AI" (v11) — training gate untuk Manual
    AI. GANTIKAN _render_groq_menu lama — backend training sekarang
    OpenRouter (nvidia/nemotron-3-ultra:free), BUKAN Groq lagi (Groq sudah
    dipindah perannya ke VC Chatter & DM Chat fallback, lihat
    security_os/promo_vc_chat.py & promo_dm_chat.py)."""
    if not _TRAINER_PANEL_AVAILABLE:
        await _safe_edit_html(
            msg,
            "⚠️ <b>Pelatihan AI tidak tersedia</b>\n\n"
            "<blockquote>Modul <code>nexus/ai_core</code> gagal dimuat.</blockquote>",
            InlineKeyboardMarkup([[InlineKeyboardButton("🔙 KEMBALI", callback_data="nx_owner_menu")]]),
        )
        return

    status = await nexus_ai_trainer_status()
    key_ok       = status["api_key_set"]
    enabled      = status["enabled"]
    prompt       = status["prompt"]
    is_default   = status["is_default"]
    rate         = status["rate"]
    model_name   = status.get("model", OPENROUTER_DEFAULT_MODEL)

    total_keys    = rate.get("total_keys", 0)
    active_keys   = rate.get("active_keys", 0)
    key_label     = (
        f"✅ {total_keys} key terpasang ({active_keys} aktif)"
        if key_ok else "❌ Belum diisi (OPENROUTER_API_KEY / OPENROUTER_API_KEYS)"
    )
    enabled_label = "✅ ON" if enabled else "⏸️ OFF"
    toggle_label  = "⏸️ Matikan Pelatihan AI" if enabled else "▶️ Nyalakan Pelatihan AI"
    prompt_label  = "Default" if is_default else "Custom (sudah diedit)"
    # v3.3 — TIDAK dipotong lagi. Sebelumnya prompt[:350]+"…" bikin prompt
    # panjang tidak bisa dibaca utuh dari panel. Sekarang teks lengkap
    # ditaruh di <blockquote> (HTML) — Telegram sendiri yang
    # collapse/expand-nya, jadi tidak ada batas karakter buatan kita lagi
    # (batas asli cuma limit pesan Telegram, ±4096 karakter, jauh lebih
    # longgar dari prompt manapun yang realistis dipakai di sini).
    prompt_html = _html_escape(prompt)

    key_lines = ""
    for k in rate.get("keys", []):
        if k["cooling_down"]:
            key_lines += f"├─ {_html_escape(k['label'])}: 🧊 cooldown {k['cooldown_left']}s ({_html_escape(k['reason'])})\n"
        else:
            key_lines += (
                f"├─ {_html_escape(k['label'])}: ✅ siap "
                f"(<code>{k['calls_last_minute']}/{k['limit_per_minute']}</code>/menit · "
                f"<code>{k['calls_last_24h']}/{k['limit_per_day']}</code>/hari)\n"
            )
    if not key_lines:
        key_lines = "└─ <i>(belum ada key)</i>\n"

    def _build_text(p_html: str) -> str:
        return (
            "🧠 <b>PELATIHAN AI — GERBANG TRAINING MANUAL AI</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "<blockquote>"
            "Pelatihan AI ini <b>TIDAK PERNAH menghapus pesan</b>. Perannya murni\n"
            "menjadi \"guru\" yang melatih Manual AI (Bayes+Feature) lewat online\n"
            "learning — dipanggil HANYA untuk pesan di zona abu-abu yang Manual AI\n"
            "sendiri belum cukup yakin. Kalau satu key kena limit, sistem otomatis\n"
            "pindah ke key lain — training tidak pernah berhenti karena satu key\n"
            f"habis. Model: <code>{_html_escape(model_name)}</code>.\n\n"
            f"🔑 <b>API Key:</b> {key_label}\n"
            f"⚡ <b>Status:</b> {enabled_label}\n"
            f"📋 <b>Prompt Aktif:</b> {prompt_label}\n\n"
            "🔄 <b>ROTASI KEY:</b>\n"
            f"{key_lines}"
            "</blockquote>\n\n"
            "📝 <b>PROMPT SAAT INI</b> — aturan moderasi, BOLEH diedit\n"
            "(ketuk untuk buka/tutup):\n"
            f"<blockquote>{p_html}</blockquote>\n\n"
            "<blockquote>"
            "🔒 <b>FORMAT OUTPUT</b> — SELALU ditempel setelah prompt di atas,\n"
            "TIDAK BISA diedit/dihapus lewat panel ini (fixed di kode):"
            "</blockquote>\n"
            f"<blockquote>{_html_escape(GROQ_FIXED_OUTPUT_FORMAT)}</blockquote>"
        )

    text = _build_text(prompt_html)

    # ── Pengaman limit ASLI Telegram (4096 karakter/pesan) — BUKAN potongan
    # buatan seperti versi lama (prompt[:350]). Cuma trim kalau memang mepet
    # limit sungguhan Telegram, dan itu jarang terjadi kecuali prompt custom
    # yang ditulis owner sangat panjang. ──────────────────────────────────
    _TELEGRAM_MSG_LIMIT = 4096
    _SAFETY_MARGIN = 80
    if len(text) > _TELEGRAM_MSG_LIMIT - _SAFETY_MARGIN:
        overflow = len(text) - (_TELEGRAM_MSG_LIMIT - _SAFETY_MARGIN)
        note = (
            "\n\n⚠️ <i>Prompt dipotong di sini karena melebihi limit pesan "
            "Telegram (4096 karakter). Pakai tombol '✏️ Edit Prompt' untuk "
            "melihat/mengubah versi lengkapnya.</i>"
        )
        trimmed_raw = prompt[: max(0, len(prompt) - overflow - len(note))]
        prompt_html = _html_escape(trimmed_raw) + note
        text = _build_text(prompt_html)

    await _safe_edit_html(
        msg,
        text,
        InlineKeyboardMarkup([
            [InlineKeyboardButton(toggle_label,          callback_data="nx_trainer_toggle")],
            [InlineKeyboardButton("✏️ Edit Prompt",       callback_data="nx_trainer_edit_prompt")],
            [InlineKeyboardButton("🔄 Reset ke Default",  callback_data="nx_trainer_reset_prompt")],
            [InlineKeyboardButton("🔄 Refresh",           callback_data="nx_trainer_menu")],
            [InlineKeyboardButton("🔙 KEMBALI",           callback_data="nx_owner_menu")],
        ]),
    )


async def _render_whitelist_page(client, chat_id: int, msg_id: int, page: int, header: str = ""):
    limit  = 5
    offset = (page - 1) * limit
    docs, total = await nexus_whitelist_page(page, limit)
    total_pages = max(1, (total + limit - 1) // limit)

    if docs:
        body        = ""
        del_buttons = []
        for local_i, doc in enumerate(docs):
            global_idx = offset + local_i
            raw        = doc.get("raw", "—")
            pola_full  = doc.get("pola", "")
            kata_list  = doc.get("kata_list", [])
            mutasi_map = doc.get("mutasi", {})

            if not kata_list and raw != "—":
                kata_list = [k.strip() for k in raw.split("|") if k.strip()]

            body += f"🛡️ **[WL-{global_idx + 1}]**\n"
            body += "📝 **Kata Aman:** " + ", ".join(f"`{k}`" for k in kata_list) + "\n"
            if mutasi_map:
                body += "🔍 **Pola Mutasi (>=50%):**\n"
                for kata in kata_list:
                    mutasi = mutasi_map.get(kata, generate_kandidat_mutasi_liar(kata))
                    body  += f"• `{kata}` ➔ `({'|'.join(mutasi)})`\n"
            elif kata_list:
                body += "🔍 **Pola Mutasi (>=50%):**\n"
                for kata in kata_list:
                    import re as _re3
                    kata_b = _re3.sub(r"[^\w]", "", kata).strip()
                    if kata_b:
                        mutasi = generate_kandidat_mutasi_liar(kata_b)
                        body  += f"• `{kata}` ➔ `({'|'.join(mutasi)})`\n"
            if pola_full:
                body += f"🛡️ **Whitelist Interlock:**\n`{pola_full}`\n"
            body += "──────────────────────────\n"
            del_buttons.append([
                InlineKeyboardButton(f"🗑 Hapus: {raw[:40]}", callback_data=f"nx_wl_del_{str(doc['_id'])}")
            ])

        content = f"🛡️ Total Whitelist: **{total} pola**\n\n{body}"
    else:
        content     = "📭 **Belum ada Whitelist Regex.**\n\n_Gunakan `/wlregex kata1|kata2` atau tombol ➕ di bawah._"
        del_buttons = []

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("⏪ Sebelumnya", callback_data=f"nx_whitelist_page_{page-1}"))
    if (offset + limit) < total:
        nav.append(InlineKeyboardButton("Selanjutnya ⏩", callback_data=f"nx_whitelist_page_{page+1}"))

    rows = del_buttons.copy()
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("➕ Tambah Whitelist Baru", callback_data=f"nx_wl_add_{page}")])
    rows.append([InlineKeyboardButton("🗑️ Hapus Semua Whitelist", callback_data="nx_wl_clear_confirm")])
    rows.append([InlineKeyboardButton("🔙 Kembali ke Nexus",      callback_data="nx_home")])

    text = (
        f"🛡️ **WHITELIST NEXUS — HAL {page}/{total_pages}**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"_Pesan yang cocok pola whitelist tidak akan dihapus meski melanggar regex spam._\n\n"
        f"{header}{content}"
    )
    try:
        await client.edit_message_text(
            chat_id, msg_id, text[:4000],
            reply_markup=InlineKeyboardMarkup(rows),
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        print(f"[_render_whitelist_page] {e}")


# ══════════════════════════════════════════════════════════════════════════════
# CALLBACK ROUTER
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_callback_query(filters.regex(r"^nx_"))
async def nexus_callback_router(client: Client, cq: CallbackQuery):
    data    = cq.data
    user_id = cq.from_user.id

    try:
        await cq.answer()
    except Exception:
        pass

    if data == "nx_back_main":
        try:
            await cq.answer()
        except Exception:
            pass
        from plugins.ui.pages import page_start
        from pyrogram.enums import ParseMode as _PM
        text, keyboard = await page_start(client)
        try:
            await cq.message.edit(text, reply_markup=keyboard, parse_mode=_PM.HTML, disable_web_page_preview=True)
        except Exception:
            pass
        return

    elif data in ("nx_home", "nx_refresh"):
        try:
            await cq.answer()
        except Exception:
            pass
        me   = client.me
        text = await _welcome_text()
        await _safe_edit_html(cq.message, text, _main_markup(me.username))

    elif data == "nx_tutorial":
        await _safe_edit_html(
            cq.message,
            "📚 <b>NEXUS AI — CARA KERJA MESIN</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "<blockquote>"
            "<b>🧩 TAHAP 1 — KLAIM &amp; NORMALISASI</b>\n"
            "Admin lapor spam via <code>/spam</code> (reply ke pesan), atau gate lain\n"
            "(regex grup, link detector, dll) menangkapnya otomatis.\n"
            "Teks dinormalisasi dulu (Unicode NFKD, strip font palsu,\n"
            "hapus karakter berulang, normalisasi leet 0→o/3→e/@→a, dedup\n"
            "kata) lalu diklaim — dedupe global supaya kalimat yang sama\n"
            "tidak diproses dobel. Raw hasil klaim tercatat di Record Data.\n\n"
            "<b>🧬 TAHAP 2 — GENERASI VARIAN &amp; VERIFIKASI GROQ</b>\n"
            "Tiap raw diledakkan jadi beberapa varian (koreksi ejaan +\n"
            "ekspansi kalimat), lalu SEMUA varian dikirim ke Pelatihan AI untuk\n"
            "dinilai murni dari isinya sendiri — SPAM atau BUKAN, dengan\n"
            "skor keyakinan. Ini satu-satunya jalur training AI Manual;\n"
            "tidak ada training langsung tanpa verifikasi Groq.\n\n"
            "<b>🧠 TAHAP 3 — TRAINING AI MANUAL</b>\n"
            "Varian yang cukup yakin SPAM/BUKAN dipakai melatih Bayes +\n"
            "PatternMemory (pola konteks, bukan cuma kata). Owner bisa\n"
            "hapus raw dari Record Data kapan saja — turunannya (klaim +\n"
            "varian) ikut terhapus, dan kontribusinya ke Bayes dicabut.\n\n"
            "<b>⚙️ TAHAP 4 — OWNER MANUAL REGEX</b>\n"
            "Owner bisa menambah regex manual via <code>/addregex</code> atau panel.\n"
            "Input kata diproses pipeline mutasi + interlock yang sama\n"
            "dan disimpan terpisah sebagai lapisan prioritas tinggi.\n\n"
            "<b>🛡️ WHITELIST NEXUS</b>\n"
            "Pola yang diketahui false positive bisa diwhitelist:\n"
            "bot tidak akan menghapus pesan yang cocok whitelist,\n"
            "meski cocok dengan pola spam."
            "</blockquote>\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "<i>Gunakan 🧪 Lab Sandbox untuk menguji kalimat secara live.</i>",
            _back_main(),
        )

    elif data == "nx_sandbox_hub":
        await _safe_edit_html(
            cq.message,
            "🧪 <b>NEXUS SANDBOX LAB</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "<blockquote>"
            "Uji coba kalimat terhadap <b>semua lapisan filter aktif</b>:\n"
            "◈ Pola AI Interlock (regex hasil generate lama — legacy)\n"
            "◈ Trigger AI (pola yang ditambah secara manual)\n"
            "◈ Whitelist Nexus (pola yang dikecualikan dari deteksi)\n\n"
            "Hasilnya menampilkan:\n"
            "✓ Apakah kalimat terdeteksi sebagai spam\n"
            "✓ Pola mana yang mencocokkan\n"
            "✓ Apakah tertahan oleh whitelist"
            "</blockquote>\n\n"
            "<i>Tekan <b>Mulai Simulasi</b>, lalu balas (reply) prompt bot dengan\n"
            "kalimat yang ingin kamu uji.</i>",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🚀  Mulai Simulasi",  callback_data="nx_pancing_sandbox")],
                [InlineKeyboardButton("🔙  Kembali",         callback_data="nx_home")],
            ]),
        )

    elif data == "nx_pancing_sandbox":
        await client.send_message(
            chat_id=cq.message.chat.id,
            text="🧪 [NEXUS SANDBOX SIMULATION MODE]\nBalas (reply) pesan ini dengan kalimat yang ingin diuji:",
            reply_markup=ForceReply(selective=True),
        )
        await cq.message.delete()

    elif data == "nx_ai_sandbox_hub":
        if user_id != OWNER_ID:
            try:
                await cq.answer("🔒 Fitur ini aktif & berfungsi normal.\nHanya Owner bot yang bisa mengaksesnya.", show_alert=True)
            except Exception:
                pass
            return
        try:
            await cq.answer()
        except Exception:
            pass
        await _safe_edit_html(
            cq.message,
            "🧠 <b>SANDBOX AI MANUAL (GATE E)</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "<blockquote>"
            "Beda dari <b>Lab Sandbox</b> (cuma cek pola regex) — sandbox ini\n"
            "mem-<i>mirror</i> PERSIS jalur produksi Gate E di\n"
            "<code>core/antispam_queue.py::_gate_nexus_ai</code>:\n\n"
            "◈ Generate varian dari Regex Helper (kamus-alay) + Fuzzy Expand\n"
            "◈ Scan SEMUA varian pakai <code>nexus_ai_auto_detect()</code> — model &amp;\n"
            f"  threshold yang SAMA dengan produksi (min_confidence {NEXUS_MIN_CONFIDENCE:.2f},\n"
            "  diatur lewat <code>.env</code>: <code>NEXUS_MIN_CONFIDENCE</code>)\n"
            "◈ Breakdown lengkap <code>explain()</code>: skor Bayes, Feature, Category,\n"
            "  Pattern Memory, Context modifier, hingga confidence gabungan\n\n"
            "Hasilnya menunjukkan apakah pesan ini <b>akan dihapus di grup\n"
            "sungguhan</b> kalau <code>anti_spam_ai</code> aktif — tanpa perlu kirim ke\n"
            "grup asli."
            "</blockquote>\n\n"
            "<i>Tekan <b>Mulai Simulasi</b>, lalu balas (reply) prompt bot dengan\n"
            "kalimat yang ingin kamu uji.</i>",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🚀  Mulai Simulasi",  callback_data="nx_ai_sandbox_start")],
                [InlineKeyboardButton("🔙  Kembali",         callback_data="nx_owner_menu")],
            ]),
        )

    elif data == "nx_ai_sandbox_start":
        if user_id != OWNER_ID:
            try:
                await cq.answer("🔒 Hanya Owner bot.", show_alert=True)
            except Exception:
                pass
            return
        await client.send_message(
            chat_id=cq.message.chat.id,
            text="🧠 [NEXUS AI MANUAL SANDBOX MODE]\nBalas (reply) pesan ini dengan kalimat yang ingin diuji:",
            reply_markup=ForceReply(selective=True),
        )
        await cq.message.delete()

    elif data == "nx_owner_menu":
        if user_id != OWNER_ID:
            try:
                await cq.answer(
                    "🔒 Fitur ini aktif & berfungsi normal.\nHanya Owner bot yang bisa mengaksesnya.",
                    show_alert=True
                )
            except Exception:
                pass
            return
        try:
            await cq.answer()
        except Exception:
            pass
        await _safe_edit_html(
            cq.message,
            _OWNER_PANEL_TEXT,
            InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("📊  Record Data",      callback_data="nx_records_page_1"),
                    InlineKeyboardButton("📂  Grup Terdaftar",   callback_data="nx_list_grup"),
                ],
                [
                    InlineKeyboardButton("🔄  Refresh Metrik",   callback_data="nx_refresh"),
                    InlineKeyboardButton("🧠  Lihat AI",         callback_data="nx_lihat_ai"),
                ],
                [
                    InlineKeyboardButton("📋  Log Regex/Kategori", callback_data="nx_actlog_page_1"),
                    InlineKeyboardButton("🔬  Debug AI (24j)",  callback_data="nx_ai_debug_page_1"),
                ],
                [InlineKeyboardButton("🩺  Cek Antrean TAHAP 2", callback_data="nx_tahap2_debug")],
                [InlineKeyboardButton("🗑️  Reset Integrasi", callback_data="nx_menu_reset")],
                [InlineKeyboardButton("🧠  Pelatihan AI",         callback_data="nx_trainer_menu")],
                [InlineKeyboardButton("🗂️  Kategori Kata",       callback_data="nx_catword_menu")],
                [InlineKeyboardButton("🧪  Sandbox AI Manual",   callback_data="nx_ai_sandbox_hub")],
                [InlineKeyboardButton("📱  Ganti Userbot",       callback_data="nx_setuserbot")],
                [InlineKeyboardButton("🔙  Kembali ke Nexus",   callback_data="nx_home")],
            ])
        )

    elif data == "nx_tahap2_debug":
        if user_id != OWNER_ID:
            try:
                await cq.answer("🔒 Fitur ini hanya untuk Owner bot.", show_alert=True)
            except Exception:
                pass
            return
        try:
            await cq.answer()
        except Exception:
            pass
        from core.spam_claim_queue import spam_claim_debug_snapshot, SPAM_CLAIM_POLL_INTERVAL
        snap = await spam_claim_debug_snapshot()

        if snap["worker_alive"]:
            status_line = f"🟢 <b>Worker HIDUP</b> (loop terakhir {snap['heartbeat_age_secs']}s lalu)"
        elif snap["heartbeat_age_secs"] is None:
            status_line = "⚪ Belum ada data heartbeat (baru saja start / belum sempat loop)"
        else:
            status_line = f"🔴 <b>Worker MATI/NYANGKUT</b> (loop terakhir {snap['heartbeat_age_secs']}s lalu — normalnya &lt;{SPAM_CLAIM_POLL_INTERVAL * 3:.0f}s)"

        lb = snap["last_batch_result"]
        if "processed" in lb:
            last_line = f"Batch terakhir: {lb['processed']} sukses, {lb['failed']} gagal"
        elif "skipped" in lb:
            last_line = f"Batch terakhir: dilewati ({lb['skipped']})"
        elif "loop_error" in lb:
            last_line = f"⚠️ Loop terakhir ERROR: {lb['loop_error']}"
        else:
            last_line = "Belum ada data batch."

        rb = snap["retry_buckets"]
        text = (
            "🩺 <b>CEK ANTREAN TAHAP 2</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{status_line}\n"
            f"<i>{last_line}</i>\n\n"
            f"⏳ <b>Total belum digenerate:</b> {snap['pending_total']}\n"
            "<blockquote>"
            f"├─ Belum dicoba sama sekali : {rb['0']}\n"
            f"├─ Gagal 1-2x (masih dicoba) : {rb['1-2']}\n"
            f"├─ Gagal 3-4x (mau di-skip)  : {rb['3-4']}\n"
            f"└─ Gagal ≥5x (bug/anomali)   : {rb['5+']}"
            "</blockquote>\n"
        )
        if snap["orphan_count_known"] > 0:
            text += (
                f"\n👻 <b>Raw hantu terdeteksi: {snap['orphan_count_known']}</b>\n"
                "<i>(sudah selesai diproses tapi status Record Data gak ke-update — "
                "bug link Mongo, bukan macet beneran)</i>\n"
            )
        if snap["sample_failing"]:
            text += "\n<b>📛 Contoh yang GAGAL (retry &gt; 0):</b>\n"
            for it in snap["sample_failing"][:8]:
                err = (it.get("last_error") or "?")[:60]
                text += f"• <code>{it['text'][:40]}</code> — {it['retry_count']}x — {err}\n"
        if snap["sample_pending_clean"]:
            text += "\n<b>⏳ Contoh yang MASIH ANTRE (belum dicoba):</b>\n"
            for it in snap["sample_pending_clean"][:8]:
                text += f"• <code>{it['text'][:50]}</code>\n"

        await _safe_edit_html(
            cq.message,
            text[:4000],
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄  Refresh", callback_data="nx_tahap2_debug")],
                [InlineKeyboardButton("🔙  KEMBALI", callback_data="nx_owner_menu")],
            ])
        )

    elif data == "nx_setuserbot":
        if user_id != OWNER_ID:
            try:
                await cq.answer(
                    "🔒 Fitur ini hanya untuk Owner bot.",
                    show_alert=True
                )
            except Exception:
                pass
            return
        try:
            await cq.answer()
        except Exception:
            pass
        # Import FSM setuserbot dari handlers_secos dan mulai dengan chat_id=0
        # chat_id=0 → konteks owner panel (bukan per-grup)
        from plugins.ui.handlers_secos import (
            _pending_setuserbot, _cancel_setuserbot_task,
            _setuserbot_timeout, WAIT_TIMEOUT_UB,
        )
        from plugins.ui.handlers_dm import safe_edit
        _cancel_setuserbot_task(user_id)
        _pending_setuserbot[user_id] = {
            "chat_id": 0,       # 0 = konteks owner, bukan per-grup
            "msg_id":  cq.message.id,
            "_task":   None,
        }
        await safe_edit(
            cq.message,
            "📱 <b>GANTI USERBOT</b>\n"
            "<i>Diakses dari Owner Bot Panel</i>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Kirim <b>nomor HP</b> akun userbot baru ke sini.\n\n"
            "<b>📋 LANGKAH-LANGKAH:</b>\n\n"
            "<b>1️⃣ Siapkan akun Telegram</b>\n"
            "   Gunakan akun biasa (bukan bot) yang akan dijadikan userbot.\n\n"
            "<b>2️⃣ Kirim nomor HP ke sini</b>\n"
            "   Format internasional: <code>+628123456789</code>\n\n"
            "<b>3️⃣ Masukkan OTP</b>\n"
            "   Telegram akan mengirim kode OTP ke nomor tersebut.\n"
            "   Kirim kode via DM bot dengan format: <code>/otp &lt;kode&gt;</code>\n\n"
            "<b>4️⃣ Adminkan userbot ke grup</b>\n"
            "   Setelah login berhasil, jadikan userbot admin dengan izin\n"
            "   <code>Kelola Obrolan Video</code> di setiap grup Security OS.\n\n"
            "⚠️ <b>Userbot lama akan diputus dan session-nya dihapus.</b>\n\n"
            f"<i>⏳ Batas waktu input: {WAIT_TIMEOUT_UB // 60} menit.</i>\n"
            "<i>Kirim /batal untuk membatalkan.</i>",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🚫  Batalkan", callback_data="nx_owner_menu")]
            ])
        )
        task = asyncio.create_task(
            _setuserbot_timeout(user_id, 0, cq.message, cq._client)
        )
        if user_id in _pending_setuserbot:
            _pending_setuserbot[user_id]["_task"] = task

    elif data == "nx_trainer_menu":
        if user_id != OWNER_ID:
            try:
                await cq.answer("🔒 Hanya Owner bot.", show_alert=True)
            except Exception:
                pass
            return
        try:
            await cq.answer()
        except Exception:
            pass
        await _render_trainer_menu(cq.message)

    elif data == "nx_trainer_toggle":
        if user_id != OWNER_ID:
            try:
                await cq.answer("🔒 Hanya Owner bot.", show_alert=True)
            except Exception:
                pass
            return
        if not _TRAINER_PANEL_AVAILABLE:
            try:
                await cq.answer("⚠️ Modul Pelatihan AI tidak tersedia.", show_alert=True)
            except Exception:
                pass
            return
        from database import get_bot_config
        current = await get_bot_config(GROQ_CONFIG_KEY_ENABLED, default=True)
        new_val = False if current is not False else True
        await save_bot_config(GROQ_CONFIG_KEY_ENABLED, new_val)
        try:
            await cq.answer("✅ Pelatihan AI DINYALAKAN" if new_val else "⏸️ Pelatihan AI DIMATIKAN", show_alert=True)
        except Exception:
            pass
        await _render_trainer_menu(cq.message)

    elif data == "nx_trainer_edit_prompt":
        if user_id != OWNER_ID:
            try:
                await cq.answer("🔒 Hanya Owner bot.", show_alert=True)
            except Exception:
                pass
            return
        try:
            await cq.answer()
        except Exception:
            pass
        _trainer_prompt_fsm[user_id] = cq.message.id
        await _safe_edit(
            cq.message,
            "✏️ **MODE EDIT PROMPT GROQ AKTIF**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Kirim teks prompt baru untuk Pelatihan AI (aturan moderasi spam/aman saja).\n"
            "Prompt ini langsung aktif setelah dikirim — TANPA redeploy/restart.\n\n"
            "🔒 _Instruksi format output JSON TIDAK PERLU ditulis di sini — "
            "sistem selalu menempelkannya otomatis di akhir, terlepas apapun "
            "yang kamu kirim. Fokus saja ke aturan spam/amannya._\n\n"
            "💡 _Kirim /batal untuk membatalkan._",
            InlineKeyboardMarkup([[InlineKeyboardButton("🚫 Batalkan", callback_data="nx_trainer_menu")]])
        )

    elif data == "nx_trainer_reset_prompt":
        if user_id != OWNER_ID:
            try:
                await cq.answer("🔒 Hanya Owner bot.", show_alert=True)
            except Exception:
                pass
            return
        if not _TRAINER_PANEL_AVAILABLE:
            try:
                await cq.answer("⚠️ Modul Pelatihan AI tidak tersedia.", show_alert=True)
            except Exception:
                pass
            return
        await save_bot_config(GROQ_CONFIG_KEY_PROMPT, GROQ_DEFAULT_PROMPT)
        try:
            await cq.answer("🔄 Prompt dikembalikan ke default.", show_alert=True)
        except Exception:
            pass
        await _render_trainer_menu(cq.message)

    elif data == "nx_list_grup":
        if user_id != OWNER_ID:
            try:
                await cq.answer(
                    "🔒 Fitur ini aktif & berfungsi normal.\nHanya Owner bot yang bisa mengaksesnya.",
                    show_alert=True
                )
            except Exception:
                pass
            return
        try:
            await cq.answer("⏳ Memuat daftar grup...")
        except Exception:
            pass

        text = await build_grup_terdaftar_text(client)
        await _safe_edit_html(cq.message, text, _back_main())

    elif data == "nx_lihat_ai":
        if user_id != OWNER_ID:
            try:
                await cq.answer(
                    "🔒 Fitur ini aktif & berfungsi normal.\nHanya Owner bot yang bisa mengaksesnya.",
                    show_alert=True
                )
            except Exception:
                pass
            return
        try:
            await cq.answer("⏳ Mengambil data AI...")
        except Exception:
            pass
        try:
            s = await nexus_ai_get_full_stats()
        except Exception as e:
            await _safe_edit(
                cq.message,
                f"❌ **Gagal ambil data AI**\n`{e}`",
                InlineKeyboardMarkup([[InlineKeyboardButton("🔙 KEMBALI", callback_data="nx_owner_menu")]]),
            )
            return

        if "error" in s:
            await _safe_edit(
                cq.message,
                f"⚠️ **NEXUS AI CORE**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n_{s['error']}_",
                InlineKeyboardMarkup([[InlineKeyboardButton("🔙 KEMBALI", callback_data="nx_owner_menu")]]),
            )
            return

        # Format statistik model
        total_check  = s.get("total_checked", 0)
        total_spam   = s.get("total_spam", 0)
        total_ham    = s.get("total_ham", 0)
        learn_count  = s.get("learn_count", 0)
        vocab_size   = s.get("vocab_size", 0)
        spam_samples = s.get("spam_samples", 0)
        ham_samples  = s.get("ham_samples", 0)
        last_upd     = s.get("last_updated", "-") or "-"
        if last_upd and last_upd != "-":
            last_upd = last_upd[:16].replace("T", " ")  # format rapi

        thr     = s.get("threshold_detail", {})
        thr_val = thr.get("threshold", s.get("threshold", "?"))
        thr_sp  = thr.get("spam_mean", "?")
        thr_hm  = thr.get("ham_mean", "?")

        version = s.get("version", "?")
        loaded  = "✅ Aktif" if s.get("loaded") else "❌ Belum"

        akurasi = "-"
        if total_check > 0:
            akurasi = f"{(total_spam / total_check * 100):.1f}% terdeteksi spam"

        text = (
            "🧠 **NEXUS AI CORE — STATUS & AKTIVITAS**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🔖 **Versi Model:** `v{version}`\n"
            f"⚡ **Status:** {loaded}\n"
            f"🕐 **Terakhir Diperbarui:** `{last_upd}`\n\n"
            "📊 **STATISTIK DETEKSI:**\n"
            f"├─ Total Diperiksa : `{total_check:,}`\n"
            f"├─ Terdeteksi Spam : `{total_spam:,}`\n"
            f"├─ Terdeteksi Aman : `{total_ham:,}`\n"
            f"└─ Rasio           : `{akurasi}`\n\n"
            "🎓 **ONLINE LEARNING:**\n"
            f"├─ Total Laporan   : `{learn_count:,}` kali belajar\n"
            f"├─ Sampel Spam     : `{spam_samples:,}`\n"
            f"└─ Sampel Ham      : `{ham_samples:,}`\n\n"
            "📚 **MODEL NAIVE BAYES:**\n"
            f"└─ Vocab Size      : `{vocab_size:,}` token\n\n"
            "🎯 **ADAPTIVE THRESHOLD:**\n"
            f"├─ Threshold Aktif : `{thr_val}`\n"
            f"├─ Rata-rata Spam  : `{thr_sp}`\n"
            f"└─ Rata-rata Ham   : `{thr_hm}`\n"
        )

        # v5.0 — PatternMemory (konteks pola, bukan cuma kata) — datanya sudah
        # ada di nexus_ai_get_full_stats() sejak lama tapi tidak pernah
        # dirender di sini. Ditambahkan supaya panel ini benar-benar
        # mencerminkan seluruh mesin AI yang aktif sekarang.
        pm = s.get("pattern_memory", {}) or {}
        if pm and "error" not in pm:
            text += (
                "\n🧩 **PATTERN MEMORY (Konteks):**\n"
                f"├─ Pola Spam Tersimpan    : `{pm.get('pattern_spam_stored', 0):,}`\n"
                f"├─ Pola Non-Spam Tersimpan: `{pm.get('pattern_nonspam_stored', 0):,}`\n"
                f"├─ Total Belajar Spam     : `{pm.get('pattern_spam_learned', 0):,}`\n"
                f"└─ Total Belajar Non-Spam : `{pm.get('pattern_nonspam_learned', 0):,}`\n"
            )

        # Mode training Pelatihan AI — status lengkap ada di panel terpisah "🧠 Pelatihan AI"
        groq_mode = s.get("groq_trigger_mode")
        if groq_mode:
            text += f"\n🧠 _Training mode: `{groq_mode}` — detail lengkap di panel Pelatihan AI_\n"

        # Log aktivitas terbaru
        logs = s.get("recent_log", [])
        if logs:
            text += "\n📋 **LOG AKTIVITAS TERBARU:**\n"
            text += "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            for line in logs[-10:]:  # tampilkan 10 terakhir
                # Singkat baris log agar tidak terlalu panjang
                short = line[11:] if len(line) > 11 else line  # potong timestamp awal
                text += f"`{short[:90]}`\n"
        else:
            text += "\n_📋 Log belum ada. Bot baru dijalankan atau log kosong._\n"

        # Potong agar tidak melebihi limit Telegram 4096
        if len(text) > 3900:
            text = text[:3900] + "\n…_(dipotong)_"

        await _safe_edit(
            cq.message,
            text,
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Refresh", callback_data="nx_lihat_ai")],
                [InlineKeyboardButton("🔙 KEMBALI", callback_data="nx_owner_menu")],
            ]),
        )

    elif data == "nx_global_regex_menu":
        try:
            await cq.answer()
        except Exception:
            pass
        ai_ct    = await nexus_get_regex_count()
        owner_ct = await get_owner_regex_count()
        wl_ct    = await nexus_whitelist_count()
        await _safe_edit(
            cq.message,
            "🔮 **GLOBAL REGEX — SUB MENU**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🧬 **AI Interlock Pattern:** `{ai_ct} pola`\n"
            f"⚙️ **Trigger AI (Manual):** `{owner_ct} pola`\n"
            f"🛡️ **Whitelist Nexus:** `{wl_ct} pola`\n\n"
            "Pilih panel yang ingin dibuka:",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🧬 VISUALISASI FILTER", callback_data="nx_regex_page_1")],
                [InlineKeyboardButton("⚙️ TRIGGER AI",        callback_data="nx_owner_regex_page_1")],
                [InlineKeyboardButton("🛡️ WHITELIST NEXUS",   callback_data="nx_whitelist_page_1")],
                [InlineKeyboardButton("🔙 KEMBALI KE MAINFRAME", callback_data="nx_home")],
            ]),
        )

    elif data.startswith("nx_regex_page_"):
        try:
            await cq.answer()
        except Exception:
            pass
        cp    = int(data.split("_")[-1])
        rows, total = await nexus_get_regex_page(cp, 5)
        limit = 5
        off   = (cp - 1) * limit

        if not rows:
            await _safe_edit(cq.message, "🧬 **VISUALISASI FILTER**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n_Mainframe belum memiliki koleksi pola interlock._", InlineKeyboardMarkup([[InlineKeyboardButton("🔙 KEMBALI", callback_data="nx_global_regex_menu")]]))
            return

        text = f"🧬 **MAPS INTELLIGENCE PATTERN SENSOR (HAL {cp}/{(total+limit-1)//limit})**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for row in rows:
            pola_full = row["pola"]
            indikator = row["kata_kunci"]
            kata_raw  = indikator.split("]", 1)[-1] if "]" in indikator else indikator
            kata_list = [k.strip() for k in kata_raw.split("+") if k.strip()]
            jalur_tag = indikator.split("]")[0] + "]" if "]" in indikator else "[?]"

            text += f"🔑 **ID Jalur:** `{jalur_tag}`\n"
            text += "📝 **Koleksi Asli:** " + ", ".join(f"`{k}`" for k in kata_list) + "\n"
            text += "🔍 **Probabilitas Lolos Mutasi (>=50%):**\n"
            for kata in kata_list:
                mutasi = generate_kandidat_mutasi_liar(kata)
                text  += f"• `{kata}` ➔ `({'|'.join(mutasi)})`\n"
            text += f"💥 **Full Interlock (Acuan Utama):**\n`{pola_full}`\n"
            text += "──────────────────────────\n"

        nav = []
        if cp > 1:
            nav.append(InlineKeyboardButton("⏪ SEBELUMNYA", callback_data=f"nx_regex_page_{cp-1}"))
        if (off + limit) < total:
            nav.append(InlineKeyboardButton("SELANJUTNYA ⏩", callback_data=f"nx_regex_page_{cp+1}"))

        rows_kb = []
        if nav:
            rows_kb.append(nav)
        rows_kb.append([InlineKeyboardButton("🔙 KEMBALI", callback_data="nx_global_regex_menu")])
        await _safe_edit(cq.message, text[:3900], InlineKeyboardMarkup(rows_kb))

    elif data.startswith("nx_owner_regex_page_"):
        if user_id != OWNER_ID:
            try:
                await cq.answer("🔒 Fitur ini aktif & berfungsi normal.\nHanya Owner bot yang bisa mengaksesnya.", show_alert=True)
            except Exception:
                pass
            return
        try:
            await cq.answer()
        except Exception:
            pass
        page = int(data.split("_")[-1])
        await _render_owner_regex_page(client, cq.message.chat.id, cq.message.id, page)

    elif data.startswith("nx_owner_rgx_add_"):
        if user_id != OWNER_ID:
            try:
                await cq.answer("🔒 Fitur ini aktif & berfungsi normal.\nHanya Owner bot yang bisa mengaksesnya.", show_alert=True)
            except Exception:
                pass
            return
        try:
            await cq.answer()
        except Exception:
            pass
        page = int(data.split("_")[-1])
        _owner_regex_fsm[user_id] = cq.message.id
        await _safe_edit(
            cq.message,
            "⚙️ **MODE INPUT TRIGGER AI AKTIF**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Kirim kata-kata yang ingin diblokir, pisahkan dengan tanda `|`",
            InlineKeyboardMarkup([[InlineKeyboardButton("🚫 Batalkan", callback_data=f"nx_owner_regex_page_{page}")]])
        )

    elif data.startswith("nx_owner_rgx_del_"):
        if user_id != OWNER_ID:
            try:
                await cq.answer("🔒 Fitur ini aktif & berfungsi normal.\nHanya Owner bot yang bisa mengaksesnya.", show_alert=True)
            except Exception:
                pass
            return
        # Sama persis pola whitelist nexus — pakai nexus_regex_delete_by_id
        # yang handle MongoDB (ObjectId) maupun SQLite (str) secara otomatis
        obj_id  = data[len("nx_owner_rgx_del_"):]
        deleted = await nexus_regex_delete_by_id(obj_id)
        await cq.answer("🗑 Dihapus." if deleted else "⚠️ Tidak ditemukan.", show_alert=False)
        await _render_owner_regex_page(client, cq.message.chat.id, cq.message.id, 1)

    elif data == "nx_catword_menu":
        if user_id != OWNER_ID:
            try:
                await cq.answer("🔒 Fitur ini aktif & berfungsi normal.\nHanya Owner bot yang bisa mengaksesnya.", show_alert=True)
            except Exception:
                pass
            return
        try:
            await cq.answer()
        except Exception:
            pass
        await _render_catword_menu(client, cq.message.chat.id, cq.message.id)

    elif data.startswith("nx_catword_page_"):
        if user_id != OWNER_ID:
            try:
                await cq.answer("🔒 Fitur ini aktif & berfungsi normal.\nHanya Owner bot yang bisa mengaksesnya.", show_alert=True)
            except Exception:
                pass
            return
        try:
            await cq.answer()
        except Exception:
            pass
        # format: nx_catword_page_{CATEGORY}_{page}
        rest     = data[len("nx_catword_page_"):]
        category, _, page_str = rest.rpartition("_")
        await _render_catword_page(client, cq.message.chat.id, cq.message.id, category, int(page_str))

    elif data.startswith("nx_catword_add_"):
        if user_id != OWNER_ID:
            try:
                await cq.answer("🔒 Fitur ini aktif & berfungsi normal.\nHanya Owner bot yang bisa mengaksesnya.", show_alert=True)
            except Exception:
                pass
            return
        try:
            await cq.answer()
        except Exception:
            pass
        # format: nx_catword_add_{CATEGORY}_{page}
        rest     = data[len("nx_catword_add_"):]
        category, _, page_str = rest.rpartition("_")
        page = int(page_str)
        _catword_fsm[user_id] = (category, cq.message.id, page)
        label = CATWORD_LABELS.get(category, category)
        await _safe_edit(
            cq.message,
            f"➕ **MODE INPUT KATA — {label}**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Kirim 1 kata/frasa yang ingin ditambahkan ke kategori ini "
            "(RAW, apa adanya — bukan regex). Kirim `/batal` untuk membatalkan.",
            InlineKeyboardMarkup([[InlineKeyboardButton("🚫 Batalkan", callback_data=f"nx_catword_page_{category}_{page}")]])
        )

    elif data.startswith("nx_catword_del_"):
        if user_id != OWNER_ID:
            try:
                await cq.answer("🔒 Fitur ini aktif & berfungsi normal.\nHanya Owner bot yang bisa mengaksesnya.", show_alert=True)
            except Exception:
                pass
            return
        # format: nx_catword_del_{CATEGORY}_{obj_id}_{page}
        # rsplit maxsplit=2 dari kanan — CATEGORY sendiri bisa mengandung
        # underscore (mis. GROUP_INVITE, PROMO_VIRAL), jadi tidak bisa pakai
        # partition biasa dari kiri.
        rest = data[len("nx_catword_del_"):]
        parts = rest.rsplit("_", 2)
        if len(parts) != 3:
            await cq.answer("⚠️ Data tidak valid.", show_alert=False)
            return
        category, obj_id, page_str = parts
        page = int(page_str) if page_str.isdigit() else 1
        from database import delete_category_word_by_id
        from nexus.ai_core.category_detector import reload_custom_words
        deleted_raw = await delete_category_word_by_id(obj_id)
        if deleted_raw:
            await reload_custom_words()
            try:
                from nexus.ai_core.bridge import get_nexus_ai
                ai = get_nexus_ai()
                if not ai._loaded:
                    await ai.load()
                ai.bayes.untrain(deleted_raw, is_spam=True)
                await ai.save()
            except Exception:
                pass
        await cq.answer("🗑 Dihapus." if deleted_raw else "⚠️ Tidak ditemukan.", show_alert=False)
        await _render_catword_page(client, cq.message.chat.id, cq.message.id, category, page)

    elif data.startswith("nx_records_page_"):
        if user_id != OWNER_ID:
            try:
                await cq.answer("🔒 Fitur ini aktif & berfungsi normal.\nHanya Owner bot yang bisa mengaksesnya.", show_alert=True)
            except Exception:
                pass
            return
        try:
            await cq.answer()
        except Exception:
            pass
        cp    = int(data.split("_")[-1])
        limit = 10
        rows, total = await nexus_get_kalimat_page(cp, limit)
        off   = (cp - 1) * limit
        total_pages = max(1, (total + limit - 1) // limit)

        text = f"📋 **RECORD DATA NEXUS DB — HAL {cp}/{total_pages}**\n"
        text += f"_(Total: {total} kalimat · Ketik /delkalimat untuk hapus via command)_\n\n"
        del_buttons = []
        for idx, row in enumerate(rows, start=(off + 1)):
            icon  = "⏳" if row["status_proses"] == 0 else "✅"
            cuplikan = row["teks"][:60] + ("…" if len(row["teks"]) > 60 else "")
            text += f"`[{idx}]` {icon} `{cuplikan}`\n"
            del_buttons.append([
                InlineKeyboardButton(
                    f"🗑  [{idx}] {cuplikan[:35]}",
                    callback_data=f"nx_rec_del_{row['_id']}_{cp}"
                )
            ])

        nav = []
        if cp > 1:
            nav.append(InlineKeyboardButton("⏪ PREV", callback_data=f"nx_records_page_{cp-1}"))
        if (off + limit) < total:
            nav.append(InlineKeyboardButton("NEXT ⏩", callback_data=f"nx_records_page_{cp+1}"))

        rows_kb = del_buttons.copy()
        if nav:
            rows_kb.append(nav)
        rows_kb.append([InlineKeyboardButton("🔙 KEMBALI", callback_data="nx_owner_menu")])
        await _safe_edit(cq.message, text[:4000], InlineKeyboardMarkup(rows_kb))

    elif data.startswith("nx_rec_del_"):
        if user_id != OWNER_ID:
            try:
                await cq.answer("🔒 Hanya Owner.", show_alert=True)
            except Exception:
                pass
            return
        # format: nx_rec_del_<oid>_<page>
        parts   = data[len("nx_rec_del_"):].rsplit("_", 1)
        oid_str = parts[0]
        cp      = int(parts[1]) if len(parts) > 1 else 1
        deleted = await nexus_delete_kalimat_by_id(oid_str)
        await cq.answer("🗑 Kalimat dihapus." if deleted else "⚠️ Data tidak ditemukan.", show_alert=False)
        # Refresh halaman yang sama (bisa jadi sudah berkurang, clamp ke total_pages baru)
        limit = 10
        _, total_after = await nexus_get_kalimat_page(1, 1)
        total_pages = max(1, (total_after + limit - 1) // limit)
        cp = min(cp, total_pages)
        rows, total = await nexus_get_kalimat_page(cp, limit)
        off = (cp - 1) * limit
        text = f"📋 **RECORD DATA NEXUS DB — HAL {cp}/{total_pages}**\n"
        text += f"_(Total: {total} kalimat · Ketik /delkalimat untuk hapus via command)_\n\n"
        del_buttons = []
        for idx, row in enumerate(rows, start=(off + 1)):
            icon     = "⏳" if row["status_proses"] == 0 else "✅"
            cuplikan = row["teks"][:60] + ("…" if len(row["teks"]) > 60 else "")
            text    += f"`[{idx}]` {icon} `{cuplikan}`\n"
            del_buttons.append([
                InlineKeyboardButton(
                    f"🗑  [{idx}] {cuplikan[:35]}",
                    callback_data=f"nx_rec_del_{row['_id']}_{cp}"
                )
            ])
        nav = []
        if cp > 1:
            nav.append(InlineKeyboardButton("⏪ PREV", callback_data=f"nx_records_page_{cp-1}"))
        if (off + limit) < total:
            nav.append(InlineKeyboardButton("NEXT ⏩", callback_data=f"nx_records_page_{cp+1}"))
        rows_kb = del_buttons.copy()
        if nav:
            rows_kb.append(nav)
        rows_kb.append([InlineKeyboardButton("🔙 KEMBALI", callback_data="nx_owner_menu")])
        await _safe_edit(cq.message, text[:4000], InlineKeyboardMarkup(rows_kb))

    elif data.startswith("nx_whitelist_page_"):
        if user_id != OWNER_ID:
            try:
                await cq.answer("⛔ Hanya Owner!", show_alert=True)
            except Exception:
                pass
            return
        try:
            await cq.answer()
        except Exception:
            pass
        page = int(data.split("_")[-1])
        await _render_whitelist_page(client, cq.message.chat.id, cq.message.id, page)

    elif data.startswith("nx_wl_add_"):
        if user_id != OWNER_ID:
            try:
                await cq.answer("⛔ Hanya Owner!", show_alert=True)
            except Exception:
                pass
            return
        try:
            await cq.answer()
        except Exception:
            pass
        page = int(data.split("_")[-1])
        _whitelist_fsm[user_id] = cq.message.id
        await _safe_edit(
            cq.message,
            "🛡️ **MODE INPUT WHITELIST AKTIF**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Kirim kata-kata yang ingin **diputihkan**, pisahkan dengan `|`\n\n"
            "💡 _Contoh: `sini | di`_",
            InlineKeyboardMarkup([[InlineKeyboardButton("🚫 Batalkan", callback_data=f"nx_whitelist_page_{page}")]])
        )

    elif data.startswith("nx_wl_del_"):
        if user_id != OWNER_ID:
            try:
                await cq.answer("⛔ Hanya Owner!", show_alert=True)
            except Exception:
                pass
            return
        obj_id  = data[len("nx_wl_del_"):]
        deleted = await nexus_whitelist_delete_by_id(obj_id)
        if deleted:
            invalidate_nexus_wl_cache()
        await cq.answer("🗑 Dihapus." if deleted else "⚠️ Tidak ditemukan.", show_alert=False)
        await _render_whitelist_page(client, cq.message.chat.id, cq.message.id, 1)

    elif data == "nx_wl_clear_confirm":
        if user_id != OWNER_ID:
            try:
                await cq.answer("⛔ Hanya Owner!", show_alert=True)
            except Exception:
                pass
            return
        try:
            await cq.answer()
        except Exception:
            pass
        await _safe_edit(
            cq.message,
            "⚠️ **KONFIRMASI HAPUS SEMUA WHITELIST**\n\nSemua pola whitelist akan dihapus permanen. Lanjutkan?",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Ya, Hapus Semua", callback_data="nx_wl_clear_exec")],
                [InlineKeyboardButton("🚫 Batal",           callback_data="nx_whitelist_page_1")],
            ])
        )

    elif data == "nx_wl_clear_exec":
        if user_id != OWNER_ID:
            try:
                await cq.answer("⛔ Hanya Owner!", show_alert=True)
            except Exception:
                pass
            return
        n = await nexus_whitelist_clear()
        invalidate_nexus_wl_cache()
        try:
            await cq.answer(f"🗑 {n} whitelist dihapus.", show_alert=True)
        except Exception:
            pass
        await _render_whitelist_page(client, cq.message.chat.id, cq.message.id, 1)

    elif data == "nx_resetai_batal":
        try:
            await cq.answer("🚫 Dibatalkan.")
        except Exception:
            pass
        try:
            await cq.message.delete()
        except Exception:
            pass

    elif data == "nx_resetai_exec":
        if user_id != OWNER_ID:
            try:
                await cq.answer("⛔ Hanya Owner!", show_alert=True)
            except Exception:
                pass
            return
        try:
            await cq.answer("⏳ Mereset total AI Manual...", show_alert=False)
        except Exception:
            pass

        await _safe_edit(cq.message, "⏳ **Sedang mereset total AI Manual...**\nJangan tutup panel ini.")

        # 1) Record Data: raw TAHAP 1 + varian TAHAP 2 + regex + claim queue
        await nexus_clear_kalimat()
        # 2) Model itu sendiri: Bayes vocab + PatternMemory + AdaptiveThreshold
        #    (termasuk data yang masuk sebelum integrasi Groq)
        stats_before = await nexus_ai_full_reset()

        await _safe_edit(
            cq.message,
            "✅ **RESET TOTAL AI MANUAL SELESAI**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Terhapus:\n"
            "• Record Data raw + varian turunan + regex + antrean klaim\n"
            f"• Bayes vocab (spam:{stats_before.get('bayes_spam', 0)} "
            f"ham:{stats_before.get('bayes_ham', 0)} "
            f"vocab:{stats_before.get('vocab_size', 0)} kata)\n"
            f"• Pattern Memory (pola spam:{stats_before.get('pattern_spam', 0)} "
            f"nonspam:{stats_before.get('pattern_nonspam', 0)})\n"
            f"• Adaptive threshold (kembali ke default)\n\n"
            "Model sudah di-seed ulang dari nol. AI Manual mulai belajar "
            "dari kondisi kosong."
        )

    elif data == "nx_menu_reset":
        if user_id != OWNER_ID:
            try:
                await cq.answer("🔒 Fitur ini aktif & berfungsi normal.\nHanya Owner bot yang bisa mengaksesnya.", show_alert=True)
            except Exception:
                pass
            return
        try:
            await cq.answer()
        except Exception:
            pass
        await _safe_edit(
            cq.message,
            "⚠️ **MAINFRAME PURGE MEMORY ZONE**\n━━━━━━━━━━━━━━━━━━━━━━━━━━\nPilih partisi memori yang akan dihancurkan:",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🗑️ PURGE KALIMAT + AI REGEX", callback_data="nx_c_kalimat")],
                [InlineKeyboardButton("🧹 FLUSH AI REGEX SAJA",      callback_data="nx_c_regex")],
                [InlineKeyboardButton("🔙 URUNGKAN PLAN",            callback_data="nx_home")],
            ]),
        )

    elif data == "nx_c_kalimat":
        if user_id != OWNER_ID:
            try:
                await cq.answer("🔒 Fitur ini aktif & berfungsi normal.\nHanya Owner bot yang bisa mengaksesnya.", show_alert=True)
            except Exception:
                pass
            return
        try:
            await cq.answer()
        except Exception:
            pass
        await nexus_clear_kalimat()
        me   = client.me
        text = await _welcome_text()
        await _safe_edit_html(cq.message, text, _main_markup(me.username))

    elif data == "nx_c_regex":
        if user_id != OWNER_ID:
            try:
                await cq.answer("🔒 Fitur ini aktif & berfungsi normal.\nHanya Owner bot yang bisa mengaksesnya.", show_alert=True)
            except Exception:
                pass
            return
        try:
            await cq.answer()
        except Exception:
            pass
        await nexus_clear_regex()
        me   = client.me
        text = await _welcome_text()
        await _safe_edit_html(cq.message, text, _main_markup(me.username))

    elif data.startswith("nx_actlog_page_"):
        if user_id != OWNER_ID:
            try:
                await cq.answer("🔒 Hanya Owner!", show_alert=True)
            except Exception:
                pass
            return
        try:
            await cq.answer()
        except Exception:
            pass
        page  = int(data.split("_")[-1])
        limit = 5
        rows, total = await nexus_actlog_get_page(page, limit)
        total_pages = max(1, (total + limit - 1) // limit)

        if not rows:
            await _safe_edit(
                cq.message,
                "📋 **LOG REGEX-AUTO & CATEGORY DETECTOR**\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "_Belum ada aktivitas yang tercatat di layer ini._\n\n"
                "Log akan muncul setelah pola auto-regex/CategoryDetector "
                "menangkap spam pertama (deteksi AI Manual ada di panel "
                "**Debug AI**).",
                InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Kembali", callback_data="nx_owner_menu"),
                ]]),
            )
            return

        IKON = {"HAPUS": "🗑️", "WHITELIST": "🛡️", "KEROYOKAN": "☠️"}

        text = (
            f"📋 **LOG AKTIVITAS — NEXUS REGEX-AUTO & CATEGORY DETECTOR**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"_Hanya layer backstop ini (pola auto-regex hasil rekalkulasi\n"
            f"tengah malam + CategoryDetector). Deteksi AI Manual (Gate E) ada\n"
            f"di panel **Debug AI**, dan SEMUA jenis deteksi (termasuk regex\n"
            f"owner) ada lengkap di panel log per grup & LOG_CHANNEL._\n\n"
            f"Total: **{total}** entri   |   Hal. **{page}/{total_pages}**\n\n"
        )
        for entry in rows:
            aksi   = entry.get("aksi", "?")
            ikon   = IKON.get(aksi, "•")
            ts     = entry.get("ts")
            if hasattr(ts, "strftime"):
                waktu = ts.strftime("%d/%m %H:%M")
            else:
                waktu = str(ts)[:16] if ts else "?"
            uname  = entry.get("user_name", "?")[:20]
            ctitle = entry.get("chat_title", "?")[:22]
            alasan = entry.get("alasan", "")[:60]
            conf   = entry.get("confidence", 0.0)
            konten = entry.get("content", "")[:60]

            conf_str = f"  AI: **{conf*100:.0f}%**" if conf > 0 else ""
            text += (
                f"{ikon} **{aksi}** — `{waktu}`{conf_str}\n"
                f"👤 {uname}  📌 {ctitle}\n"
                f"🔑 _{alasan}_\n"
                f"💬 `{konten}`\n"
                f"─────────────────────\n"
            )

        nav = []
        if page > 1:
            nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"nx_actlog_page_{page-1}"))
        if page < total_pages:
            nav.append(InlineKeyboardButton("▶️ Next", callback_data=f"nx_actlog_page_{page+1}"))

        rows_kb = []
        if nav:
            rows_kb.append(nav)
        rows_kb.append([InlineKeyboardButton("🧹 Hapus Semua Log", callback_data="nx_actlog_clear_confirm")])
        rows_kb.append([InlineKeyboardButton("🔙 Kembali", callback_data="nx_owner_menu")])
        await _safe_edit(cq.message, text[:4000], InlineKeyboardMarkup(rows_kb))

    elif data == "nx_actlog_clear_confirm":
        if user_id != OWNER_ID:
            try:
                await cq.answer("🔒 Hanya Owner!", show_alert=True)
            except Exception:
                pass
            return
        try:
            await cq.answer()
        except Exception:
            pass
        await _safe_edit(
            cq.message,
            "⚠️ **HAPUS SEMUA LOG AKTIVITAS?**\n\nSeluruh riwayat tindakan bot akan dihapus permanen.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Ya, Hapus", callback_data="nx_actlog_clear_exec")],
                [InlineKeyboardButton("🚫 Batal",     callback_data="nx_actlog_page_1")],
            ]),
        )

    elif data == "nx_actlog_clear_exec":
        if user_id != OWNER_ID:
            try:
                await cq.answer("🔒 Hanya Owner!", show_alert=True)
            except Exception:
                pass
            return
        n = await nexus_actlog_clear()
        try:
            await cq.answer(f"🧹 {n} entri log dihapus.", show_alert=True)
        except Exception:
            pass
        await _safe_edit(
            cq.message,
            "📋 **LOG AKTIVITAS NEXUS AI**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "_Log telah dibersihkan._",
            InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Kembali", callback_data="nx_owner_menu"),
            ]]),
        )

    elif data.startswith("nx_ai_debug_page_"):
        if user_id != OWNER_ID:
            try:
                await cq.answer("🔒 Hanya Owner!", show_alert=True)
            except Exception:
                pass
            return
        try:
            page = int(data.split("_")[-1])
            if page < 1:
                page = 1
        except (ValueError, IndexError):
            page = 1

        from database import ai_debug_log_get_page as _get_ai_log
        docs, total = await _get_ai_log(page, per_page=5)
        total_pages = max(1, (total + 4) // 5)

        if not docs and page == 1:
            await _safe_edit(
                cq.message,
                "🔬 **DEBUG AI — LOG 24 JAM**\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "_Belum ada aktivitas AI dalam 24 jam terakhir._\n\n"
                "_Log muncul saat AI mendeteksi spam, belajar dari laporan /spam, "
                "atau setelah cron midnight berjalan._",
                InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Kembali", callback_data="nx_owner_menu"),
                ]]),
            )
            return

        text = (
            "🔬 **DEBUG AI — LOG 24 JAM TERAKHIR**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📄 Hal **{page}/{total_pages}** | Total: **{total}** aksi\n\n"
        )
        for entry in docs:
            aksi       = entry.get("aksi", "?")
            confidence = entry.get("confidence", 0.0)
            ringkasan  = entry.get("ringkasan", "")[:100]
            ts_raw     = entry.get("ts", 0)
            try:
                from datetime import datetime as _dt, timezone as _tz_mod
                dt    = _dt.fromtimestamp(ts_raw, tz=_tz_mod.utc).astimezone(TZ_JAKARTA)
                waktu = dt.strftime("%d/%m %H:%M WIB")
            except Exception:
                waktu = str(ts_raw)

            pct      = f"{confidence * 100:.0f}%"
            conf_str = f" | `{pct}`" if confidence > 0 else ""
            text += f"**{aksi}**{conf_str}\n"
            text += f"🕐 `{waktu}`\n"
            if ringkasan:
                text += f"_{ringkasan}_\n"
            text += "─────────────────────\n"

        nav = []
        if page > 1:
            nav.append(InlineKeyboardButton("◀️", callback_data=f"nx_ai_debug_page_{page-1}"))
        nav.append(InlineKeyboardButton("🔄 Refresh", callback_data="nx_ai_debug_page_1"))
        if page < total_pages:
            nav.append(InlineKeyboardButton("▶️", callback_data=f"nx_ai_debug_page_{page+1}"))

        rows_kb = []
        if nav:
            rows_kb.append(nav)
        rows_kb.append([InlineKeyboardButton("🔙 Kembali", callback_data="nx_owner_menu")])
        await _safe_edit(cq.message, text[:4000], InlineKeyboardMarkup(rows_kb))

    else:

        try:
            await cq.answer()
        except Exception:
            pass
