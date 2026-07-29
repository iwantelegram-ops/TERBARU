"""
plugins/commands/pelanggaranku.py
──────────────────────────────────
Fitur /pelanggaranku — user cek riwayat pelanggaran dirinya sendiri.

Diakses lewat 3 jalur:
  1. Teks link "cek selengkapnya" di notif grup (deeplink start=pelanggaranku)
     → diteruskan dari antigcast_group.py ke handle_dm_pelanggaranku()
  2. /pelanggaranku langsung di DM bot
  3. /pelanggaranku di grup → bot konfirmasi + kirim DM

Menampilkan maks 5 pelanggaran terakhir user lintas semua grup (7 hari).
Setiap pelanggaran disertai penjelasan detail agar user paham apa yang terjadi.
Jika bersih → info positif.
"""

import asyncio
import html
from datetime import datetime
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ParseMode
from pyrogram.errors import UserIsBlocked, PeerIdInvalid, InputUserDeactivated

from database import get_user_violations_lintas_grup, TZ_WIB


# ── Penjelasan detail tiap jenis pelanggaran ──────────────────────────────────
# Teks ini ditampilkan di DM agar user benar-benar paham apa yang dilanggar.
_VIOLATION_EXPLAIN: dict[str, str] = {
    "DUPLIKAT_LOKAL": (
        "Kamu mengirim <b>pesan yang sama atau sangat mirip berkali-kali</b> di grup yang sama. "
        "Bot mendeteksi teks duplikat dari akunmu dan menghapusnya otomatis."
    ),
    "FLOOD_DUPLIKAT_RAM": (
        "Kamu mengirim pesan duplikat secara <b>sangat cepat (flood)</b>. "
        "Sistem deteksi RAM menangkapnya seketika — lebih sensitif dari filter biasa."
    ),
    "GCAST_GLOBAL": (
        "Pesan yang <b>sama persis</b> dari akunmu terdeteksi dikirim ke <b>lebih dari satu grup</b> "
        "yang dikelola bot ini sekaligus. Ini pola klasik akun penyebar spam/promosi massal."
    ),
    "BIO_LINK": (
        "Profil Telegram kamu mengandung <b>link yang mengarah ke grup Telegram lain</b>. "
        "Bot mendeteksinya sebagai potensi akun promosi saat kamu aktif di VC atau berkirim pesan."
    ),
    "MENTION_NON_MEMBER": (
        "Kamu menyebut (<b>@mention</b>) akun yang <b>bukan anggota grup</b> ini. "
        "Ini pola umum spammer untuk mengundang atau mempromosikan user dari luar ke dalam grup."
    ),
    "MENTION_BIO_GRUP": (
        "Kamu menyebut (<b>@mention</b>) akun yang bio profilnya berisi <b>promosi atau link grup lain</b>. "
        "Meski akunnya sudah anggota, mention ini dinilai menyebarkan promosi secara terselubung."
    ),
    "LINK_PESAN": (
        "Pesanmu mengandung <b>link/URL</b> yang masuk daftar blokir grup ini. "
        "Admin grup mengaktifkan filter link untuk mencegah penyebaran link mencurigakan."
    ),
    "REGEX_GLOBAL": (
        "Pesanmu mengandung <b>kata atau pola yang masuk daftar blokir global</b> "
        "(berlaku di semua grup yang dikelola bot ini, diatur oleh owner bot)."
    ),
    "REGEX_GRUP": (
        "Pesanmu mengandung <b>kata atau pola yang masuk daftar blokir lokal</b> grup ini. "
        "Daftar blokir ini diatur oleh admin grup. Tanyakan ke admin kata apa yang diblokir."
    ),
    "NEXUS_AI": (
        "Pesanmu dinilai mengandung <b>konten spam oleh sistem AI Nexus</b>. "
        "AI menganalisis pola, konteks, dan gaya penulisan — bukan sekadar kata tertentu. "
        "Kalau kamu merasa ini salah deteksi, hubungi admin grup."
    ),
    "AI_MANUAL_AUTO": (
        "Pesanmu dinilai <b>spam oleh AI Manual</b> (sistem AI yang dilatih dari pola filter owner). "
        "AI ini mengecek pesan lebih dulu sebelum filter kata biasa — kalau kamu merasa ini salah "
        "deteksi, hubungi admin grup."
    ),
    "CAS_BAN": (
        "Akunmu terdaftar di <b>database global CAS (Combot Anti-Spam)</b> sebagai spammer terverifikasi. "
        "CAS adalah database eksternal berisi 200.000+ akun spam dari seluruh Telegram. "
        "Untuk mengajukan banding: <a href=\"https://cas.chat\">cas.chat</a>"
    ),
    "MUTE_ESKALASI": (
        "Kamu mencapai batas <b>pelanggaran berturut-turut</b> di grup ini → di-mute otomatis. "
        "Durasi mute berlipat tiap kali terulang: 5 mnt → 10 mnt → 20 mnt → dst. "
        "Pesan bersih berturut-turut akan mereset hitunganmu."
    ),
    "BAN_ESKALASI": (
        "Kamu mencapai batas <b>pelanggaran berturut-turut</b> → di-ban permanen dari grup itu. "
        "Admin grup mengaktifkan Mode Hukuman: Ban (bukan mute) untuk grup tersebut."
    ),
    "MUTE_GAGAL": (
        "Bot mencoba mute akunmu tapi <b>gagal dieksekusi</b> — kemungkinan izin admin bot "
        "di grup itu tidak mencukupi saat kejadian. <i>Kamu tidak benar-benar di-mute.</i>"
    ),
    "BAN_GAGAL": (
        "Bot mencoba ban akunmu tapi <b>gagal dieksekusi</b> — kemungkinan izin admin bot "
        "di grup itu tidak mencukupi saat kejadian. <i>Kamu tidak benar-benar di-ban.</i>"
    ),
    "WHITELIST_SPARED": (
        "Pesanmu cocok dengan pola filter, <b>TAPI tidak dihapus</b> karena akunmu ada di "
        "whitelist grup ini. Ini log informatif saja — tidak ada tindakan yang diambil. ✅"
    ),
    "MUTE_SENYAP": (
        "Pesanmu <b>dihapus diam-diam</b> karena kamu sedang dalam <b>masa mute aktif</b> di grup ini. "
        "Pesan tidak mendapat notifikasi tambahan — langsung hilang saja."
    ),
    "BIO_ADMIN_WAJIB": (
        "Status admin kamu di grup dicabut otomatis karena <b>bio profil Telegram kamu tidak "
        "memenuhi teks wajib</b> yang ditetapkan admin utama grup tersebut. "
        "Pasang teks yang diminta di bio profil kamu untuk bisa diangkat admin lagi."
    ),
    "STICKER_BLACKLIST": (
        "Kamu mengirim stiker dari <b>pack yang masuk daftar blokir global</b> bot. "
        "Pack ini dilaporkan oleh user/admin dan sudah diblokir di semua grup yang pakai bot ini."
    ),
    "VC_MUTE_NON_MEMBER": (
        "Mic kamu di-mute di <b>obrolan suara (VC)</b> karena kamu <b>bukan anggota resmi</b> "
        "grup ini. Gabung sebagai anggota grup terlebih dahulu untuk bisa berbicara di VC."
    ),
    "VC_MUTE_PEER": (
        "Mic kamu di-mute di VC karena <b>profil kamu belum bisa diverifikasi</b> oleh sistem "
        "Security OS. Biasanya terjadi saat bio sedang diperbarui atau ada delay sisi Telegram."
    ),
    "VC_MUTE_BIO_LINK": (
        "Mic kamu di-mute di VC karena <b>bio profil kamu mengandung link yang mengarah ke "
        "grup Telegram lain</b>. Hapus link tersebut dari bio kamu — mic akan dibuka otomatis."
    ),
    "VC_MUTE_ONKEM": (
        "Mic kamu di-mute di VC karena <b>kamu menyalakan kamera (onkem)</b>. "
        "Fitur Inspeksi Onkem aktif di grup ini — kamera dilarang selama sesi VC berlangsung."
    ),
    "VC_UNMUTE": (
        "Mic kamu <b>dibuka kembali</b> di VC karena bio profil kamu sudah bersih "
        "dari link yang bermasalah. ✅"
    ),
}

_FALLBACK_EXPLAIN = (
    "Pelanggaran ini tercatat oleh sistem bot. "
    "Hubungi admin grup jika kamu memerlukan penjelasan lebih lanjut."
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fmt_ts(ts: float) -> str:
    try:
        dt = datetime.fromtimestamp(ts, tz=TZ_WIB)
        return dt.strftime("%d %b %Y, %H:%M WIB")
    except Exception:
        return "—"


def _aksi_label(aksi: str) -> str:
    return {
        "HAPUS":         "Pesan Dihapus",
        "MUTE":          "User Di-Mute",
        "BAN":           "User Di-Ban",
        "UNADMIN":       "Admin Dicopot",
        "KICK-VC":       "Dikeluarkan dari VC",
        "SECOS":         "Security OS",
        "MUTE-VC-MIC":   "Mic Di-Mute (VC)",
        "UNMUTE-VC-MIC": "Mic Dibuka (VC)",
    }.get(aksi, aksi)


# ── Builder teks DM ───────────────────────────────────────────────────────────

async def build_pelanggaranku_text(user_id: int, user_name: str) -> str:
    """
    Bangun teks lengkap riwayat pelanggaran user untuk dikirim via DM.
    Sudah include penjelasan detail tiap jenis pelanggaran.
    """
    from core.violation_types import get_violation_meta

    docs = await get_user_violations_lintas_grup(user_id, limit=5)

    header = (
        f"📋 <b>RIWAYAT PELANGGARANMU</b>\n"
        f"👤 {html.escape(user_name)}  ·  <code>{user_id}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Data 7 hari terakhir — maks 5 terbaru</i>\n\n"
    )

    if not docs:
        return (
            header
            + "✅ <b>Kamu tidak punya catatan pelanggaran apapun.</b>\n\n"
            + "<i>Akunmu bersih di semua grup yang dikelola bot ini. "
            + "Pertahankan terus ya! 👍</i>"
        )

    entries = []
    for i, d in enumerate(docs, 1):
        jenis   = d.get("jenis")
        aksi    = d.get("aksi", "?")
        alasan  = (d.get("alasan") or "—").strip()
        konten  = (d.get("konten") or "").strip()
        ts_str  = _fmt_ts(d.get("ts", 0))
        cid     = d.get("chat_id", "?")

        icon, label, _ = get_violation_meta(jenis)
        penjelasan     = _VIOLATION_EXPLAIN.get(jenis or "", _FALLBACK_EXPLAIN)

        konten_line = ""
        if konten:
            display   = html.escape(konten[:150])
            ellipsis  = "…" if len(konten) > 150 else ""
            konten_line = f"\n📨 <b>Isi pesan:</b>\n<code>{display}{ellipsis}</code>"

        entry = (
            f"<b>{i}. {icon} {label}</b>\n"
            f"🏛 Grup: <code>{cid}</code>\n"
            f"⏱ {ts_str}\n"
            f"⚖️ Tindakan: <b>{_aksi_label(aksi)}</b>\n"
            f"📌 Detail: <i>{html.escape(alasan)}</i>"
            f"{konten_line}\n\n"
            f"ℹ️ {penjelasan}"
        )
        entries.append(entry)

    separator = "\n\n" + "─" * 22 + "\n\n"
    body      = separator.join(entries)

    footer = (
        "\n\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<i>Ada yang dirasa tidak sesuai? Hubungi admin grup yang bersangkutan.</i>"
    )

    return header + body + footer


# ── Kirim DM (reusable) ───────────────────────────────────────────────────────

async def send_pelanggaranku_dm(client, user_id: int, user_name: str) -> bool:
    """
    Kirim DM riwayat pelanggaran ke user.
    Return True jika berhasil, False jika user belum start bot / diblokir.
    """
    try:
        text = await build_pelanggaranku_text(user_id, user_name)
        await client.send_message(
            chat_id=user_id,
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        return True
    except (UserIsBlocked, PeerIdInvalid, InputUserDeactivated):
        return False
    except Exception as e:
        print(f"[pelanggaranku] gagal kirim DM ke {user_id}: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
#  Handler deeplink DM — dipanggil dari antigcast_group.py
#  Terpicu saat user klik "cek selengkapnya" di notif grup
#  (deeplink: t.me/bot?start=pelanggaranku)
# ─────────────────────────────────────────────────────────────────────────────

async def handle_dm_pelanggaranku(client: Client, message: Message):
    """Dipanggil dari antigcast_dm_handler saat parameter start=pelanggaranku."""
    user = message.from_user
    if not user:
        return
    text = await build_pelanggaranku_text(user.id, user.first_name or str(user.id))
    await message.reply(
        text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
#  /pelanggaranku — DM langsung
# ─────────────────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("pelanggaranku") & filters.private)
async def cmd_pelanggaranku_dm(client: Client, message: Message):
    user = message.from_user
    if not user:
        return
    text = await build_pelanggaranku_text(user.id, user.first_name or str(user.id))
    await message.reply(
        text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
#  /pelanggaranku — di GRUP
# ─────────────────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("pelanggaranku") & (filters.group | filters.forum))
async def cmd_pelanggaranku_grup(client: Client, message: Message):
    user = message.from_user
    if not user:
        return

    uid   = user.id
    cid   = message.chat.id
    uname = user.first_name or str(uid)

    try:
        await message.delete()
    except Exception:
        pass

    success = await send_pelanggaranku_dm(client, uid, uname)

    if success:
        notif = await client.send_message(
            cid,
            f"📬 {user.mention}, riwayat pelanggaranmu sudah dikirim ke DM kamu!",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    else:
        me        = client.me
        start_url = f"https://t.me/{me.username}?start=pelanggaranku"
        notif = await client.send_message(
            cid,
            f"⚠️ {user.mention}, bot tidak bisa mengirim DM ke kamu.\n"
            f'<a href="{start_url}">Klik di sini</a> untuk start bot dulu, lalu coba lagi.',
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )

    async def _del():
        await asyncio.sleep(10)
        try:
            await notif.delete()
        except Exception:
            pass

    asyncio.create_task(_del())
