"""
plugins/filters/ubot_detect_filter.py
─────────────────────────────────────────────────────────────────────────────
Filter DETEKSI UBOT (pengganti "Agresif Spam" lama).

PINDAH ARSITEKTUR: Handler on_message terpisah (dulu group=0, dieksekusi
PALING PERTAMA sebelum semua filter lain, DI LUAR sistem antrian) SUDAH
DIHAPUS dari file ini. Logikanya (record_sentence selalu jalan + evaluasi
& eksekusi hapus) sekarang jadi Gate F — ikut race PARALEL bersama gate
regex/link/duplikat-lokal/gcast di dalam lorong per-grup.

Lihat implementasinya di: core/antispam_queue.py :: _gate_ubot()

Kenapa dipindah:
  - Dulu handler ini SENDIRI mengecek is_admin() + VIP (free_col) di depan,
    terpisah dan redundan dari pengecekan yang sama di
    plugins/filters/antispam.py — sekarang cukup 1x lewat Fase 0
    (_resolve_bypass di core/antispam_queue.py), dipakai bersama semua gate.
  - Dulu handler ini jalan SEBELUM pesan sempat masuk ke sistem antrian
    per-grup sama sekali — jadi tidak ikut menikmati isolasi & paralelisme
    yang sudah dibangun di lorong. Sekarang jadi gate biasa yang race
    bersama gate lain via Event "found" yang sama.

record_sentence() TETAP selalu jalan tanpa syarat (independen toggle
ubot_detect maupun status found gate lain) — perilaku ini dipertahankan
persis sama di _gate_ubot(), cuma pindah lokasi eksekusi.

MEMORI TERPISAH: Fitur ini tetap memakai collection Mongo sendiri
  (ubot_sentence_tracker, lihat core/ubot_detect.py) — TERPISAH TOTAL dari
  collection seen_messages yang dipakai "Anti Duplikasi Lokal".

Fungsi _log_ubot_deletion() di bawah ini TETAP di sini (dipakai lewat
import oleh _gate_ubot) supaya format log panel-grup/LOG_CHANNEL tidak
perlu diduplikasi di core/antispam_queue.py.
"""

import html as _html

from database import insert_group_action_log
from core.violation_types import VIOLATION_NEXUS_AI, format_violation_header

_VIOLATION_UBOT = VIOLATION_NEXUS_AI


async def _log_ubot_deletion(client, message, raw_text: str) -> None:
    """Log aksi hapus ubot detect ke panel grup dan LOG_CHANNEL."""
    import os
    from plugins.commands.log import _send_log, _fmt_waktu, _user_line

    uid  = message.from_user.id
    cid  = message.chat.id
    name = message.from_user.first_name or str(uid)

    # Panel per-grup
    try:
        await insert_group_action_log(
            cid, "HAPUS",
            "Deteksi Ubot — kalimat berulang tanpa variasi",
            uid, name,
            raw_text[:100],
            jenis=_VIOLATION_UBOT,
        )
    except Exception:
        pass

    # LOG_CHANNEL
    LOG_CHANNEL = int(os.environ.get("LOG_CHANNEL", 0))
    if not LOG_CHANNEL:
        return

    user_mention = _user_line(uid, name)
    log_text = (
        f"<b>❖ {format_violation_header(_VIOLATION_UBOT)} ❖</b>\n"
        f"◈ <b>User:</b> {user_mention}\n"
        f"◈ <b>Grup:</b> {_html.escape(message.chat.title or str(cid))} (<code>{cid}</code>)\n"
        f"◈ <b>Waktu:</b> {_fmt_waktu()}\n"
        f"◈ <b>Keterangan:</b> Terdeteksi sebagai ubot — mengirim kalimat yang sama berulang tanpa variasi\n\n"
        f"📨 <b>Pesan terakhir:</b>\n<code>{_html.escape(raw_text[:300])}</code>"
    )
    await _send_log(client, log_text)
