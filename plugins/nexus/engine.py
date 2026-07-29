"""
plugins/nexus/engine.py
────────────────────────
v8.0 — generate_regex_otomatis_async() & cron_midnight_scheduler() SUDAH
DIHAPUS. Fitur "auto-generate regex dari korpus tengah malam" ini datanya
selama ini diambil dari nexus_kalimat_db, yang sejak v8.0 sudah dialihkan
fungsinya jadi penyimpanan raw teks TAHAP 1 yang diperiksa Groq TAHAP 2
(Record Data — lihat database.py & core/spam_claim_queue.py), BUKAN lagi
korpus untuk regenerasi regex. Fitur ini juga sudah nonaktif duluan di
main.py sebelum dihapus (cron_midnight_scheduler tidak pernah di-start).

Semua pembentukan pola AI sekarang murni lewat jalur Groq-verified:
core/spam_claim_queue.py (TAHAP 1, klaim + dedupe) → core/groq_queue.py
(TAHAP 2, generate varian + train Bayes/PatternMemory langsung, tanpa
regex baru yang perlu di-generate ulang tiap malam).

File ini hanya re-export util teks yang masih dipakai modul lain, supaya
importer (nexus_group.py, nexus_handlers.py, core/antispam_queue.py,
plugins/commands/log.py) tidak perlu diubah:
  - pipeline_pembersihan          → core/regex_utils.py
  - generate_kandidat_mutasi_liar → core/regex_utils.py
"""

from core.regex_utils import pipeline_pembersihan, generate_kandidat_mutasi_liar

__all__ = ["pipeline_pembersihan", "generate_kandidat_mutasi_liar"]
