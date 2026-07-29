# Referensi `.env` — Promo Userbot "Auto Typing VC Grup" (Gemini)

Semua var di bawah OPSIONAL — kalau tidak diisi, pakai nilai default yang tertulis.

---

## 1. Gemini API (jalur AI khusus promo, terpisah dari Groq)

| Env | Default | Fungsi |
|---|---|---|
| `GEMINI_API_KEYS` | *(kosong)* | Banyak API key sekaligus, dipisah koma/spasi/baris baru. Dirotasi round-robin. |
| `GEMINI_API_KEY` | *(kosong)* | Alternatif kalau cuma 1 key. |
| `GEMINI_MODEL` | `gemini-flash-latest` | Nama model Gemini yang dipanggil. Pakai alias (`*-latest`) supaya tidak kena deprecation. |
| `GEMINI_MAX_CALLS_PER_MIN` | `12` | Limiter LOKAL (bukan limit asli Google) — maks panggilan/menit PER KEY. |
| `GEMINI_MAX_CALLS_PER_DAY` | `1200` | Limiter LOKAL — maks panggilan/hari PER KEY. |

> Groq (`GROQ_API_KEY(S)`) **tidak ada di tabel ini** — itu tetap 100% dipakai AI antispam, tidak disentuh sama sekali oleh fitur ini.

---

## 2. Ritme & Trigger (kapan sistem "melirik" & menembak)

| Env | Default | Fungsi |
|---|---|---|
| `PROMO_VC_CHAT_TICK_SECS` | `5` | Interval loop utama — seberapa sering sistem cek ulang eligibility, refresh deteksi typing, & pilih giliran akun berikutnya. **Bukan** jarak kirim. |
| `PROMO_VC_CHAT_MIN_ACCOUNT_GAP` | `86.4` | Jarak MINIMAL antar tembakan AI **baru** untuk akun yang SAMA (apapun grupnya). Set `0` untuk nonaktifkan. **Tidak berlaku** untuk cicilan teks panjang (lihat §4) — cicilan selalu lolos gap ini. |
| `PROMO_VC_CHAT_MIN_CLUSTER_INTERVAL` | `5.0` | Jeda tambahan HANYA saat giliran PINDAH ke akun lain (akun A → akun B). Tidak berlaku kalau gilirannya akun yang sama lagi. |

---

## 3. Jeda kirim antar-GRUP (dalam 1x giliran akun yang sama)

| Env | Default | Fungsi |
|---|---|---|
| `PROMO_VC_CHAT_SEND_STAGGER_MIN` | `2` | Batas bawah jeda acak (detik) sebelum kirim ke grup berikutnya dalam giliran yang sama. |
| `PROMO_VC_CHAT_SEND_STAGGER_MAX` | `6` | Batas atas jeda acak tsb. |

Contoh: akun A harus kirim ke 5 grup dalam 1 giliran → kirim grup 1 → tunggu acak 2-6 detik → grup 2 → tunggu lagi → dst. **Ini tidak berlaku untuk potongan cicilan di grup yang sama** (lihat §4).

---

## 4. Cicilan teks panjang (1 grup, 1 akun)

**Tidak ada env var khusus** untuk jarak antar potongan cicilan. Jaraknya mengikuti kapan akun itu dapat giliran lagi (`PROMO_VC_CHAT_TICK_SECS`), karena akun dengan cicilan tersisa **selalu lolos** dari `PROMO_VC_CHAT_MIN_ACCOUNT_GAP` — supaya teks panjang cepat kelar dikirim, tidak nge-gantung lama di tengah kalimat.

| Env | Default | Fungsi |
|---|---|---|
| `PROMO_VC_CHAT_CHUNK_CHARS` | `30` | Panjang maksimal (karakter) per potongan cicilan. |
| `PROMO_VC_CHAT_MAX_SEND_CHARS` | `60` | Batas karakter maksimal 1x kirim ke kolom chat VC (di luar cicilan). |

---

## 5. Konten & format pesan

| Env | Default | Fungsi |
|---|---|---|
| `PROMO_VC_CHAT_CONTEXT_N` | `5` | Berapa banyak pesan riwayat VC yang dikirim sebagai konteks ke Gemini. |
| `PROMO_VC_CHAT_MAX_CHARS_PER_TEXT` | `25` | Batas karakter tiap potongan konteks yang dikirim ke Gemini. |
| `PROMO_VC_CHAT_STRIP_NON_BMP` | `0` (off) | `1`/`true` untuk buang emoji/karakter non-BMP dari balasan AI. |

---

## 6. Keandalan & keamanan pengiriman

| Env | Default | Fungsi |
|---|---|---|
| `PROMO_VC_CHAT_FAIL_THRESHOLD` | `2` | Berapa kali gagal kirim beruntun sebelum grup di-pause sementara. |
| `PROMO_VC_CHAT_PAUSE_COOLDOWN_SECS` | `1800` (30 menit) | Berapa lama grup di-pause setelah kena threshold gagal di atas. |
| `PROMO_VC_CHAT_SETTLE_AFTER_JOIN_SECS` | `45` | Jeda "adaptasi" setelah akun baru join VC, sebelum boleh kirim pesan pertama. |
| `PROMO_VC_CHAT_RETRY_SETTLE_BUFFER_SECS` | `15` | Buffer tambahan settle time saat retry join. |

---

## 7. Master switch

| Env | Default | Fungsi |
|---|---|---|
| `PROMO_VC_CHAT_ENABLED` | `0` (off) | Set `1`/`true` untuk mengaktifkan seluruh fitur auto-typing VC promo userbot. |

---

## 8. List Grup Promo Userbot (join & sinkronisasi, beda modul: `promo_userbot.py`)

| Env | Default | Fungsi |
|---|---|---|
| `PROMO_UB_GROUP_SYNC_INTERVAL` | `1800` (30 menit) | Interval auto-sync daftar grup dari dialog Telegram. |
| `PROMO_UB_GROUP_SYNC_MIN_GAP` | `15` | Jeda minimal antar klik "🔄 Refresh List Grup" MANUAL. |
| `PROMO_UB_ROUND_INTERVAL` | `600` (10 menit) | Interval siklus join-grup baru. |
| `PROMO_UB_JOIN_MIN` / `PROMO_UB_JOIN_MAX` | `8` / `20` | Rentang jeda acak antar join grup baru. |

---

### Cara pakai
Tambahkan baris manapun yang mau diubah ke `.env` (Railway → Variables), contoh:
```
GEMINI_MODEL=gemini-flash-latest
PROMO_VC_CHAT_MIN_ACCOUNT_GAP=20
PROMO_VC_CHAT_SEND_STAGGER_MIN=2
PROMO_VC_CHAT_SEND_STAGGER_MAX=6
```
Yang tidak ditulis otomatis pakai nilai default di tabel atas. Restart service setelah ubah.
