"""
Tab "LLM Chat": ngobrol bebas (free text) ke Gemini, TERPISAH TOTAL dari
Flink -- tidak menyentuh `t_env` atau session lock manapun sama sekali,
jadi tab ini gak akan pernah ke-block walau SQL/Python tab lagi nyangkut
(atau sebaliknya).

Multi-turn: pakai fitur chat session dari SDK (`client.chats.create()`),
yang otomatis nyimpen history percakapan dan ngirim ulang semuanya tiap
kali kirim pesan baru. History-nya sendiri cuma hidup di memory proses
ini (`_chat`) -- hilang kalau server di-restart, sama seperti job history
Flink di `flink_runner.py`.

SYSTEM_INSTRUCTION di bawah minta Gemini membungkus kode yang MEMANG
dimaksudkan buat langsung dijalankan di notebook ini pakai code fence
```sql / ```python -- static/index.html mendeteksi tag itu buat nampilkan
tombol "Kirim ke SQL/Python Notebook" di bawah blok kodenya. Ini cuma
petunjuk ke model (best-effort, bukan jaminan): kalau modelnya salah kasih
tag, tombolnya cuma gak muncul -- tidak ada risiko keamanan tambahan,
karena kode dari chat tetap cuma DIMASUKKAN sebagai cell baru (tidak
langsung dieksekusi) begitu user klik tombolnya, dan user masih harus
klik Run sendiri di cell itu seperti biasa.
"""

import json
import os

from dotenv import load_dotenv
from google import genai
from google.genai import types

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(PROJECT_DIR, ".env"))

GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-flash-latest")

SYSTEM_INSTRUCTION = """\
Kamu bantu user belajar PyFlink (Apache Flink Python Table API) lewat notebook
web ini. Kalau kasih kode yang MEMANG dimaksudkan buat langsung dijalankan
user di notebook ini, bungkus pakai code fence dengan tag bahasa yang tepat:
- ```sql untuk statement Flink SQL (CREATE TABLE ... WITH (...), SELECT,
  INSERT INTO ... SELECT, dst) buat tab "SQL Notebook".
- ```python untuk kode Python buat tab "Python Notebook", dieksekusi lewat
  exec() dengan variabel `t_env` (TableEnvironment) yang sudah tersedia --
  HANYA print() yang tampil sebagai output, tidak ada auto-display ekspresi
  terakhir.

Kalau kode itu cuma ilustrasi/penjelasan (bukan buat langsung dijalankan di
notebook ini), JANGAN pakai tag 'sql'/'python' di code fence-nya, supaya
tidak muncul tombol "kirim ke notebook" yang salah.

Konteks penting soal notebook ini:
- TableEnvironment dibuat mode STREAMING (in_streaming_mode()), bukan batch --
  agregat seperti COUNT/SUM bisa muncul sebagai baris update/retraction, bukan
  cuma satu baris hasil akhir.
- Tiap session notebook punya TableEnvironment sendiri, terisolasi dari
  session lain.
- SELECT di tab SQL dibatasi preview 20 baris pertama saja.
- Kalau kasih contoh CREATE TABLE dengan connector 'kafka', selalu sertakan
  'scan.bounded.mode' supaya preview SELECT-nya berhenti sendiri (source
  Kafka defaultnya unbounded, bisa menggantung tanpa ini).
"""

_client: genai.Client | None = None
_chat = None


def _get_client() -> genai.Client:
    global _client
    if _client is None:
        _client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    return _client


def _get_chat():
    global _chat
    if _chat is None:
        _chat = _get_client().chats.create(
            model=GEMINI_MODEL,
            config=types.GenerateContentConfig(system_instruction=SYSTEM_INSTRUCTION),
        )
    return _chat


def ask(message: str) -> str:
    chat = _get_chat()
    response = chat.send_message(message)
    return response.text


SQL_AGENT_SYSTEM_INSTRUCTION = """\
Kamu bantu user menulis Flink SQL untuk session PyFlink notebook yang SEDANG
AKTIF. Kamu dikasih dua tools READ-ONLY untuk cek keadaan session itu sebelum
jawab -- PAKAI tools itu dulu (jangan menebak nama tabel/kolom):
- describe_table(name): panggil dengan name kosong ("") buat lihat daftar
  semua table/view di session ini, atau isi nama table buat lihat kolom +
  DDL aslinya.
- preview_rows(sql, limit): jalankan SELECT terbatas buat lihat CONTOH data
  asli (misal tipe/isi kolom yang ambigu dari nama doang).

Balas HANYA dengan satu statement SQL yang valid buat Flink Table API
(CREATE TABLE ... WITH (...), SELECT, atau INSERT INTO ... SELECT) --
tanpa penjelasan, tanpa code fence, tanpa teks lain. Kalau user minta hal
yang bukan soal SQL/tabel di session ini, balas dengan pesan error singkat
sebagai pengganti SQL (misal: "-- Ini bukan permintaan SQL.").

Tools ini cuma bisa MEMBACA -- kamu tidak pernah bisa CREATE/INSERT/DROP apa
pun lewat tools ini, jadi tidak akan pernah mengubah isi session ini sendiri.
Session ini mode STREAMING atau BATCH (lihat hasil describe_table kalau
perlu tahu), dan SELECT di notebook ini dibatasi preview 20 baris pertama.
"""


def generate_sql(prompt: str, session) -> str:
    """Agent ringan (bukan chat bebas): satu kali generate_content dengan
    function-calling, BUKAN _chat yang persist -- setiap panggilan berdiri
    sendiri, tidak menyimpan riwayat. Tools di bawah dibungkus di sini (bukan
    di level modul) supaya masing-masing "menempel" ke SATU session tertentu
    lewat closure, karena tiap request bisa datang dari session yang beda.

    Kenapa harus session.lock dipegang di tiap tool (bukan sekali di luar)?
    Karena SDK genai yang manggil tool-nya sendiri (automatic function
    calling) -- kita tidak pegang kendali kapan itu terjadi, jadi lock-nya
    harus ada di titik tool-nya sendiri yang benar-benar menyentuh t_env,
    bukan membungkus seluruh generate_content() (yang juga nunggu network
    call ke Gemini -- kalau itu ikut dikunci, job SQL/Python lain di session
    yang sama bisa ke-block lama nunggu Gemini balas)."""

    from api import flink_runner  # local import: hindari import siklus modul

    def describe_table(name: str = "") -> str:
        """Lihat daftar table/view di session ini (name kosong), atau kolom
        + DDL satu table/view tertentu (isi name-nya)."""
        with session.lock:
            if not name:
                tables = flink_runner.list_tables(session)
                return json.dumps([{"name": t["name"], "kind": t["kind"]} for t in tables])
            info = flink_runner.describe_table(session, name)
            if info is None:
                return f"Table/view '{name}' tidak ditemukan di session ini."
            return json.dumps(info)

    def preview_rows(sql: str, limit: int = 5) -> str:
        """Jalankan SELECT terbatas di session ini, balikin contoh baris
        sebagai JSON. Cuma boleh SELECT -- statement lain ditolak."""
        with session.lock:
            try:
                result = flink_runner.preview_select(session, sql, limit)
            except Exception as exc:
                return f"Error: {exc}"
            return json.dumps(result)

    client = _get_client()
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
        config=types.GenerateContentConfig(
            system_instruction=SQL_AGENT_SYSTEM_INSTRUCTION,
            tools=[describe_table, preview_rows],
            automatic_function_calling=types.AutomaticFunctionCallingConfig(
                maximum_remote_calls=3
            ),
        ),
    )
    return (response.text or "").strip()


def history() -> list[dict]:
    # Belum pernah ada chat sama sekali -> jangan sampai bikin client/chat
    # baru cuma buat ngintip history kosong (dan gak perlu API key valid
    # sebelum pesan pertama dikirim).
    if _chat is None:
        return []
    return [
        {"role": msg.role, "text": "".join(part.text or "" for part in msg.parts)}
        for msg in _chat.get_history()
    ]


def reset() -> None:
    global _chat
    _chat = None
