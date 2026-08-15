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


def _get_chat():
    global _client, _chat
    if _chat is None:
        if _client is None:
            _client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
        _chat = _client.chats.create(
            model=GEMINI_MODEL,
            config=types.GenerateContentConfig(system_instruction=SYSTEM_INSTRUCTION),
        )
    return _chat


def ask(message: str) -> str:
    chat = _get_chat()
    response = chat.send_message(message)
    return response.text


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
