"""
Tab "LLM Chat": ngobrol bebas (free text) ke Gemini, TERPISAH TOTAL dari
Flink -- tidak menyentuh `t_env` atau `flink_runner.LOCK` sama sekali,
jadi tab ini gak akan pernah ke-block walau SQL/Python tab lagi nyangkut
(atau sebaliknya).

Multi-turn: pakai fitur chat session dari SDK (`client.chats.create()`),
yang otomatis nyimpen history percakapan dan ngirim ulang semuanya tiap
kali kirim pesan baru. History-nya sendiri cuma hidup di memory proses
ini (`_chat`) -- hilang kalau server di-restart, sama seperti job history
Flink di `flink_runner.py`.
"""

import os

from dotenv import load_dotenv
from google import genai

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(PROJECT_DIR, ".env"))

GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-flash-latest")

_client: genai.Client | None = None
_chat = None


def _get_chat():
    global _client, _chat
    if _chat is None:
        if _client is None:
            _client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
        _chat = _client.chats.create(model=GEMINI_MODEL)
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
