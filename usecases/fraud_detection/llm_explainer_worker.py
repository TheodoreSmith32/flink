"""
Worker Python TERPISAH dari fraud_job.py -- bukan bagian dari Flink job,
sengaja plain sleep-loop, BUKAN agent (tanpa tools, satu prompt per baris).
Alasannya: fraud_job.py cuma boleh nge-flag transaksi secepat mungkin, gak
boleh nunggu network call ke Gemini di jalur streaming-nya -- makanya
penjelasan LLM dikerjakan di sini, di luar Flink sama sekali.

Alur tiap iterasi:
    1. Ambil baris fraud_alerts yang explanation IS NULL.
    2. Panggil Gemini sekali (plain completion, tanpa tools) buat bikin
       penjelasan singkat berbasis kolom-kolom yang sudah di-flag Flink.
    3. UPDATE fraud_alerts SET explanation, explained_at.
    4. INSERT governance_log stage='llm_explained' -- baris audit kedua,
       melengkapi 'flink_flagged' yang sudah ditulis fraud_job.py sendiri.

Butuh `psycopg2-binary` (ditambahkan ke requirements.txt) -- beda dari
fraud_job.py yang nulis ke Postgres lewat JDBC connector Flink, worker ini
baca+tulis langsung lewat psycopg2 karena bukan bagian dari job Flink.

SEBELUM JALANKAN: isi GEMINI_API_KEY di .env (lihat template.env). Tanpa itu
worker ini akan error saat mencoba memanggil Gemini -- fraud_job.py sendiri
TETAP jalan normal tanpa GEMINI_API_KEY, cuma kolom explanation-nya kosong.

Jalankan (setelah fraud_job.py sudah mulai nge-flag baris):
    source .venv/bin/activate
    python usecases/fraud_detection/llm_explainer_worker.py
"""

import os
import time

import psycopg2
from dotenv import load_dotenv
from google import genai

base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(base_dir, ".env"))

postgres_host = os.environ.get("POSTGRES_HOST", "localhost").strip()
postgres_port = os.environ.get("POSTGRES_PORT", "5433").strip()
postgres_db = os.environ.get("POSTGRES_DB", "fraud_demo").strip()
postgres_user = os.environ.get("POSTGRES_USER", "postgres").strip()
postgres_password = os.environ.get("POSTGRES_PASSWORD", "postgres").strip()

GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-flash-latest")
POLL_INTERVAL_SECONDS = 5

PROMPT_TEMPLATE = """\
Kamu bantu tim fraud analyst bank menjelaskan kenapa sebuah transaksi
di-flag oleh sistem deteksi otomatis. Jelaskan singkat (2-3 kalimat, bahasa
Indonesia, tanpa basa-basi) berdasarkan data ini -- JANGAN mengklaim ini
pasti fraud, sistem cuma menandai sebagai PERLU DIPERIKSA:

- account_id: {account_id}
- jumlah transaksi dalam window: {txn_count_window}
- amount transaksi representatif: Rp {amount:,.0f}
- merchant: {merchant}
- raw_score (semakin tinggi dari 1.0, semakin melewati ambang batas normal): {raw_score:.2f}
"""


def get_connection():
    return psycopg2.connect(
        host=postgres_host,
        port=postgres_port,
        dbname=postgres_db,
        user=postgres_user,
        password=postgres_password,
    )


def explain(client: genai.Client, row: dict) -> str:
    prompt = PROMPT_TEMPLATE.format(**row)
    response = client.models.generate_content(model=GEMINI_MODEL, contents=prompt)
    return (response.text or "").strip()


def process_pending(conn, client: genai.Client) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT transaction_id, account_id, amount, merchant, txn_count_window, raw_score
            FROM fraud_alerts
            WHERE explanation IS NULL
            ORDER BY flagged_at
            LIMIT 20
        """)
        rows = cur.fetchall()

    for transaction_id, account_id, amount, merchant, txn_count_window, raw_score in rows:
        explanation = explain(client, {
            "account_id": account_id,
            "amount": amount,
            "merchant": merchant,
            "txn_count_window": txn_count_window,
            "raw_score": raw_score,
        })
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE fraud_alerts SET explanation = %s, explained_at = now() WHERE transaction_id = %s",
                (explanation, transaction_id),
            )
            cur.execute(
                "INSERT INTO governance_log (transaction_id, stage, detail) VALUES (%s, 'llm_explained', %s)",
                (transaction_id, explanation),
            )
        conn.commit()
        print(f"[explained] transaction_id={transaction_id} account_id={account_id}\n  -> {explanation}\n")

    return len(rows)


def main():
    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    conn = get_connection()
    print(f"Polling fraud_alerts di {postgres_host}:{postgres_port}/{postgres_db} tiap {POLL_INTERVAL_SECONDS}s (Ctrl+C untuk berhenti)\n")
    try:
        while True:
            n = process_pending(conn, client)
            if n == 0:
                time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        pass
    finally:
        conn.close()


if __name__ == "__main__":
    main()
