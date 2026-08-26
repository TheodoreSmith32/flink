"""
Dashboard read-only buat use case fraud detection (usecases/fraud_detection/)
-- BUKAN bagian dari Session Manager/TableEnvironment sama sekali, cuma baca
langsung dari Postgres LOKAL (docker-compose.yml) lewat psycopg2, sama seperti
llm_explainer_worker.py. Sengaja terpisah dari flink_runner.py/session_manager.py
karena dashboard ini tidak butuh Flink apa pun, cuma nampilin ISI tabel yang
sudah ditulis fraud_job.py + llm_explainer_worker.py.

Kalau tabelnya belum ada (docker-compose belum dijalankan / init.sql belum
jalan), fungsi di bawah sengaja melempar error yang jelas ("Postgres fraud
detection belum bisa diakses...") daripada stack trace psycopg2 mentah,
supaya kelihatan di /fraud-alerts kalau infra-nya belum dinyalakan -- lihat
usecases/fraud_detection/fraud_job.py buat cara nyalain docker-compose-nya.
"""

import os

import psycopg2

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Default-nya sama kayak usecases/fraud_detection/fraud_job.py & docker-compose.yml.
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost").strip()
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5433").strip()
POSTGRES_DB = os.environ.get("POSTGRES_DB", "fraud_demo").strip()
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres").strip()
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres").strip()


def _connect():
    try:
        return psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        raise RuntimeError(
            f"Postgres fraud detection belum bisa diakses di {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB} -- "
            f"jalankan `docker compose up -d` dulu dari root project. Detail: {exc}"
        ) from exc


def get_alerts(limit: int = 50) -> list[dict]:
    """Baris fraud_alerts terbaru dulu, plus jumlah baris governance_log
    terkait (buat nunjukin ada berapa 'jejak audit' per transaksi -- biasanya
    2: flink_flagged + llm_explained, atau 1 kalau belum di-explain)."""
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT
                fa.transaction_id, fa.account_id, fa.amount, fa.merchant,
                fa.txn_count_window, fa.raw_score, fa.flagged_at,
                fa.explanation, fa.explained_at,
                COUNT(gl.id) AS governance_entries
            FROM fraud_alerts fa
            LEFT JOIN governance_log gl ON gl.transaction_id = fa.transaction_id
            GROUP BY fa.transaction_id, fa.account_id, fa.amount, fa.merchant,
                     fa.txn_count_window, fa.raw_score, fa.flagged_at,
                     fa.explanation, fa.explained_at
            ORDER BY fa.flagged_at DESC
            LIMIT %s
        """, (limit,))
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

    return [
        {col: (value.isoformat() if hasattr(value, "isoformat") else value) for col, value in zip(columns, row)}
        for row in rows
    ]


def get_summary() -> dict:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM fraud_alerts")
        total_alerts = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM fraud_alerts WHERE explanation IS NOT NULL")
        explained = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT account_id) FROM fraud_alerts")
        distinct_accounts = cur.fetchone()[0]
    return {
        "total_alerts": total_alerts,
        "explained": explained,
        "distinct_accounts": distinct_accounts,
    }
