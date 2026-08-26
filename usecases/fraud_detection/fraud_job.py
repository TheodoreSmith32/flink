"""
Fraud detection job (pakai PyFlink Table API/SQL murni, tanpa DataStream API
-- beda pendekatan dari jobs/hackatown/flink_sql_01.py yang pakai
ProcessWindowFunction, sengaja dipilih di sini buat nunjukin windowing lewat
window TVF `TUMBLE` versi SQL, lihat dokumentasi Flink soal Windowing TVF).

Alur:
    transactions_source (Kafka, JSON)
      -> window TUMBLE 1 menit, GROUP BY account_id
      -> HAVING jumlah transaksi ATAU nilai transaksi terbesar di window itu
         melewati threshold (lihat MIN_TXN_COUNT / MIN_TXN_AMOUNT di bawah --
         placeholder, sama kasarnya dengan BASELINE_MIN_COUNT di
         flink_sql_01.py, belum berbasis baseline historis per akun)
      -> ditulis SEKALIGUS (satu Flink job, StatementSet -- baca sumber yang
         sama, tidak dobel-consume Kafka) ke dua sink Postgres:
           - fraud_alerts   : baris flagged-nya sendiri
           - governance_log : audit trail stage 'flink_flagged'

llm_explainer_worker.py (proses Python terpisah, BUKAN bagian dari job ini)
yang nanti mengisi kolom fraud_alerts.explanation -- job Flink ini sengaja
tidak pernah memanggil LLM, biar latency LLM tidak numpang di jalur
streaming (sama alasannya dengan yang sudah didiskusikan buat use case ini).

SEBELUM JALANKAN:
    docker compose up -d
    python usecases/fraud_detection/generate_transactions.py   (di terminal lain)

Jalankan:
    source .venv/bin/activate
    python usecases/fraud_detection/fraud_job.py
"""

import os

from dotenv import load_dotenv
from pyflink.table import EnvironmentSettings, TableEnvironment

base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(base_dir, ".env"))

bootstrap_servers = os.environ.get("KAFKA_FRAUD_BOOTSTRAP_SERVERS", "localhost:9094").strip()
topic = os.environ.get("KAFKA_FRAUD_TOPIC", "fraud_transactions").strip()

# Default-nya sengaja cocok sama docker-compose.yml (host port 5433, db/user/
# password default) -- biar demo bisa langsung jalan tanpa isi .env dulu.
# Override lewat .env kalau kamu ubah docker-compose.yml.
postgres_host = os.environ.get("POSTGRES_HOST", "localhost").strip()
postgres_port = os.environ.get("POSTGRES_PORT", "5433").strip()
postgres_db = os.environ.get("POSTGRES_DB", "fraud_demo").strip()
postgres_user = os.environ.get("POSTGRES_USER", "postgres").strip()
postgres_password = os.environ.get("POSTGRES_PASSWORD", "postgres").strip()

# Threshold placeholder -- kasar, belum berbasis baseline historis per akun
# (sama seperti catatan BASELINE_MIN_COUNT di flink_sql_01.py).
MIN_TXN_COUNT = 5
MIN_TXN_AMOUNT = 1_000_000.0
WINDOW_MINUTES = 1


def create_table_env() -> TableEnvironment:
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    kafka_jar = os.path.join(base_dir, "jars", "flink-sql-connector-kafka-1.17.2.jar")
    jdbc_jar = os.path.join(base_dir, "jars", "flink-connector-jdbc-3.1.2-1.17.jar")
    postgres_driver_jar = os.path.join(base_dir, "jars", "postgresql-42.7.4.jar")
    t_env.get_config().set(
        "pipeline.jars",
        f"file://{kafka_jar};file://{jdbc_jar};file://{postgres_driver_jar}",
    )
    t_env.get_config().set("pipeline.name", "fraud-detection-demo")
    # WAJIB -- tanpa ini, TableEnvironment default ke parallelism = jumlah
    # CPU core (16 di sandbox ini). transactions_source cuma punya 1 Kafka
    # partition, jadi source subtask ke-2..16 gak pernah dapat split (idle
    # selamanya). Watermark gabungan Flink = MINIMUM watermark dari SEMUA
    # subtask paralel termasuk yang idle itu -- makanya window TUMBLE di
    # bawah gak akan PERNAH fire di stream unbounded (idle subtask nge-stuck
    # watermark-nya), walau data yang lewat partition yang aktif kelihatan
    # normal. Baru ketemu setelah dites jalan beneran: debug bounded-read
    # terpisah (scan.bounded.mode=latest-offset) sempat kelihatan "window
    # kerja normal" karena source BOUNDED otomatis emit watermark akhir pas
    # selesai, beda dari source UNBOUNDED yang idle selamanya kalau parallel
    # instance-nya gak kebagian partition.
    t_env.get_config().set("parallelism.default", "1")
    return t_env


def register_source(t_env: TableEnvironment):
    t_env.execute_sql(f"""
        CREATE TABLE transactions_source (
            transaction_id BIGINT,
            account_id STRING,
            amount DOUBLE,
            merchant STRING,
            event_time TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic}',
            'properties.bootstrap.servers' = '{bootstrap_servers}',
            'properties.group.id' = 'pyflink-fraud-detection',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)


def register_sinks(t_env: TableEnvironment):
    jdbc_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"

    t_env.execute_sql(f"""
        CREATE TABLE fraud_alerts_sink (
            transaction_id BIGINT,
            account_id STRING,
            amount DOUBLE,
            merchant STRING,
            txn_count_window BIGINT,
            raw_score DOUBLE,
            flagged_at TIMESTAMP(3)
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{jdbc_url}',
            'table-name' = 'fraud_alerts',
            'username' = '{postgres_user}',
            'password' = '{postgres_password}'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE governance_log_sink (
            transaction_id BIGINT,
            stage STRING,
            detail STRING
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{jdbc_url}',
            'table-name' = 'governance_log',
            'username' = '{postgres_user}',
            'password' = '{postgres_password}'
        )
    """)


def build_flagged_view(t_env: TableEnvironment):
    # Window TVF TUMBLE (SQL, bukan DataStream ProcessWindowFunction) --
    # LAST_VALUE dipakai buat ambil satu transaksi representatif dari window
    # itu (yang paling akhir) sebagai baris yang disimpan ke fraud_alerts;
    # txn_count_window & raw_score menjelaskan KENAPA window itu di-flag.
    t_env.execute_sql(f"""
        CREATE TEMPORARY VIEW flagged_transactions AS
        SELECT
            LAST_VALUE(transaction_id) AS transaction_id,
            account_id,
            LAST_VALUE(amount) AS amount,
            LAST_VALUE(merchant) AS merchant,
            COUNT(*) AS txn_count_window,
            GREATEST(
                CAST(COUNT(*) AS DOUBLE) / {MIN_TXN_COUNT},
                MAX(amount) / {MIN_TXN_AMOUNT}
            ) AS raw_score,
            window_end AS flagged_at
        FROM TABLE(
            TUMBLE(TABLE transactions_source, DESCRIPTOR(event_time), INTERVAL '{WINDOW_MINUTES}' MINUTE)
        )
        GROUP BY window_start, window_end, account_id
        HAVING COUNT(*) >= {MIN_TXN_COUNT} OR MAX(amount) >= {MIN_TXN_AMOUNT}
    """)


def main():
    t_env = create_table_env()
    register_source(t_env)
    register_sinks(t_env)
    build_flagged_view(t_env)

    print(
        f"Fraud detection job jalan: topic='{topic}' @ {bootstrap_servers} -> "
        f"Postgres {postgres_host}:{postgres_port}/{postgres_db} "
        f"(fraud_alerts + governance_log). Ctrl+C untuk berhenti.\n"
    )

    # Satu job (StatementSet), dua sink, baca transactions_source SEKALI --
    # bukan dua job terpisah yang masing-masing consume ulang dari Kafka.
    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert_sql("""
        INSERT INTO fraud_alerts_sink
        SELECT transaction_id, account_id, amount, merchant, txn_count_window, raw_score, flagged_at
        FROM flagged_transactions
    """)
    stmt_set.add_insert_sql(f"""
        INSERT INTO governance_log_sink
        SELECT
            transaction_id,
            'flink_flagged',
            CONCAT('txn_count_window=', CAST(txn_count_window AS STRING), ' raw_score=', CAST(raw_score AS STRING))
        FROM flagged_transactions
    """)
    stmt_set.execute().wait()


if __name__ == "__main__":
    main()
