"""
SOURCE dari Kafka -> Flink SQL -> SINK ke PostgreSQL LOCAL kamu.

Konsepnya gabungan dari dua contoh sebelumnya:
- hello_flink_kafka.py buat pola CREATE TABLE source Kafka.
- hello_flink_sink.py buat pola INSERT INTO ... SELECT ke sebuah sink table.

Bedanya di sini: sink-nya bukan file, tapi tabel Postgres beneran lewat
connector 'jdbc'.

SEBELUM JALANKAN INI:
1. Buat tabelnya dulu di Postgres -- connector jdbc Flink TIDAK auto-create
   tabel, cuma nulis ke tabel yang sudah ada. Contoh DDL (sesuaikan nama
   tabel dengan POSTGRES_TABLE di .env):

       CREATE TABLE kafka_sink (
           id BIGINT,
           nama TEXT,
           nilai DOUBLE PRECISION,
           event_time TIMESTAMP
       );

2. Isi kredensial Postgres di .env: POSTGRES_HOST, POSTGRES_PORT,
   POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_TABLE.
   Sama seperti KAFKA_BOOTSTRAP_SERVERS, sengaja tidak di-hardcode di sini
   biar kredensial tidak ikut ke-commit.

SKEMA kafka_source DI BAWAH INI CUMA CONTOH PLACEHOLDER (id, nama, nilai,
event_time) -- ganti sesuai skema JSON asli topic KAFKA_TOPIC kamu. Kalau
belum tahu skemanya, jalankan dulu hello_flink_kafka.py (format='raw') buat
lihat isi mentah pesannya.

Butuh 3 JAR sekaligus (didaftarkan sama-sama lewat 'pipeline.jars', dipisah
titik-koma):
- jars/flink-sql-connector-kafka-1.17.2.jar  -> connector 'kafka'
- jars/flink-connector-jdbc-3.1.2-1.17.jar   -> connector 'jdbc'
- jars/postgresql-42.7.4.jar                 -> driver JDBC Postgres
  (flink-connector-jdbc TIDAK membundel driver vendor manapun, jadi driver
  Postgres-nya harus didaftarkan terpisah)

Job ini genuinely UNBOUNDED (job Kafka sink pada umumnya jalan terus
menerima data baru) -- .wait() di bawah akan menggantung sampai kamu
Ctrl+C. Ini beda dari hello_flink_sink.py yang sink-nya dari data bounded
makanya .wait() otomatis selesai.

Tabel sink di bawah SENGAJA tanpa PRIMARY KEY -- artinya tiap pesan Kafka
jadi satu baris INSERT baru di Postgres (append-only), bukan upsert. Kalau
mau idempotent (baris dengan id yang sama menimpa, bukan dobel), tambahkan
'PRIMARY KEY (id) NOT ENFORCED' di CREATE TABLE postgres_sink -- connector
jdbc otomatis pindah ke mode upsert begitu ada primary key.

Jalankan:
    source .venv/bin/activate
    python jobs/flink_sink_postgre/topic_to_postgre.py
"""

import os

from dotenv import load_dotenv
from pyflink.table import EnvironmentSettings, TableEnvironment

base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(base_dir, ".env"))

bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
topic = os.environ.get("KAFKA_TOPIC", "").strip()

postgres_host = os.environ.get("POSTGRES_HOST", "").strip()
postgres_port = os.environ.get("POSTGRES_PORT", "").strip()
postgres_db = os.environ.get("POSTGRES_DB", "").strip()
postgres_user = os.environ.get("POSTGRES_USER", "").strip()
postgres_password = os.environ.get("POSTGRES_PASSWORD", "").strip()
postgres_table = os.environ.get("POSTGRES_TABLE", "").strip()


if not bootstrap_servers:
    raise SystemExit(
        "KAFKA_BOOTSTRAP_SERVERS masih kosong di .env. "
        "Isi dulu dengan alamat broker Kafka kamu, misal:\n"
        "KAFKA_BOOTSTRAP_SERVERS=broker1:9092,broker2:9092"
    )
if not all([postgres_host, postgres_port, postgres_db, postgres_user, postgres_table]):
    raise SystemExit(
        "Kredensial Postgres masih kosong di .env. Isi dulu POSTGRES_HOST, "
        "POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, "
        "POSTGRES_TABLE (lihat template.env)."
    )

env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# 3 JAR sekaligus, dipisah titik-koma, masing-masing tetap pakai prefix
# file:// -- lihat penjelasan lengkap di docstring modul ini.
kafka_jar = os.path.join(base_dir, "jars", "flink-sql-connector-kafka-1.17.2.jar")
jdbc_jar = os.path.join(base_dir, "jars", "flink-connector-jdbc-3.1.2-1.17.jar")
postgres_driver_jar = os.path.join(base_dir, "jars", "postgresql-42.7.4.jar")
t_env.get_config().set(
    "pipeline.jars",
    f"file://{kafka_jar};file://{jdbc_jar};file://{postgres_driver_jar}",
)

t_env.execute_sql(f"""
    CREATE TABLE kafka_source (
        id BIGINT,
        nama STRING,
        nilai DOUBLE,
        event_time TIMESTAMP(3)
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{topic}',
        'properties.bootstrap.servers' = '{bootstrap_servers}',
        'properties.group.id' = 'pyflink-belajar-sink-postgre',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601',
        'json.ignore-parse-errors' = 'true'
    )
""")

jdbc_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"

t_env.execute_sql(f"""
    CREATE TABLE postgres_sink (
        id BIGINT,
        nama STRING,
        nilai DOUBLE,
        event_time TIMESTAMP(3)
    ) WITH (
        'connector' = 'jdbc',
        'url' = '{jdbc_url}',
        'table-name' = '{postgres_table}',
        'username' = '{postgres_user}',
        'password' = '{postgres_password}'
    )
""")

print(
    f"Nyink topic '{topic}' -> Postgres {postgres_host}:{postgres_port}/"
    f"{postgres_db}.{postgres_table} ... (Ctrl+C untuk berhenti)\n"
)

# INSERT INTO ... SELECT: contoh transformasi paling sederhana (SELECT *
# apa adanya). Ganti SELECT-nya kalau mau filter/transform kolom dulu
# sebelum masuk Postgres.
t_env.execute_sql("INSERT INTO postgres_sink SELECT * FROM kafka_source").wait()
