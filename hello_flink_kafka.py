"""
SOURCE dari Kafka. Konsepnya sama seperti hello_flink_file.py (CREATE TABLE
... WITH ('connector' = ...)), cuma sekarang connector-nya 'kafka' dan
sumbernya topic yang terus menerima data (unbounded), bukan file yang ada
ujungnya (bounded).

Bedanya dengan semua contoh sebelumnya:
- Butuh JAR connector Kafka (tidak kebundle di pip install apache-flink).
  Sudah didownload ke jars/flink-sql-connector-kafka-1.17.2.jar -- versinya
  HARUS cocok dengan versi apache-flink di requirements.txt.
- Broker Kafka & nama topic diambil dari .env (KAFKA_BOOTSTRAP_SERVERS,
  KAFKA_TOPIC), BUKAN di-hardcode di sini -- supaya alamat broker tidak
  ikut ke-commit/kebaca orang lain lewat kode.
- Script ini TIDAK PERNAH SELESAI SENDIRI selama topic-nya masih hidup
  (unbounded stream beneran, bukan bounded stream yang cuma "streaming
  mode doang" seperti hello_flink_streaming.py). Berhenti pakai Ctrl+C.
- format = 'raw' dipakai karena kita belum tahu skema pesan di topic ini --
  'raw' membaca SELURUH isi pesan Kafka sebagai satu kolom STRING apa
  adanya, tanpa asumsi JSON/Avro/dll. Begitu tahu skemanya, connector
  format bisa diganti (misal 'json') supaya field-field-nya otomatis
  terpisah jadi kolom.

Jalankan:
    source .venv/bin/activate
    python hello_flink_kafka.py
"""

import os

from dotenv import load_dotenv
from pyflink.table import EnvironmentSettings, TableEnvironment

base_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(base_dir, ".env"))

bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
topic = os.environ.get("KAFKA_TOPIC", "").strip()

if not bootstrap_servers:
    raise SystemExit(
        "KAFKA_BOOTSTRAP_SERVERS masih kosong di .env. "
        "Isi dulu dengan alamat broker Kafka kamu, misal:\n"
        "KAFKA_BOOTSTRAP_SERVERS=broker1:9092,broker2:9092"
    )

env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# Daftarkan JAR connector Kafka -- tanpa ini, Flink tidak kenal
# 'connector' = 'kafka' dan CREATE TABLE di bawah akan gagal.
jar_path = os.path.join(base_dir, "jars", "flink-sql-connector-kafka-1.17.2.jar")
t_env.get_config().set("pipeline.jars", f"file://{jar_path}")

t_env.execute_sql(f"""
    CREATE TABLE kafka_source (
        message STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{topic}',
        'properties.bootstrap.servers' = '{bootstrap_servers}',
        'properties.group.id' = 'pyflink-belajar',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'raw'
    )
""")

print(f"Membaca topic '{topic}' dari {bootstrap_servers} ... (Ctrl+C untuk berhenti)\n")

t_env.sql_query("SELECT message FROM kafka_source").execute().print()
