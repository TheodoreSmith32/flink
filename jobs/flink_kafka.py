"""
SOURCE dari Kafka pakai DataStream API (bukan Table API/SQL).

Bedanya dengan hello_flink_kafka.py:
- Di sana pakai Table API: CREATE TABLE ... WITH ('connector' = 'kafka').
- Di sini pakai DataStream API langsung: StreamExecutionEnvironment +
  KafkaSource, tanpa skema tabel/kolom sama sekali. Tiap pesan Kafka
  diperlakukan sebagai satu string mentah (SimpleStringSchema), persis
  seperti format='raw' di versi Table API.

Tetap butuh JAR yang sama (jars/flink-sql-connector-kafka-1.17.2.jar) --
walau namanya "sql-connector", jar ini juga membundle kelas-kelas
DataStream connector (org.apache.flink.connector.kafka...) yang dipakai
KafkaSource di bawah.

Script ini TIDAK PERNAH SELESAI SENDIRI selama topic-nya masih hidup
(unbounded stream). Berhenti pakai Ctrl+C.

Jalankan:
    source .venv/bin/activate
    python flink_kafka.py
"""

import os

from dotenv import load_dotenv
from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

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

env = StreamExecutionEnvironment.get_execution_environment()

# Daftarkan JAR connector Kafka -- tanpa ini, KafkaSource di bawah akan
# gagal karena kelas connector-nya belum ada di classpath job.
jar_path = os.path.join(base_dir, "jars", "flink-sql-connector-kafka-1.17.2.jar")
env.add_jars(f"file://{jar_path}")

kafka_source = (
    KafkaSource.builder()
    .set_bootstrap_servers(bootstrap_servers)
    .set_topics(topic)
    .set_group_id("pyflink-belajar")
    .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

stream = env.from_source(
    kafka_source,
    WatermarkStrategy.no_watermarks(),
    "kafka_source",
)

print(f"Membaca topic '{topic}' dari {bootstrap_servers} ... (Ctrl+C untuk berhenti)\n")

stream.print()

env.execute("flink_kafka_datastream")
