"""
Kirim data JSON contoh ke Kafka, khusus buat nge-test
jobs/flink_sink_postgre/topic_to_postgre.py end-to-end tanpa perlu topic
Kafka beneran yang sudah ada isinya.

Baca baris-baris JSON dari sample_data.jsonl (satu objek JSON per baris,
skemanya sudah cocok dengan kafka_source di topic_to_postgre.py: id, nama,
nilai, event_time) lalu produce satu-satu ke KAFKA_TOPIC (dari .env).

PENTING -- JANGAN jalankan ini kalau KAFKA_TOPIC di .env masih menunjuk ke
topic CDC beneran (misal CDC_AVRO.FBNK_CUSTOMER dari broker dev Bank
Sinarmas) -- itu topic production-ish, bukan buat ditulisi data contoh, dan
formatnya juga Avro bukan JSON. Ganti dulu KAFKA_TOPIC di .env ke nama
topic scratch/test milikmu sendiri sebelum jalankan script ini (broker
Kafka pada umumnya auto-create topic baru begitu ada yang produce ke nama
topic yang belum pernah ada).

Butuh `kafka-python` (sudah ditambahkan ke requirements.txt).

Jalankan:
    source .venv/bin/activate
    python jobs/flink_sink_postgre/produce_test_data.py
"""

import json
import os

from dotenv import load_dotenv
from kafka import KafkaProducer

base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(base_dir, ".env"))

bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
topic = os.environ.get("KAFKA_TOPIC", "").strip()

if not bootstrap_servers or not topic:
    raise SystemExit("KAFKA_BOOTSTRAP_SERVERS / KAFKA_TOPIC masih kosong di .env.")

if "CDC" in topic.upper():
    raise SystemExit(
        f"KAFKA_TOPIC saat ini ('{topic}') kelihatan seperti topic CDC beneran, "
        "bukan topic test -- dibatalkan biar tidak salah nulis data contoh ke "
        "situ. Ganti dulu KAFKA_TOPIC di .env ke nama topic scratch/test "
        "kamu sendiri, baru jalankan lagi script ini."
    )

sample_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sample_data.jsonl")

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers.split(","),
    value_serializer=lambda v: v.encode("utf-8"),
)

sent = 0
with open(sample_file, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        json.loads(line)  # validasi cepat -- biar ketauan kalau ada baris rusak
        producer.send(topic, value=line)
        sent += 1

producer.flush()
producer.close()

print(f"Selesai kirim {sent} pesan JSON ke topic '{topic}' @ {bootstrap_servers}")
print("Sekarang jalankan jobs/flink_sink_postgre/topic_to_postgre.py buat lihat hasilnya masuk ke Postgres.")
