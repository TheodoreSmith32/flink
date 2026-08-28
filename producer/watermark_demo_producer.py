"""
Producer buat hello_flink_watermark.py -- kirim 6 event bernomor ke topic
Kafka lokal (docker-compose.yml, port 9094), PERSIS di urutan list EVENTS
di bawah (BUKAN diurutkan dulu berdasar event_time), termasuk satu event
yang SENGAJA dikirim telat parah, supaya efek "watermark nutup window lalu
event telat di-drop" kelihatan asli lewat hello_flink_watermark.py.

Kenapa gap antar-kirimnya pakai time.sleep() beneran (bukan cuma beda nilai
event_time di payload): supaya Kafka consumer di sisi Flink beneran nerima
pesan-pesan ini terpisah waktu asli, bukan numpuk sekaligus dalam satu
batch kecil -- watermark generator native Flink (JVM, lihat WATERMARK FOR
di hello_flink_watermark.py) tick berdasarkan waktu asli/wall-clock, jadi
kalau semua pesan nyampe nyaris bersamaan, window bisa langsung nutup
semuanya sekaligus dan efek drop-nya gak kelihatan jelas.

Jalankan (setelah `docker compose up -d` dan hello_flink_watermark.py
sudah jalan duluan di terminal lain, biar consumer-nya udah siap nunggu):
    source .venv/bin/activate
    python producer/watermark_demo_producer.py
"""

import json
import os
import time
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(base_dir, ".env"))

bootstrap_servers = os.environ.get("KAFKA_LOCAL_BOOTSTRAP_SERVERS", "localhost:9094").strip()
topic = os.environ.get("KAFKA_WATERMARK_TOPIC", "watermark_demo").strip()

JEDA_ANTAR_EVENT_DETIK = 2.0

# (value, event_time detik sejak start) -- dikirim PERSIS urutan ini.
# Window TUMBLE di hello_flink_watermark.py = 20 detik, out-of-orderness
# yang ditoleransi = 5 detik.
EVENTS = [
    (10, 2),   # window [0,20)
    (20, 8),   # window [0,20)
    (15, 15),  # window [0,20)
    (99, 26),  # window [20,40) -- watermark jadi 26-5=21s, LEWAT 20s -> window [0,20) DITUTUP di sini
    (5, 4),    # TELAT! harusnya masuk window [0,20) yang SUDAH ditutup -- amati: nongol di hasil atau tidak?
    (30, 45),  # window [40,60) -- watermark jadi 45-5=40s -> window [20,40) ditutup
]


def main():
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    base_time = datetime.now(timezone.utc)
    print(f"Mengirim {len(EVENTS)} event ke topic '{topic}' @ {bootstrap_servers} "
          f"(jeda {JEDA_ANTAR_EVENT_DETIK}s antar event)...\n")

    for value, offset_s in EVENTS:
        event_time = base_time + timedelta(seconds=offset_s)
        payload = {
            "sensor_id": "sensor-1",
            "value_": value,
            "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
        }
        producer.send(topic, value=payload)
        producer.flush()
        print(f"  >> value={value} event_time=+{offset_s}s ({payload['event_time']})")
        time.sleep(JEDA_ANTAR_EVENT_DETIK)

    print("\nSelesai. Cek output hello_flink_watermark.py -- apakah value=5 "
          "muncul di window [0,20) atau tidak sama sekali?")


if __name__ == "__main__":
    main()
