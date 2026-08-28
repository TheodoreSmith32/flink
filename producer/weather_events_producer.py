"""
Producer cuaca "beneran" (bukan skenario demo interval join) -- kirim
dummy weather event ke topic `weather_events` secara terus-menerus,
format **plain JSON, TANPA Schema Registry**.

Pasangan dari producer/trip_events_producer.py -- sengaja dibikin simetris
biar gampang dibaca berdampingan. Producer ini murni simulasi peran "Python
producer (weather)" di arsitektur `flink_traffic_weather_use_case_upt.pdf`
(section 2) -- jalan selamanya, generate reading acak per zona, gak peduli
bakal di-join sama apa.

KENAPA plain JSON, bukan Avro+Schema Registry: sesuai desain di dokumen use
case, `trip_events` yang dapet Avro+SR (karena datangnya dari CDC Debezium,
schema-nya perlu dijaga ketat), `weather_events` sengaja dibiarkan JSON
polos karena datang dari producer sederhana kayak file ini -- lihat
`jobs/hello_flink_kafka_avro.py` kalau nanti butuh sisi trip_events yang
Avro (belum dibuat di file ini, di luar scope producer ini).

Zona cuaca (`location_id`) diambil dari master list data/weather_locations.json
-- SAMA PERSIS 5 zona yang dipakai jobs/hackatown/flink_sql_interval_join.py buat lookup
(lihat data/trip_locations.json), biar data dummy dari sini beneran bisa
di-join sama trip_events yang datang dari topic manapun.

Broker: dev Bank Sinarmas beneran (KAFKA_BOOTSTRAP_SERVERS) -- lihat
PERINGATAN soal topic bersama di docstring jobs/hackatown/flink_sql_interval_join.py
sebelum jalanin ini (data dummy bakal numpang lewat kalau ada consumer lain
di topic yang sama).

Jalankan:
    source .venv/bin/activate
    python producer/weather_events_producer.py
    (Ctrl+C buat berhenti -- ini genuinely jalan selamanya)
"""

import json
import os
import random
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(base_dir, ".env"))

bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
weather_topic = os.environ.get("KAFKA_WEATHER_TOPIC", "weather_events").strip()

JEDA_ANTAR_EVENT_DETIK = 5.0

with open(os.path.join(base_dir, "data", "weather_locations.json"), encoding="utf-8") as f:
    WEATHER_LOCATIONS = json.load(f)

# Kondisi + rentang curah hujan (mm) yang realistis buat tiap kondisi --
# precipitation dipilih acak DALAM rentang kondisinya, bukan lepas
# sepenuhnya acak, biar datanya konsisten (gak ada "clear" tapi
# precipitation=20).
CONDITIONS = [
    ("clear", (0, 0)),
    ("cloudy", (0, 1)),
    ("light_rain", (1, 5)),
    ("heavy_rain", (5, 20)),
    ("storm", (20, 50)),
]


def random_reading():
    zone = random.choice(WEATHER_LOCATIONS)
    condition, (lo, hi) = random.choice(CONDITIONS)
    precipitation = round(random.uniform(lo, hi), 1)
    return {
        "location_id": zone["weather_location_id"],
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        "condition": condition,
        "precipitation": precipitation,
    }, zone["weather_location_name"]


def main():
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Mengirim dummy weather event ke '{weather_topic}' @ {bootstrap_servers} "
          f"tiap {JEDA_ANTAR_EVENT_DETIK}s (Ctrl+C buat berhenti)...\n")

    try:
        while True:
            event, zone_name = random_reading()
            producer.send(weather_topic, value=event)
            producer.flush()
            print(f"  [weather] location_id={event['location_id']} ({zone_name}) "
                  f"condition={event['condition']} precipitation={event['precipitation']} "
                  f"event_time={event['event_time']}")
            time.sleep(JEDA_ANTAR_EVENT_DETIK)
    except KeyboardInterrupt:
        print("\nDihentikan.")


if __name__ == "__main__":
    main()
