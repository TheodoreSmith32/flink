"""
Producer trip "beneran" (bukan skenario demo interval join) -- kirim dummy
trip event ke topic `trip_events` secara terus-menerus, format
**Avro + Confluent Schema Registry** (BUKAN plain JSON lagi).

Pasangan dari producer/weather_events_producer.py, tapi sengaja BEDA format
-- lihat catatan "KENAPA plain JSON, bukan Avro" di file itu. Ini SATU-nya
producer plain-Python di project ini yang emit Avro; semua contoh Avro lain
(`jobs/hello_flink_kafka_avro.py`, `jobs/avro_schema_lookup.py`) baru sisi
CONSUME, belum ada yang PRODUCE.

INI PRODUCER SEMENTARA buat testing lokal (sebelum trip_events beneran
disambungkan ke CDC Debezium dari Postgres, sesuai rencana kamu) -- format
Avro-nya SENGAJA sudah dipasang dari sekarang (bukan nunggu CDC-nya jadi
dulu), biar `jobs/hackatown/flink_sql_interval_join.py` bisa langsung dites baca Avro.
Begitu CDC dari Postgres beneran jalan, producer ini SEHARUSNYA dimatikan --
CDC bakal register schema-nya sendiri (biasanya beda konvensi field dari
schema manual di bawah, misal ada envelope Debezium before/after/op), jadi
kemungkinan besar `trip_events` DDL di jobs/hackatown/flink_sql_interval_join.py perlu
disesuaikan lagi waktu itu, bukan tinggal pakai apa adanya.

CARA KERJA (gak pakai `confluent-kafka` -- itu butuh librdkafka, C
extension yang gak selalu gampang diinstal; project ini sudah pilih
`kafka-python` murni buat semua producer lain, jadi Avro wire-format-nya
dirakit manual di sini):
    1. Skema Avro (AVRO_SCHEMA di bawah) didaftarkan ke Schema Registry
       lewat REST API (`register_avro_schema()`, pakai `urllib` bawaan,
       pola yang sama dengan jobs/avro_schema_lookup.py) -- subject-nya
       `{topic}-value` (TopicNameStrategy, konvensi default paling umum,
       sama asumsi yang dipakai avro_schema_lookup.py). Registry idempoten:
       daftar skema yang PERSIS sama berkali-kali balikin schema id yang
       sama, jadi aman dipanggil tiap kali script ini start.
    2. Tiap record di-encode `fastavro.schemaless_writer()` (skema
       terparse), lalu dibungkus **Confluent wire format**: 1 byte magic
       (`0x00`) + 4 byte schema id (big-endian) + payload Avro biner --
       ini format yang di-expect `format = 'avro-confluent'` di Flink.
    3. `pickup_datetime` dikirim sebagai objek `datetime` (timezone-aware,
       UTC) langsung, BUKAN string ISO-8601 lagi -- field Avro-nya pakai
       logicalType `timestamp-millis` (long), fastavro yang convert. Ini
       yang bikin kolom `pickup_datetime` di Flink DDL tetap otomatis jadi
       TIMESTAMP(3) asli (bisa dipakai WATERMARK FOR), sama seperti waktu
       masih JSON.

VERIFIED (sebagian): encode/decode Confluent wire-format di atas sudah
dites roundtrip lokal pakai fastavro langsung (fake schema id, tanpa
registry beneran) -- hasilnya balik persis sama field & tipe-nya. Yang
BELUM kebukti jalan: register skema ke Schema Registry ASLI (`SCHEMA_REGISTRY_URL`
di .env nunjuk ke jaringan internal Bank Sinarmas, gak reachable dari
sandbox penulisan file ini) dan baca hasilnya lewat jobs/hackatown/flink_sql_interval_join.py
beneran.

BIAS CUACA (buat demo "hujan = trip lebih sedikit" di traffic_weather_result):
producer ini dan weather_events_producer.py TADINYA independen total --
gak ada hubungan sebab-akibat, cuma dua generator acak yang kebetulan
ke-join kalau lokasi+waktunya cocok. Ditambahkan sekarang: thread
background di sini yang JADI KafkaConsumer weather_events juga (bukan cuma
producer trip), nge-track kondisi cuaca TERKINI per zona
(_latest_condition_by_zone). Tiap mau generate trip, dicek dulu kondisi
cuaca ASLI di zona itu (dari data yang beneran dikirim
weather_events_producer.py, bukan ditebak sendiri), lalu diundi pakai
EMIT_PROBABILITY_BY_CONDITION -- makin ekstrem cuacanya, makin besar
kemungkinan trip event-nya DISKIP (gak dikirim sama sekali).

INI SIMULASI BUAT KEBUTUHAN DEMO, BUKAN DATA ORGANIK -- kalau
dipresentasikan, jelasin ke juri ini scenario yang sengaja di-skenario-in
biar polanya kelihatan, bukan "kami menemukan insight dari data asli".
Zona yang belum pernah dapat weather event sama sekali (baru start)
defaultnya dianggap probability 1.0 (gak ada alasan buat nge-skip).

Zona trip (`pu_location_id`) diambil dari master list
data/trip_locations.json -- 10 kelurahan yang sama yang dipakai
jobs/hackatown/flink_sql_interval_join.py buat lookup weather_location_id.

Broker & registry: dev Bank Sinarmas beneran (KAFKA_BOOTSTRAP_SERVERS,
SCHEMA_REGISTRY_URL) -- lihat PERINGATAN soal topic bersama di docstring
jobs/hackatown/flink_sql_interval_join.py sebelum jalanin ini.

Jalankan (weather_events_producer.py SEBAIKNYA start duluan -- bukan wajib,
tapi zona yang belum pernah dapat weather event dianggap "unknown" =
peluang kirim 1.0, jadi bias-nya baru kelihatan setelah weather event
pertama masuk buat zona itu):
    source .venv/bin/activate
    python producer/weather_events_producer.py   # terminal lain, duluan
    python producer/trip_events_producer.py
    (Ctrl+C buat berhenti -- ini genuinely jalan selamanya)
"""

import json
import os
import random
import struct
import threading
import time
from datetime import datetime, timezone
from io import BytesIO

import fastavro
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(base_dir, ".env"))

bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
trip_topic = os.environ.get("KAFKA_TRIP_TOPIC", "trip_events").strip()
weather_topic = os.environ.get("KAFKA_WEATHER_TOPIC", "weather_events").strip()
schema_registry_url = os.environ.get("SCHEMA_REGISTRY_URL", "").strip().rstrip("/")

JEDA_ANTAR_EVENT_DETIK = 5.0

with open(os.path.join(base_dir, "data", "trip_locations.json"), encoding="utf-8") as f:
    TRIP_LOCATIONS = json.load(f)

# Peluang trip event BENERAN dikirim, per kondisi cuaca zona itu SEKARANG --
# makin ekstrem, makin sering diskip. "unknown" dipakai kalau zona itu belum
# pernah dapat weather event sejak thread watcher ini start.
EMIT_PROBABILITY_BY_CONDITION = {
    "clear": 1.0,
    "cloudy": 0.85,
    "light_rain": 0.6,
    "heavy_rain": 0.3,
    "storm": 0.15,
    "unknown": 1.0,
}

# Kondisi cuaca TERKINI per weather_location_id, diisi thread
# _watch_weather_conditions() di bawah -- dibaca random_trip() tiap mau
# generate trip buat nentuin peluang skip. Lock biar aman dibaca/ditulis
# dari 2 thread beda (main loop vs consumer thread).
_latest_condition_by_zone: dict[int, str] = {}
_condition_lock = threading.Lock()


def _watch_weather_conditions():
    """Jalan di background thread selama proses ini hidup -- consume
    weather_events (plain JSON, sama seperti yang dibaca Flink), simpan
    kondisi TERBARU per zona ke _latest_condition_by_zone. auto_offset_reset
    'latest' sengaja dipakai: yang penting kondisi SEKARANG, bukan histori
    dari awal topic (menghindari lag pas start)."""
    consumer = KafkaConsumer(
        weather_topic,
        bootstrap_servers=bootstrap_servers.split(","),
        group_id=None,  # gak perlu commit offset, ini cuma observasi kondisi terkini
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    for message in consumer:
        event = message.value
        with _condition_lock:
            _latest_condition_by_zone[event["location_id"]] = event["condition"]


def _current_condition(weather_location_id: int) -> str:
    with _condition_lock:
        return _latest_condition_by_zone.get(weather_location_id, "unknown")

# Skema Avro trip_events -- field & tipenya SENGAJA dibuat cocok 1:1 sama
# DDL trip_events di jobs/hackatown/flink_sql_interval_join.py (kolom fisik-nya, BUKAN
# weather_location_id yang computed column). pickup_datetime pakai
# logicalType timestamp-millis (bukan string) biar Flink otomatis baca-nya
# sebagai TIMESTAMP(3), bukan STRING yang perlu di-parse manual.
AVRO_SCHEMA_DICT = {
    "type": "record",
    "name": "TripEvent",
    "namespace": "com.banksinarmas.flink_demo",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "pickup_datetime", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "pu_location_id", "type": "int"},
        {"name": "pu_location_name", "type": "string"},
        {"name": "trip_distance", "type": "double"},
        {"name": "fare_amount", "type": "double"},
    ],
}
AVRO_SCHEMA = fastavro.parse_schema(AVRO_SCHEMA_DICT)

CONFLUENT_MAGIC_BYTE = b"\x00"

_next_id = 1


def register_avro_schema(registry_url: str, subject: str, schema_dict: dict) -> int:
    # POST /subjects/{subject}/versions -- idempoten, skema yang PERSIS
    # sama balikin schema id yang sudah ada (gak bikin versi baru).
    import urllib.request

    payload = json.dumps({"schema": json.dumps(schema_dict)}).encode("utf-8")
    req = urllib.request.Request(
        f"{registry_url}/subjects/{subject}/versions",
        data=payload,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())["id"]


def to_confluent_avro(record: dict, schema_id: int) -> bytes:
    buf = BytesIO()
    buf.write(CONFLUENT_MAGIC_BYTE)
    buf.write(struct.pack(">I", schema_id))
    fastavro.schemaless_writer(buf, AVRO_SCHEMA, record)
    return buf.getvalue()


def build_trip(loc: dict) -> dict:
    global _next_id
    trip_distance = round(random.uniform(0.5, 15.0), 1)
    fare_amount = round(4 + trip_distance * random.uniform(3.0, 5.0))

    trip = {
        "id": _next_id,
        "pickup_datetime": datetime.now(timezone.utc),
        "pu_location_id": loc["pu_location_id"],
        "pu_location_name": loc["pu_location_name"],
        "trip_distance": trip_distance,
        "fare_amount": float(fare_amount),
    }
    _next_id += 1
    return trip


def main():
    if not schema_registry_url:
        raise SystemExit("SCHEMA_REGISTRY_URL masih kosong di .env.")

    subject = f"{trip_topic}-value"
    schema_id = register_avro_schema(schema_registry_url, subject, AVRO_SCHEMA_DICT)
    print(f"Skema terdaftar di Schema Registry: subject='{subject}' schema_id={schema_id}\n")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=lambda v: to_confluent_avro(v, schema_id),
    )

    # Daemon=True biar thread ini ikut mati begitu proses utama berhenti
    # (Ctrl+C) -- gak perlu join() manual.
    watcher = threading.Thread(target=_watch_weather_conditions, daemon=True)
    watcher.start()

    print(f"Mengirim dummy trip event (Avro) ke '{trip_topic}' @ {bootstrap_servers} "
          f"tiap {JEDA_ANTAR_EVENT_DETIK}s (Ctrl+C buat berhenti)...")
    print("Dibiaskan cuaca (simulasi demo): makin ekstrem cuaca zona itu SEKARANG, "
          "makin sering trip event diskip.\n")

    try:
        while True:
            loc = random.choice(TRIP_LOCATIONS)
            condition = _current_condition(loc["weather_location_id"])
            probability = EMIT_PROBABILITY_BY_CONDITION.get(condition, 1.0)

            if random.random() > probability:
                print(f"  [skip]    pu_location_id={loc['pu_location_id']} "
                      f"({loc['pu_location_name']}) cuaca zona={condition} "
                      f"(peluang kirim={probability}) -- trip TIDAK dikirim")
                time.sleep(JEDA_ANTAR_EVENT_DETIK)
                continue

            trip = build_trip(loc)
            producer.send(trip_topic, value=trip)
            producer.flush()
            print(f"  [trip]    id={trip['id']} pu_location_id={trip['pu_location_id']} "
                  f"({trip['pu_location_name']}) cuaca zona={condition} "
                  f"trip_distance={trip['trip_distance']} fare_amount={trip['fare_amount']} "
                  f"pickup_datetime={trip['pickup_datetime'].isoformat()}")
            time.sleep(JEDA_ANTAR_EVENT_DETIK)
    except KeyboardInterrupt:
        print("\nDihentikan.")


if __name__ == "__main__":
    main()
