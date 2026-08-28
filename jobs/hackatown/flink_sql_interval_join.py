"""
Interval join (Table API/SQL) antara trip_events x weather_events --
roadmap #11 windowing, bagian "stateful join dua stream" dari
`flink_traffic_weather_use_case_upt.pdf` (section 4-5).

Satu folder sama `flink_sql_01.py`, TAPI SENGAJA file terpisah: flink_sql_01.py
sudah punya interval join yang SAMA, cuma dibundel jadi satu sama windowed
anomaly detection (DataStream API, ProcessWindowFunction). Di sini cuma
bagian interval join-nya doang, Table API murni, biar konsepnya kelihatan
sendiri dulu sebelum lanjut ke anomaly detection -- lebih gampang buat
verifikasi source Avro/JSON-nya kebaca bener SEBELUM nambah kerumitan
windowing di atasnya.

GRANULARITAS LOKASI: trip_events dan weather_events TIDAK punya
granularitas lokasi yang sama. trip_events dikirim per KELURAHAN
(`pu_location_id`, lihat data/trip_locations.json -- Grogol, Tomang, Kebon
Jeruk, dst), sementara weather_events cuma dikirim per KOTA ADMINISTRASI
(`location_id`, lihat data/weather_locations.json -- cuma 5 zona: Jakarta
Pusat/Utara/Barat/Selatan/Timur), sesuai cakupan stasiun BMKG yang beneran
(gak ada 1 stasiun cuaca per kelurahan). Artinya BANYAK pu_location_id trip
bisa merujuk ke SATU location_id cuaca yang sama (many-to-one) -- Grogol,
Tomang, DAN Kebon Jeruk semuanya "Jakarta Barat".

Konsekuensinya: trip_events.pu_location_id TIDAK BISA langsung dibandingkan
ke weather_events.location_id (beda skema angka sama sekali). Perlu 1
langkah lookup dulu: pu_location_id -> weather_location_id, pakai
data/trip_locations.json sebagai master list-nya.

KENAPA LOOKUP-NYA PAKAI UDF + COMPUTED COLUMN, BUKAN JOIN KE TABEL
data/trip_locations.json LANGSUNG: opsi paling "SQL banget" adalah bikin
trip_locations jadi CREATE TABLE (connector filesystem) lalu JOIN biasa ke
trip_events dulu sebelum interval join ke weather_events. TAPI itu berisiko
merusak status "time attribute" pada kolom pickup_datetime -- interval join
di bawah MENSYARATKAN kedua sisi punya watermark yang valid, dan join biasa
di antara dua tabel bisa bikin kolom rowtime jadi TIMESTAMP biasa (bukan lagi
time attribute) tergantung shape query-nya. Computed column + Python UDF
(pure function, dieksekusi per baris tanpa menyentuh watermark) menghindari
masalah itu sama sekali -- pickup_datetime tetap didefinisikan WATERMARK FOR
langsung dari source, gak lewat operator join tambahan.

VERIFIED (sebagian): `register_source_tables()` + `build_joined_table()` di
bawah sudah dites lolos DDL parsing & query planning Flink beneran (bukan
cuma bikin skrip-nya doang), TERMASUK setelah trip_events dipindah ke
`format = 'avro-confluent'` -- `joined_table.get_schema()` mengonfirmasi
`pickup_datetime` di hasil query tetap ditandai `*ROWTIME*`, artinya time
attribute-nya BENERAN gak rusak lewat computed column ini. Encode/decode
Confluent wire-format Avro-nya sendiri (lihat producer/trip_events_producer.py)
juga sudah dites roundtrip lokal pakai fastavro langsung. Yang BELUM
kebukti jalan: register skema ke Schema Registry ASLI dan baca data Avro
asli dari topic Kafka beneran -- itu butuh broker+registry beneran, gak
reachable dari sandbox penulisan file ini (SCHEMA_REGISTRY_URL nunjuk ke
jaringan internal Bank Sinarmas).

FORMAT PESAN: `trip_events` = **Avro + Confluent Schema Registry**,
`weather_events` = **plain JSON**. Ini keputusan yang sama dengan desain
`flink_traffic_weather_use_case_upt.pdf` (trip_events dari CDC Debezium,
naturalnya emit Avro+SR; weather_events dari producer Python sederhana,
gak butuh registry) -- lihat catatan lengkap di
`producer/trip_events_producer.py` & `producer/weather_events_producer.py`.
JAR TAMBAHAN dibutuhkan buat sisi Avro: `flink-sql-avro-confluent-registry-1.17.2.jar`
(sama seperti `jobs/hello_flink_kafka_avro.py`), didaftarkan bareng JAR
kafka lewat `pipeline.jars`.

Field JSON & artinya: lihat tabel di README.md section 9.

Broker & topic yang dipakai: broker dev Bank Sinarmas beneran
(`KAFKA_BOOTSTRAP_SERVERS` di .env), topic `trip_events`/`weather_events` --
SAMA PERSIS dengan topic yang di-hardcode di flink_sql_01.py (satu folder).

PERINGATAN: kalau topic ini juga dipakai/di-consume oleh pipeline lain
(misal flink_sql_01.py, atau CDC Debezium beneran dari Postgres seperti di
`flink_traffic_weather_use_case_upt.pdf`), event dummy dari
producer/trip_events_producer.py & producer/weather_events_producer.py
bakal numpang lewat di topic yang sama dan ikut ke-consume di sana juga --
pastikan itu bukan masalah sebelum jalanin producer-nya. trip_events_producer.py
khusus buat testing SEBELUM CDC dari Postgres beneran jalan -- matikan kalau
CDC-nya sudah live (lihat catatan di file itu).

CATATAN INSIDEN (ketemu pas beneran dites jalan): `trip_events` sempat
error `Unknown data format. Magic number does not match` dari deserializer
`avro-confluent` -- penyebabnya ada pesan LAMA non-Avro nyangkut di topic
(dari sebelum `trip_events_producer.py` dipindah ke Avro+SR), dan
`scan.startup.mode = earliest-offset` bikin Flink kebentur pesan itu duluan.
Fix-nya: **hapus & bikin ulang topic `trip_events` (dan/atau hapus subject
`trip_events-value` di Schema Registry)** sebelum run ulang, biar topic-nya
bersih cuma isi Avro -- bukan diakalin ganti ke `latest-offset`, supaya
`earliest-offset` di bawah tetap aman dipakai. Kalau kejadian lagi
(magic-byte-mismatch), itu tandanya ada pesan non-Avro numpang lagi di
topic -- bersihin topic-nya, bukan format DDL-nya yang diubah.

SEBELUM JALANKAN (2 terminal terpisah):
    python producer/weather_events_producer.py
    python producer/trip_events_producer.py

Jalankan:
    source .venv/bin/activate
    JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 python jobs/hackatown/flink_sql_interval_join.py
    (Ctrl+C buat berhenti -- ini genuinely unbounded, sama seperti
    hello_flink_kafka.py/hello_flink_watermark.py)
"""

import json
import os

from dotenv import load_dotenv
from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.udf import udf

base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(base_dir, ".env"))

# Broker dev Bank Sinarmas beneran + topic yang sama dengan flink_sql_01.py
# (satu folder) -- lihat PERINGATAN soal topic bersama di docstring atas
# sebelum jalanin producer-nya.
bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
trip_topic = os.environ.get("KAFKA_TRIP_TOPIC", "trip_events").strip()
weather_topic = os.environ.get("KAFKA_WEATHER_TOPIC", "weather_events").strip()
# Cuma dipakai trip_events (Avro+SR) -- weather_events tetap plain JSON,
# gak butuh registry sama sekali.
schema_registry_url = os.environ.get("SCHEMA_REGISTRY_URL", "").strip()

# Toleransi keterlambatan sebelum watermark nganggap event "sudah lewat" --
# sesuai flink_sql_01.py & dokumen use case, sama-sama 5 menit di kedua sisi.
OUT_OF_ORDERNESS_MINUTES = 5
# Jarak waktu maksimum antara trip dan cuaca supaya masih dianggap "cocok" --
# ini yang bikin state join bisa dibuang (lihat catatan di docstring atas).
JOIN_INTERVAL_MINUTES = 30

# Master list pu_location_id (kelurahan, trip) -> weather_location_id (kota
# administrasi, cuaca) -- SATU sumber kebenaran yang sama dipakai
# producer/trip_events_producer.py & producer/weather_events_producer.py
# buat nge-generate nama lokasi. Lihat data/trip_locations.json &
# data/weather_locations.json.
with open(os.path.join(base_dir, "data", "trip_locations.json"), encoding="utf-8") as f:
    _TRIP_LOCATIONS = json.load(f)
WEATHER_ZONE_BY_PU_LOCATION = {
    loc["pu_location_id"]: loc["weather_location_id"] for loc in _TRIP_LOCATIONS
}


@udf(result_type=DataTypes.INT())
def weather_zone_of(pu_location_id):
    # Pure function -- gak nyentuh watermark/time attribute sama sekali,
    # lihat catatan "KENAPA LOOKUP-NYA PAKAI UDF" di docstring atas.
    return WEATHER_ZONE_BY_PU_LOCATION.get(pu_location_id)


def create_table_env() -> TableEnvironment:
    if not schema_registry_url:
        raise SystemExit("SCHEMA_REGISTRY_URL masih kosong di .env (dibutuhkan trip_events, format avro-confluent).")

    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    # Dua JAR: connector Kafka + format avro-confluent (buat trip_events).
    # weather_events (plain JSON) gak butuh JAR tambahan di luar Kafka
    # connector, sama seperti sebelumnya. Pola sama dengan
    # jobs/hello_flink_kafka_avro.py.
    kafka_jar = os.path.join(base_dir, "jars", "flink-sql-connector-kafka-1.17.2.jar")
    avro_jar = os.path.join(base_dir, "jars", "flink-sql-avro-confluent-registry-1.17.2.jar")
    t_env.get_config().set("pipeline.jars", f"file://{kafka_jar};file://{avro_jar}")

    # WAJIB didaftarkan SEBELUM CREATE TABLE trip_events di bawah, karena
    # dipakai sebagai computed column (weather_zone_of(pu_location_id)) di
    # DDL-nya -- Flink perlu functionnya sudah ada di catalog pas parse DDL.
    t_env.create_temporary_function("weather_zone_of", weather_zone_of)

    # Beda dari fraud_job.py/hello_flink_watermark.py: baris ini di sini
    # BUKAN buat menghindari bug watermark-gak-maju (window TUMBLE di kedua
    # file itu emang butuh watermark maju dulu buat nutup window). Interval
    # join JUSTRU langsung emit match begitu ketemu di state, gak nunggu
    # watermark -- watermark cuma dipakai buat bersihin state lama yang udah
    # gak mungkin match lagi. Baris ini cuma jaga-jaga: topic di broker dev
    # ini partition-nya belum tentu 1 kayak topic lokal, jadi biar demo ini
    # gak kena masalah idle-subtask yang sama kalau ternyata partition-nya
    # sedikit. Aman dihapus kalau throughput lebih penting dari kesederhanaan.
    t_env.get_config().set("parallelism.default", "1")
    return t_env


def register_source_tables(t_env: TableEnvironment):
    # format = 'avro-confluent' -- Flink tanya Schema Registry buat tau
    # writer schema aslinya, lalu deserialize byte Avro-nya (magic byte +
    # schema id + payload biner, ditulis producer/trip_events_producer.py)
    # jadi kolom SQL di bawah. Kolom & tipe di sini HARUS cocok sama skema
    # Avro asli-nya (lihat AVRO_SCHEMA_DICT di producer/trip_events_producer.py)
    # -- Flink TIDAK auto-generate DDL dari registry, sama seperti catatan
    # di jobs/hello_flink_kafka_avro.py. pickup_datetime otomatis jadi
    # TIMESTAMP(3) dari logicalType timestamp-millis di skema Avro-nya,
    # BUKAN di-parse dari string kayak waktu masih format json.
    t_env.execute_sql(f"""
        CREATE TABLE trip_events (
            id BIGINT,
            pickup_datetime TIMESTAMP(3),
            pu_location_id INT,
            pu_location_name STRING,
            trip_distance DOUBLE,
            fare_amount DOUBLE,
            weather_location_id AS weather_zone_of(pu_location_id),
            WATERMARK FOR pickup_datetime AS pickup_datetime - INTERVAL '{OUT_OF_ORDERNESS_MINUTES}' MINUTE
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{trip_topic}',
            'properties.bootstrap.servers' = '{bootstrap_servers}',
            'properties.group.id' = 'pyflink-interval-join-demo',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'avro-confluent',
            'avro-confluent.schema-registry.url' = '{schema_registry_url}'
        )
    """)

    # Kolom `condition` dibungkus backtick -- CONDITION reserved keyword di
    # Flink SQL, sama persis kasusnya dengan flink_sql_01.py.
    t_env.execute_sql(f"""
        CREATE TABLE weather_events (
            location_id INT,
            event_time TIMESTAMP(3),
            `condition` STRING,
            precipitation DOUBLE,
            WATERMARK FOR event_time AS event_time - INTERVAL '{OUT_OF_ORDERNESS_MINUTES}' MINUTE
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{weather_topic}',
            'properties.bootstrap.servers' = '{bootstrap_servers}',
            'properties.group.id' = 'pyflink-interval-join-demo',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)


def build_joined_table(t_env: TableEnvironment):
    return t_env.sql_query(f"""
        SELECT
            t.pickup_datetime,
            t.pu_location_id,
            t.pu_location_name,
            t.weather_location_id,
            t.trip_distance,
            t.fare_amount,
            w.`condition`,
            w.precipitation
        FROM trip_events t
        JOIN weather_events w
            ON t.weather_location_id = w.location_id
            AND t.pickup_datetime BETWEEN w.event_time - INTERVAL '{JOIN_INTERVAL_MINUTES}' MINUTE
                                       AND w.event_time + INTERVAL '{JOIN_INTERVAL_MINUTES}' MINUTE
    """)


def main():
    t_env = create_table_env()
    register_source_tables(t_env)

    print(f"Baca topic '{trip_topic}' + '{weather_topic}' dari {bootstrap_servers} "
          "(Ctrl+C untuk berhenti)")
    print(f"Interval join +/- {JOIN_INTERVAL_MINUTES} menit, lokasi trip (kelurahan) "
          "di-lookup ke zona cuaca (kota) dulu lewat weather_zone_of().\n"
          "Jalankan producer/weather_events_producer.py + producer/trip_events_producer.py "
          "di terminal lain buat kirim contoh event.\n")

    joined_table = build_joined_table(t_env)
    joined_table.execute().print()


if __name__ == "__main__":
    main()
