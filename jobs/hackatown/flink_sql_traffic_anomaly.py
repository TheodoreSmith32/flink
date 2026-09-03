"""
Deteksi anomali trip berdasar cuaca -- gabungan TABLE API (source Avro+JSON,
interval join trip x cuaca, sama konsepnya dengan
jobs/hackatown/flink_sql_interval_join.py) + DATASTREAM API (window 15
menit, ProcessWindowFunction, niru pola yang sudah ada di
jobs/hackatown/flink_sql_01.py).

FILE INI SENGAJA BERDIRI SENDIRI -- TIDAK MENGUBAH ATAU MENG-IMPORT
flink_sql_interval_join.py MAUPUN flink_sql_01.py SAMA SEKALI (use case
interval-join-doang yang lama harus tetap seperti semula, sesuai
permintaan). Konsekuensinya: source table DDL, UDF `weather_zone_of`, dan
master list lookup DI BAWAH INI TERDUPLIKASI PERSIS dari
flink_sql_interval_join.py -- ini konsisten juga dengan gaya project ini
(tiap script belajar berdiri sendiri, lihat hello_flink_*.py). Kalau salah
satu DDL/lookup itu berubah di file lain, HARUS diubah manual di sini juga.

STRUKTUR PESAN KAFKA TIDAK BERUBAH SAMA SEKALI: `trip_events` (Avro +
Confluent Schema Registry) dan `weather_events` (plain JSON) dibaca dengan
DDL identik ke flink_sql_interval_join.py, dari producer yang sama persis
(producer/trip_events_producer.py, producer/weather_events_producer.py) --
file ini murni computation TAMBAHAN di sisi Flink, tidak menyentuh
producer/skema pesan sama sekali.

KENAPA BUTUH DUA API SEKALIGUS: interval join wajib pakai Table API/SQL
(PyFlink DataStream API tidak punya operator interval-join siap pakai),
tapi window 15-menit + hitung per-zona (ProcessWindowFunction) butuh
DataStream API (Table API PyFlink belum expose custom window function
sefleksibel itu). Jadi hasil join "diturunkan" ke DataStream
(`t_env.to_data_stream()`), diproses per window, lalu "dinaikkan" lagi ke
Table (`t_env.from_data_stream()`) buat di-sink ke MySQL lewat SQL biasa.
Watermark dari DDL source (WATERMARK FOR pickup_datetime ...) otomatis
kebawa ke DataStream, jadi window di bawah tetap event-time asli, bukan
processing-time.

KENAPA ANOMALI DI SINI BUKAN THRESHOLD FLAT (beda dari placeholder
`trip_count < BASELINE_MIN_COUNT` di flink_sql_01.py): producer/trip_events_producer.py
SENGAJA menurunkan jumlah trip kalau cuaca zona itu lagi ekstrem
(`EMIT_PROBABILITY_BY_CONDITION` di file itu) -- trip count rendah pas hujan
deras itu POLA YANG DIHARAPKAN, bukan anomali. Anomali yang bener di sini =
trip count actual MENYIMPANG jauh dari ekspektasi berdasar cuaca dominan di
window itu (lihat EXPECTED_TRIP_COUNT_BY_CONDITION di bawah), baik naik
ATAU turun di luar rentang wajar. Baseline-nya MASIH STATIS (dihitung
manual dari parameter producer, lihat komentar di konstanta itu), BUKAN
baseline adaptif dari histori (z-score dari state per key) -- itu next
step kalau mau lebih canggih.

SINK: hasil anomali di-INSERT ke MySQL tabel `traffic_anomaly_result`
(BEDA dari `traffic_weather_result` yang dipakai flink_sql_interval_join.py
-- tabel terpisah, DDL terpisah di
kafka-db/mysql-ddl/traffic_anomaly_result.sql) lewat kredensial
MYSQL_SINK_HOST/PORT/DB/USER/PASSWORD yang SAMA (host/db/user/password),
cuma nama tabelnya beda (`MYSQL_ANOMALY_TABLE` di .env, lihat template.env).

SEBELUM JALANKAN:
    1. Jalankan DDL tabel anomali (kalau volume MySQL sudah pernah
       diinisialisasi sebelum file DDL ini ada, docker-entrypoint-initdb.d
       TIDAK otomatis jalan lagi -- sama seperti catatan di
       flink_sql_interval_join.py):
           docker exec -i mysql mysql -uroot -pmysql < kafka-db/mysql-ddl/traffic_anomaly_result.sql
    2. Isi MYSQL_ANOMALY_TABLE di .env (lihat template.env) -- kredensial
       MYSQL_SINK_* lainnya reuse yang sudah ada kalau
       flink_sql_interval_join.py pernah disetup.
    3 & 4 (2 terminal terpisah, biarin jalan selamanya):
           python producer/weather_events_producer.py
           python producer/trip_events_producer.py

Jalankan:
    source .venv/bin/activate
    JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 python jobs/hackatown/flink_sql_traffic_anomaly.py
    (Ctrl+C buat berhenti -- genuinely unbounded, sama seperti
    flink_sql_interval_join.py)

VERIFIED (sebagian, sama seperti flink_sql_interval_join.py): create_table_env()
+ register_source_tables() + register_anomaly_sink_table() +
build_joined_table() + run_anomaly_detection() sudah dites beneran jalan
sampai job graph selesai dibangun tanpa exception -- TERMASUK
joined_table.get_schema() mengonfirmasi pickup_datetime tetap ditandai
*ROWTIME* setelah lewat computed column weather_zone_of(), dan bridging
Table -> DataStream (window 15 menit + ProcessWindowFunction) -> Table lagi
berhasil di-plan. Yang BELUM kebukti jalan: baca data Avro/JSON asli dari
topic Kafka beneran dan tulis ke MySQL beneran -- itu butuh
broker+registry+MySQL beneran, gak reachable dari sandbox penulisan file
ini (KAFKA_BOOTSTRAP_SERVERS/SCHEMA_REGISTRY_URL nunjuk ke jaringan
internal Bank Sinarmas, MySQL butuh kafka-db/docker-compose.yml jalan).
"""

import json
import os
from collections import Counter

from dotenv import load_dotenv
from pyflink.common import Row
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.table import DataTypes, Schema, StreamTableEnvironment
from pyflink.table.udf import udf

base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(base_dir, ".env"))

bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
trip_topic = os.environ.get("KAFKA_TRIP_TOPIC", "trip_events").strip()
weather_topic = os.environ.get("KAFKA_WEATHER_TOPIC", "weather_events").strip()
schema_registry_url = os.environ.get("SCHEMA_REGISTRY_URL", "").strip()

# Sink hasil anomali -- MySQL dari kafka-db/docker-compose.yml, database +
# kredensial SAMA dengan flink_sql_interval_join.py, tabelnya beda.
mysql_host = os.environ.get("MYSQL_SINK_HOST", "").strip()
mysql_port = os.environ.get("MYSQL_SINK_PORT", "").strip()
mysql_db = os.environ.get("MYSQL_SINK_DB", "").strip()
mysql_user = os.environ.get("MYSQL_SINK_USER", "").strip()
mysql_password = os.environ.get("MYSQL_SINK_PASSWORD", "").strip()
mysql_anomaly_table = os.environ.get("MYSQL_ANOMALY_TABLE", "").strip()

OUT_OF_ORDERNESS_MINUTES = 5
JOIN_INTERVAL_MINUTES = 30
WINDOW_MINUTES = 15

# DUPLIKAT dari flink_sql_interval_join.py (lihat catatan di docstring atas
# soal kenapa gak di-import) -- pu_location_id (kelurahan, trip) ->
# weather_location_id (kota administrasi, cuaca).
with open(os.path.join(base_dir, "data", "trip_locations.json"), encoding="utf-8") as f:
    _TRIP_LOCATIONS = json.load(f)
WEATHER_ZONE_BY_PU_LOCATION = {
    loc["pu_location_id"]: loc["weather_location_id"] for loc in _TRIP_LOCATIONS
}


@udf(result_type=DataTypes.INT())
def weather_zone_of(pu_location_id):
    return WEATHER_ZONE_BY_PU_LOCATION.get(pu_location_id)


# Baseline trip count per window 15 menit PER ZONA CUACA, kalau kondisinya
# 'clear' -- dihitung manual dari parameter producer/trip_events_producer.py,
# BUKAN ditebak: JEDA_ANTAR_EVENT_DETIK=5 detik di producer itu -> dalam 15
# menit (900 detik) ada 900/5 = 180 percobaan generate trip (gabungan SEMUA
# zona), dibagi rata ke 5 zona cuaca (2 dari 10 kelurahan di
# trip_locations.json ngarah ke zona yang sama) -> 180 * (2/10) = 36
# trip/window per zona kalau emit probability=1.0 ('clear'/'unknown', lihat
# EMIT_PROBABILITY_BY_CONDITION di producer itu). HARUS disesuaikan manual
# kalau salah satu dari 2 angka itu berubah di producer.
BASELINE_TRIP_COUNT_CLEAR = 36

# Mirror EMIT_PROBABILITY_BY_CONDITION di producer/trip_events_producer.py --
# expected count = baseline 'clear' x probability kirim buat kondisi itu.
EXPECTED_TRIP_COUNT_BY_CONDITION = {
    "clear": round(BASELINE_TRIP_COUNT_CLEAR * 1.0),
    "cloudy": round(BASELINE_TRIP_COUNT_CLEAR * 0.85),
    "light_rain": round(BASELINE_TRIP_COUNT_CLEAR * 0.6),
    "heavy_rain": round(BASELINE_TRIP_COUNT_CLEAR * 0.3),
    "storm": round(BASELINE_TRIP_COUNT_CLEAR * 0.15),
}

# Toleransi penyimpangan dari ekspektasi sebelum ditandai anomali -- dua
# arah (trip TERLALU BANYAK dari ekspektasi juga ditandai, bukan cuma
# kekurangan).
ANOMALY_LOWER_RATIO = 0.5
ANOMALY_UPPER_RATIO = 1.5


def create_table_env():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # cukup 1 buat demo, sama seperti flink_sql_01.py

    # env.add_jars(), BUKAN t_env.get_config().set("pipeline.jars", ...) --
    # environment ini dibangun dari StreamExecutionEnvironment (dibutuhkan
    # DataStream API di bawah), pola yang kebukti jalan di
    # jobs/hackatown/flink_sql_01.py (lihat catatan "FIXED" di docstring-nya).
    kafka_jar = os.path.join(base_dir, "jars", "flink-sql-connector-kafka-1.17.2.jar")
    avro_jar = os.path.join(base_dir, "jars", "flink-sql-avro-confluent-registry-1.17.2.jar")
    jdbc_jar = os.path.join(base_dir, "jars", "flink-connector-jdbc-3.1.2-1.17.jar")
    mysql_driver_jar = os.path.join(base_dir, "jars", "mysql-connector-j-8.0.33.jar")
    env.add_jars(
        f"file://{kafka_jar}",
        f"file://{avro_jar}",
        f"file://{jdbc_jar}",
        f"file://{mysql_driver_jar}",
    )

    t_env = StreamTableEnvironment.create(env)
    # WAJIB sebelum CREATE TABLE trip_events di bawah -- dipakai sebagai
    # computed column (weather_zone_of(pu_location_id)) di DDL-nya.
    t_env.create_temporary_function("weather_zone_of", weather_zone_of)
    return env, t_env


def register_source_tables(t_env: StreamTableEnvironment):
    # DDL identik dengan register_source_tables() di
    # flink_sql_interval_join.py -- lihat komentar lengkap di file itu soal
    # kenapa formatnya beda per topic dan kenapa lookup-nya lewat UDF.
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
            'properties.group.id' = 'pyflink-traffic-anomaly',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'avro-confluent',
            'avro-confluent.schema-registry.url' = '{schema_registry_url}'
        )
    """)

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
            'properties.group.id' = 'pyflink-traffic-anomaly',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)


def register_anomaly_sink_table(t_env: StreamTableEnvironment):
    # PRIMARY KEY komposit (weather_location_id, window_end) -- sama
    # alasannya dengan traffic_weather_result.sql: bikin connector jdbc
    # pindah ke mode UPSERT, idempotent kalau job di-restart & window yang
    # sama diproses ulang (source-nya earliest-offset).
    jdbc_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_db}"
    t_env.execute_sql(f"""
        CREATE TABLE mysql_anomaly_sink (
            weather_location_id INT,
            window_end TIMESTAMP(3),
            trip_count BIGINT,
            avg_fare DOUBLE,
            weather_condition STRING,
            expected_trip_count BIGINT,
            is_anomaly BOOLEAN,
            PRIMARY KEY (weather_location_id, window_end) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{jdbc_url}',
            'table-name' = '{mysql_anomaly_table}',
            'username' = '{mysql_user}',
            'password' = '{mysql_password}'
        )
    """)


def build_joined_table(t_env: StreamTableEnvironment):
    # Cuma kolom yang beneran dibutuhkan buat window di bawah -- BEDA dari
    # flink_sql_interval_join.py yang select semua kolom trip buat sink
    # per-trip (id, pu_location_name, dst gak relevan buat agregasi ini).
    return t_env.sql_query(f"""
        SELECT
            t.pickup_datetime,
            t.weather_location_id,
            t.fare_amount,
            w.`condition` AS weather_condition
        FROM trip_events t
        JOIN weather_events w
            ON t.weather_location_id = w.location_id
            AND t.pickup_datetime BETWEEN w.event_time - INTERVAL '{JOIN_INTERVAL_MINUTES}' MINUTE
                                       AND w.event_time + INTERVAL '{JOIN_INTERVAL_MINUTES}' MINUTE
    """)


class AnomalyDetector(ProcessWindowFunction):
    """
    Per window 15 menit per zona cuaca: hitung trip_count + avg_fare, cari
    weather_condition DOMINAN di window itu (modus -- window bisa aja
    kelewat 2 weather event kalau cuacanya berubah dalam 15 menit itu), lalu
    bandingkan trip_count actual ke EXPECTED_TRIP_COUNT_BY_CONDITION buat
    kondisi dominan itu. is_anomaly = True kalau actual di luar rentang
    [expected * ANOMALY_LOWER_RATIO, expected * ANOMALY_UPPER_RATIO].

    CATATAN KETERBATASAN: window yang BENERAN kosong (gak ada satupun trip
    ke-join di zona itu selama 15 menit) TIDAK memicu process() ini sama
    sekali -- window keyed di Flink cuma dibuat begitu ada elemen pertama
    masuk. Artinya "zona mati total" tidak akan pernah ditandai anomali
    lewat mekanisme ini (butuh timer per-key tambahan kalau itu mau
    dideteksi juga -- di luar scope versi ini).
    """

    def process(self, key, context, elements, out):
        rows = list(elements)
        trip_count = len(rows)
        avg_fare = round(sum(r.fare_amount for r in rows) / trip_count, 2) if trip_count > 0 else 0.0

        condition_counts = Counter(r.weather_condition for r in rows)
        dominant_condition = condition_counts.most_common(1)[0][0] if rows else "unknown"
        expected = EXPECTED_TRIP_COUNT_BY_CONDITION.get(dominant_condition, BASELINE_TRIP_COUNT_CLEAR)
        is_anomaly = trip_count < expected * ANOMALY_LOWER_RATIO or trip_count > expected * ANOMALY_UPPER_RATIO

        out.collect(Row(
            weather_location_id=key,
            window_end=context.window().end,
            trip_count=trip_count,
            avg_fare=avg_fare,
            weather_condition=dominant_condition,
            expected_trip_count=expected,
            is_anomaly=is_anomaly,
        ))


def run_anomaly_detection(t_env: StreamTableEnvironment, joined_table):
    # Table -> DataStream. Hasil interval join append-only (bukan changelog
    # update/delete), jadi cukup to_data_stream(). Watermark dari DDL sumber
    # otomatis kebawa (lihat catatan di docstring atas).
    ds = t_env.to_data_stream(joined_table)

    # output_type WAJIB eksplisit -- tanpa ini PyFlink gak bisa infer
    # nama+tipe field dari Row yang di-collect() AnomalyDetector, dan
    # from_data_stream() di bawah gagal nyari kolom-kolomnya (ketemu &
    # di-fix di flink_sql_01.py, pola yang sama dipakai di sini).
    anomaly_type_info = Types.ROW_NAMED(
        [
            "weather_location_id",
            "window_end",
            "trip_count",
            "avg_fare",
            "weather_condition",
            "expected_trip_count",
            "is_anomaly",
        ],
        [
            Types.INT(),
            Types.SQL_TIMESTAMP(),
            Types.LONG(),
            Types.DOUBLE(),
            Types.STRING(),
            Types.LONG(),
            Types.BOOLEAN(),
        ],
    )
    anomaly_stream = (
        ds.key_by(lambda r: r.weather_location_id)
          .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_MINUTES)))
          .process(AnomalyDetector(), output_type=anomaly_type_info)
    )

    # DataStream -> Table (balik lagi ke SQL buat sink JDBC).
    anomaly_table = t_env.from_data_stream(
        anomaly_stream,
        Schema.new_builder()
              .column("weather_location_id", DataTypes.INT())
              .column("window_end", DataTypes.TIMESTAMP(3))
              .column("trip_count", DataTypes.BIGINT())
              .column("avg_fare", DataTypes.DOUBLE())
              .column("weather_condition", DataTypes.STRING())
              .column("expected_trip_count", DataTypes.BIGINT())
              .column("is_anomaly", DataTypes.BOOLEAN())
              .build()
    )
    t_env.create_temporary_view("anomaly_result", anomaly_table)


def main():
    if not schema_registry_url:
        raise SystemExit("SCHEMA_REGISTRY_URL masih kosong di .env (dibutuhkan trip_events, format avro-confluent).")
    if not all([mysql_host, mysql_port, mysql_db, mysql_user, mysql_anomaly_table]):
        raise SystemExit(
            "Kredensial MySQL anomaly sink masih kosong di .env. Isi MYSQL_SINK_HOST/PORT/DB/USER/PASSWORD "
            "(sudah ada kalau flink_sql_interval_join.py pernah disetup) + MYSQL_ANOMALY_TABLE (baru -- lihat "
            "template.env), dan jalankan kafka-db/mysql-ddl/traffic_anomaly_result.sql dulu di MySQL-nya."
        )

    env, t_env = create_table_env()
    register_source_tables(t_env)
    register_anomaly_sink_table(t_env)

    print(f"Baca topic '{trip_topic}' + '{weather_topic}' dari {bootstrap_servers} (Ctrl+C untuk berhenti)")
    print(f"Window {WINDOW_MINUTES} menit per zona cuaca, dibandingkan ke ekspektasi per kondisi cuaca "
          f"({EXPECTED_TRIP_COUNT_BY_CONDITION}).\n"
          f"Hasil di-sink ke MySQL {mysql_host}:{mysql_port}/{mysql_db}.{mysql_anomaly_table}.\n"
          "Jalankan producer/weather_events_producer.py + producer/trip_events_producer.py "
          "di terminal lain buat kirim contoh event.\n")

    joined_table = build_joined_table(t_env)
    run_anomaly_detection(t_env, joined_table)

    # Genuinely unbounded, sama seperti flink_sql_interval_join.py -- .wait()
    # di sini bakal menggantung sampai Ctrl+C.
    t_env.execute_sql("""
        INSERT INTO mysql_anomaly_sink
        SELECT weather_location_id, window_end, trip_count, avg_fare,
               weather_condition, expected_trip_count, is_anomaly
        FROM anomaly_result
    """).wait()


if __name__ == "__main__":
    main()
