"""
Contoh use case "traffic anomaly detection": gabungan dua stream Kafka
(trip_events + weather_events) lewat interval join (Table API/SQL), lalu
deteksi anomali per lokasi per window 15 menit lewat DataStream API
(ProcessWindowFunction) -- bukan fraud detection perbankan yang dibahas di
awal, ini contoh lain buat latihan windowing + gabungan Table API/DataStream.

FIXED (sebelumnya file ini gak bisa jalan -- NameError di banyak tempat,
kelihatannya kepotong pas disalin dari draft/dokumen use case):
- Import yang tadinya belum ada sama sekali: StreamExecutionEnvironment,
  StreamTableEnvironment, Schema, DataTypes, Row, Time,
  TumblingEventTimeWindows, ProcessWindowFunction.
- `BOOTSTRAP_SERVERS` (huruf besar, gak pernah didefinisikan) di
  register_source_tables()/register_sink_table() diganti jadi
  `bootstrap_servers` (variabel yang beneran ada, dibaca dari .env).
- `BASELINE_MIN_COUNT` ditambahkan sebagai konstanta placeholder di bawah --
  threshold ini MASIH SEMBARANGAN (5), belum berbasis data asli. Sesuai
  komentar AnomalyDetector di bawah, ini yang nanti diganti pakai
  baseline/z-score dari histori beneran.
- Kolom `condition` di `weather_events` dibungkus backtick (`` `condition` ``)
  di DDL DAN di SELECT-nya -- `CONDITION` reserved keyword di Flink SQL,
  tanpa backtick parser-nya nolak dengan `ParseException`. Ini ketemu pas
  script-nya beneran dijalankan (bukan cuma dites impor), jadi kemungkinan
  belum pernah sekali pun berhasil sampai ke tahap ini sebelumnya.
- JAR connector Kafka (`jars/flink-sql-connector-kafka-1.17.2.jar`) tidak
  pernah didaftarkan (`env.add_jars(...)`) di `create_table_env()` -- tanpa
  ini, tiap `CREATE TABLE ... WITH ('connector'='kafka')` di bawah gagal
  dengan "Could not find any factory for identifier 'kafka'". Ini juga
  ketemu lewat jalanin beneran, bukan tebak-tebakan.
- `.process(AnomalyDetector())` di `run_anomaly_detection()` gak dikasih
  `output_type` eksplisit -- PyFlink gak bisa infer nama+tipe kolom dari
  `Row` yang di-collect() (cuma kelihatan 1 field generic `f0`), jadi
  `from_data_stream()` sesudahnya gagal nyari kolom `location_id` dkk.
  Ditambahkan `Types.ROW_NAMED(...)` eksplisit -- ketemu & dites juga lewat
  jalanin beneran.

Topic Kafka (`trip_events`, `weather_events`, `traffic_anomaly_events`) masih
di-hardcode, bukan dari `.env` seperti script lain di project ini -- sengaja
tidak diubah di sini (di luar scope perbaikan NameError), tapi kalau ini mau
dipakai serius, pindahkan ke `.env` dulu.

Sudah dites jalan sampai titik SEBELUM konek ke broker Kafka beneran (semua
NameError, ParseException, dan error factory/type-info di atas sudah
kepencet dan diperbaiki) -- yang belum bisa dites di sini cuma bagian
"beneran baca data dari topic trip_events/weather_events", karena sandbox
ini gak punya broker Kafka. Isi KAFKA_BOOTSTRAP_SERVERS di .env dan pastikan
kedua topic itu ada + ada datanya buat verifikasi penuh end-to-end.
"""

import os

from dotenv import load_dotenv
from pyflink.common import Row
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.table import DataTypes, Schema, StreamTableEnvironment

base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(base_dir, ".env"))

bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
topic = os.environ.get("KAFKA_TOPIC", "").strip()

# Placeholder threshold -- lihat catatan di AnomalyDetector di bawah soal
# rencana ganti ini pakai baseline/z-score dari data historis beneran.
BASELINE_MIN_COUNT = 5


# ---------------------------------------------------------------------------
# 1. Setup environment
# ---------------------------------------------------------------------------
def create_table_env():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # cukup 1 buat demo, naikkan kalau perlu

    # JAR connector Kafka -- tanpa ini, CREATE TABLE ... WITH ('connector'='kafka')
    # di bawah gagal dengan "Could not find any factory for identifier 'kafka'"
    # (ketemu pas dites jalan beneran; env.add_jars() dipakai bukan
    # t_env.get_config().set("pipeline.jars", ...) karena environment ini
    # dibangun dari StreamExecutionEnvironment, sama seperti jobs/flink_kafka.py).
    kafka_jar = os.path.join(base_dir, "jars", "flink-sql-connector-kafka-1.17.2.jar")
    env.add_jars(f"file://{kafka_jar}")

    t_env = StreamTableEnvironment.create(env)
    return env, t_env

 
# ---------------------------------------------------------------------------
# 2. DDL source tables (plain JSON, tanpa Avro/Debezium dulu)
# ---------------------------------------------------------------------------
def register_source_tables(t_env: StreamTableEnvironment):
    t_env.execute_sql(f"""
        CREATE TABLE trip_events (
            id BIGINT,
            pickup_datetime TIMESTAMP(3),
            pu_location_id INT,
            trip_distance DOUBLE,
            fare_amount DOUBLE,
            WATERMARK FOR pickup_datetime AS pickup_datetime - INTERVAL '5' MINUTE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'trip_events',
            'properties.bootstrap.servers' = '{bootstrap_servers}',
            'properties.group.id' = 'flink-traffic-anomaly',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)
 
    t_env.execute_sql(f"""
        CREATE TABLE weather_events (
            location_id INT,
            event_time TIMESTAMP(3),
            `condition` STRING,
            precipitation DOUBLE,
            WATERMARK FOR event_time AS event_time - INTERVAL '5' MINUTE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'weather_events',
            'properties.bootstrap.servers' = '{bootstrap_servers}',
            'properties.group.id' = 'flink-traffic-anomaly',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)
 
 
# ---------------------------------------------------------------------------
# 3. DDL sink table (plain JSON juga, sesuai topic yang sudah dibuat)
# ---------------------------------------------------------------------------
def register_sink_table(t_env: StreamTableEnvironment):
    t_env.execute_sql(f"""
        CREATE TABLE traffic_anomaly_events (
            location_id INT,
            window_end TIMESTAMP(3),
            trip_count BIGINT,
            avg_fare DOUBLE,
            is_anomaly BOOLEAN
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'traffic_anomaly_events',
            'properties.bootstrap.servers' = '{bootstrap_servers}',
            'format' = 'json'
        )
    """)
 
 
# ---------------------------------------------------------------------------
# 4. Interval join (Table API / SQL)
# ---------------------------------------------------------------------------
def build_joined_table(t_env: StreamTableEnvironment):
    return t_env.sql_query("""
        SELECT
            t.pickup_datetime,
            t.pu_location_id,
            t.trip_distance,
            t.fare_amount,
            w.`condition`,
            w.precipitation
        FROM trip_events t
        JOIN weather_events w
            ON t.pu_location_id = w.location_id
            AND t.pickup_datetime BETWEEN w.event_time - INTERVAL '30' MINUTE
                                       AND w.event_time + INTERVAL '30' MINUTE
    """)
 
 
# ---------------------------------------------------------------------------
# 5. Custom logic - DataStream API: windowed anomaly detection
# ---------------------------------------------------------------------------
class AnomalyDetector(ProcessWindowFunction):
    """
    Logic sederhana: kalau jumlah trip dalam window 15 menit di satu lokasi
    turun di bawah BASELINE_MIN_COUNT, tandai sebagai anomaly.
    (Placeholder threshold - nanti bisa diganti z-score/percentile terhadap
    baseline historis yang disimpan di state, sesuai catatan section 10
    dokumen use case.)
    """
 
    def process(self, key, context, elements, out):
        rows = list(elements)
        trip_count = len(rows)
        avg_fare = sum(r.fare_amount for r in rows) / trip_count if trip_count > 0 else 0.0
        is_anomaly = trip_count < BASELINE_MIN_COUNT
 
        out.collect(Row(
            location_id=key,
            window_end=context.window().end,
            trip_count=trip_count,
            avg_fare=round(avg_fare, 2),
            is_anomaly=is_anomaly,
        ))
 
 
def run_anomaly_detection(t_env: StreamTableEnvironment, joined_table):
    # Table -> DataStream
    # Karena hasil interval join append-only (bukan changelog dengan update/delete),
    # cukup to_data_stream(). Watermark dari DDL sumber otomatis terbawa.
    ds = t_env.to_data_stream(joined_table)
 
    # output_type WAJIB diisi eksplisit -- tanpa ini, PyFlink gak bisa infer
    # nama+tipe field dari Row yang di-collect() AnomalyDetector (cuma
    # kelihatan sebagai 1 field generic 'f0'), dan from_data_stream() di
    # bawah gagal nyari kolom 'location_id' dkk. Ketemu pas dites jalan
    # beneran, sama seperti 2 fix Kafka di atas.
    anomaly_type_info = Types.ROW_NAMED(
        ["location_id", "window_end", "trip_count", "avg_fare", "is_anomaly"],
        [Types.INT(), Types.SQL_TIMESTAMP(), Types.LONG(), Types.DOUBLE(), Types.BOOLEAN()],
    )
    anomaly_stream = (
        ds.key_by(lambda r: r.pu_location_id)
          .window(TumblingEventTimeWindows.of(Time.minutes(15)))
          .process(AnomalyDetector(), output_type=anomaly_type_info)
    )
 
    # DataStream -> Table (balik lagi ke SQL buat sink)
    anomaly_table = t_env.from_data_stream(
        anomaly_stream,
        Schema.new_builder()
              .column("location_id", DataTypes.INT())
              .column("window_end", DataTypes.TIMESTAMP(3))
              .column("trip_count", DataTypes.BIGINT())
              .column("avg_fare", DataTypes.DOUBLE())
              .column("is_anomaly", DataTypes.BOOLEAN())
              .build()
    )
 
    t_env.create_temporary_view("anomaly_result", anomaly_table)
 
 
# ---------------------------------------------------------------------------
# 6. Main
# ---------------------------------------------------------------------------
def main():
    env, t_env = create_table_env()
 
    register_source_tables(t_env)
    register_sink_table(t_env)
 
    joined_table = build_joined_table(t_env)
    run_anomaly_detection(t_env, joined_table)
 
    t_env.execute_sql("""
        INSERT INTO traffic_anomaly_events
        SELECT location_id, window_end, trip_count, avg_fare, is_anomaly
        FROM anomaly_result
    """).wait()
 
 
if __name__ == "__main__":
    main()