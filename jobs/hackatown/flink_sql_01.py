
import os

from dotenv import load_dotenv
from pyflink.table import EnvironmentSettings, TableEnvironment

base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(base_dir, ".env"))

bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
topic = os.environ.get("KAFKA_TOPIC", "").strip()

 
# ---------------------------------------------------------------------------
# 1. Setup environment
# ---------------------------------------------------------------------------
def create_table_env():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # cukup 1 buat demo, naikkan kalau perlu
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
            'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
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
            condition STRING,
            precipitation DOUBLE,
            WATERMARK FOR event_time AS event_time - INTERVAL '5' MINUTE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'weather_events',
            'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
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
            'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
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
            w.condition,
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
 
    anomaly_stream = (
        ds.key_by(lambda r: r.pu_location_id)
          .window(TumblingEventTimeWindows.of(Time.minutes(15)))
          .process(AnomalyDetector())
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