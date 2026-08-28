"""
Contoh event time & watermark (Table API/SQL) -- roadmap #10 di README.

CATATAN PENTING soal kenapa ini Table API/SQL, BUKAN DataStream API kayak
jobs/hackatown/flink_sql_01.py: awalnya contoh ini ditulis pakai DataStream
API murni (source dari list Python + WatermarkStrategy.for_bounded_out_of_
orderness dengan TimestampAssigner Python custom), supaya gak perlu Kafka
sama sekali. Tapi setelah BENERAN dites jalan, ketemu keanehan nyata:
periodic watermark generator-nya TIDAK PERNAH "nge-tick" di tengah proses
sekalipun sudah dikasih jeda time.sleep() asli antar-event DAN diset
python.execution-mode=thread + pipeline.auto-watermark-interval=50ms --
watermark cuma "meloncat" sekali pas source-nya mau selesai (dibuktikan
lewat print context.current_watermark() di ProcessWindowFunction: window
[0,20) dan [20,40) yang seharusnya nutup di waktu BERBEDA malah nutup
BARENGAN, sama-sama di watermark 39999, persis sebelum event terakhir
diproses). Dugaan kuat: watermark generator utk TimestampAssigner custom
Python di DataStream API PyFlink 1.17.2 gak jalan di scheduled JVM timer
yang independen, jadi gak bisa dipakai buat demo "late event di-drop" yang
kredibel dari source Python biasa.

Makanya dipindah ke pendekatan yang SAMA seperti fraud_job.py dan
flink_sql_01.py yang sudah TERBUKTI jalan: `WATERMARK FOR ... AS ...` di
DDL Table API/SQL. Watermark generator-nya di sini 100% native Java (bukan
lewat callback Python), jadi gak kena masalah di atas -- dan karena
sumbernya Kafka beneran (unbounded), gap waktu asli antar pesan (diatur di
producer, watermark_demo_producer.py) bikin efek "late event di-drop"
kelihatan sungguhan, bukan cuma teori.

Alur demo:
    watermark_demo_producer.py mengirim 6 event bernomor sensor "sensor-1"
    ke topic Kafka lokal (docker-compose.yml, port 9094), SATU di antaranya
    SENGAJA dikirim telat (lihat komentar EVENTS di watermark_demo_producer.py).
    Job ini baca topic itu, window TUMBLE 20 detik, dan nge-print jumlah +
    daftar value tiap window begitu window itu ditutup oleh watermark.
    Event yang telat parah TIDAK akan muncul di window manapun -- itu bukti
    dia di-drop.

SEBELUM JALANKAN (butuh docker-compose.yml yang sudah ada, dipakai bareng
demo fraud detection -- topic-nya beda jadi gak numpuk sama data fraud):
    docker compose up -d
    (di terminal lain) python producer/watermark_demo_producer.py

Jalankan:
    source .venv/bin/activate
    JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 python jobs/hello_flink_watermark.py
    (Ctrl+C buat berhenti -- ini genuinely unbounded, sama seperti
    hello_flink_kafka.py)
"""

import os

from dotenv import load_dotenv
from pyflink.table import EnvironmentSettings, TableEnvironment

# HANYA satu dirname dari jobs/hello_flink_watermark.py ke root repo --
# BEDA dari jobs/hello_flink_kafka.py yang cuma pakai
# os.path.dirname(os.path.abspath(__file__)) TANPA naik satu level lagi,
# jadi diam-diam nyari jobs/.env yang gak pernah ada (load_dotenv gagal
# senyap, .env harus sudah ke-export manual di shell biar itu masih jalan).
# Ketemu pas nulis file ini -- lihat catatan yang sama di README kalau mau
# ikut dibenerin di sana.
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(base_dir, ".env"))

# Broker & topic LOKAL (docker-compose.yml) -- SENGAJA bukan
# KAFKA_BOOTSTRAP_SERVERS/KAFKA_TOPIC (itu broker dev Bank Sinarmas
# beneran), sama alasannya dengan KAFKA_FRAUD_BOOTSTRAP_SERVERS di
# usecases/fraud_detection/fraud_job.py -- data belajar jangan numpang di
# broker beneran.
bootstrap_servers = os.environ.get("KAFKA_LOCAL_BOOTSTRAP_SERVERS", "localhost:9094").strip()
topic = os.environ.get("KAFKA_WATERMARK_TOPIC", "watermark_demo").strip()

WINDOW_SECONDS = 20
OUT_OF_ORDERNESS_SECONDS = 5


def create_table_env() -> TableEnvironment:
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    kafka_jar = os.path.join(base_dir, "jars", "flink-sql-connector-kafka-1.17.2.jar")
    t_env.get_config().set("pipeline.jars", f"file://{kafka_jar}")

    # WAJIB -- topic ini dibuat via KAFKA_AUTO_CREATE_TOPICS_ENABLE jadi
    # cuma 1 partition. Sama seperti catatan panjang di fraud_job.py:
    # TableEnvironment default parallelism = jumlah CPU core, dan watermark
    # gabungan Flink = MINIMUM dari SEMUA source subtask paralel (termasuk
    # yang idle karena gak kebagian partition) -- tanpa baris ini, window
    # TUMBLE di bawah gak akan PERNAH nutup di topic 1-partition.
    t_env.get_config().set("parallelism.default", "1")
    return t_env


def register_source(t_env: TableEnvironment):
    t_env.execute_sql(f"""
        CREATE TABLE watermark_demo_source (
            sensor_id STRING,
            value_ INT,
            event_time TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '{OUT_OF_ORDERNESS_SECONDS}' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic}',
            'properties.bootstrap.servers' = '{bootstrap_servers}',
            'properties.group.id' = 'pyflink-watermark-demo',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)
    # Catatan: kolom dinamai `value_` (bukan `value`) karena VALUE adalah
    # reserved keyword di Flink SQL -- coba pakai `value` polos dan
    # CREATE TABLE ini bakal gagal ParseException, sama kelasnya dengan
    # kasus `condition` yang harus dibungkus backtick di flink_sql_01.py.


def main():
    t_env = create_table_env()
    register_source(t_env)

    print(f"Membaca topic '{topic}' dari {bootstrap_servers} (Ctrl+C untuk berhenti)")
    print(f"Window TUMBLE {WINDOW_SECONDS}s, out-of-orderness {OUT_OF_ORDERNESS_SECONDS}s.")
    print("Cek: apakah event value=5 (dikirim telat oleh producer) muncul di")
    print("salah satu window di bawah? Kalau TIDAK muncul sama sekali -- itu")
    print("buktinya dia berhasil di-drop oleh watermark.\n")

    t_env.sql_query(f"""
        SELECT
            sensor_id,
            window_start,
            window_end,
            COUNT(*) AS jumlah_event,
            SUM(value_) AS total_value,
            LISTAGG(CAST(value_ AS STRING), ',') AS values_
        FROM TABLE(
            TUMBLE(TABLE watermark_demo_source, DESCRIPTOR(event_time), INTERVAL '{WINDOW_SECONDS}' SECOND)
        )
        GROUP BY sensor_id, window_start, window_end
    """).execute().print()


if __name__ == "__main__":
    main()
