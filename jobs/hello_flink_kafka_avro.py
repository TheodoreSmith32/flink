"""
SOURCE dari Kafka, tapi pesannya di-encode Avro dan skemanya terdaftar di
Confluent Schema Registry -- lanjutan dari hello_flink_kafka.py yang masih
pakai format = 'raw' karena skema pesannya belum diketahui.

Bedanya dengan hello_flink_kafka.py:
- format = 'avro-confluent', bukan 'raw'. Flink akan tanya Schema
  Registry (lewat 'avro-confluent.schema-registry.url') buat tau skema
  ASLI si penulis pesan (writer schema), lalu deserialize byte Avro-nya
  jadi kolom-kolom SQL sesuai DDL di bawah.
- PENTING: Flink TIDAK auto-generate DDL dari Schema Registry. Kamu tetap
  harus menuliskan sendiri kolom & tipe SQL-nya di CREATE TABLE, dan itu
  HARUS cocok dengan skema Avro aslinya (nama field + tipe yang kompatibel).
  Jalankan dulu jobs/avro_schema_lookup.py buat lihat skema asli topic
  ini, baru sesuaikan kolom-kolom di bawah -- KOLOM DI BAWAH INI CUMA
  CONTOH PLACEHOLDER, ganti sesuai hasil lookup.
- Butuh JAR TAMBAHAN di luar connector Kafka biasa:
  jars/flink-sql-avro-confluent-registry-1.17.2.jar (didaftarkan sama-sama
  lewat 'pipeline.jars', dipisah koma). Ini fat jar dari Apache Flink
  sendiri (bukan dari Confluent), sudah termasuk client Avro + Schema
  Registry di dalamnya.
- SCHEMA_REGISTRY_URL diambil dari .env, sama alasannya dengan
  KAFKA_BOOTSTRAP_SERVERS -- alamat registry jangan ikut ke-commit.
- Contoh ini TANPA basic-auth ke registry. Kalau registry-mu butuh
  username/password, tambahkan dua opsi ini di WITH (...):
    'avro-confluent.basic-auth.credentials-source' = 'USER_INFO',
    'avro-confluent.basic-auth.user-info' = '${SCHEMA_REGISTRY_USER}:${SCHEMA_REGISTRY_PASSWORD}'

Skema di CREATE TABLE di bawah sudah disesuaikan dengan hasil
`python jobs/avro_schema_lookup.py` buat topic CDC_AVRO.FBNK_CUSTOMER --
ini format CDC dari Attunity/Qlik Replicate (Oracle -> Kafka), BUKAN field
customer flat:
    data:    ROW<RECID STRING, XMLRECORD STRING>
    headers: ROW<`timestamp` STRING>
Field-field customer asli (nama, dst) belum jadi kolom terpisah -- mereka
masih terbungkus string XML di dalam `data.XMLRECORD`. Parsing isi XML itu
jadi UDF/langkah berikutnya, di luar cakupan script ini yang baru sebatas
"berhasil consume + lihat bentuk mentahnya".

`headers.operation` SENGAJA TIDAK dicantumkan sebagai kolom: tipe aslinya
Avro ENUM, dan Flink SQL tidak punya tipe ENUM. Mendeklarasikannya sebagai
STRING bikin Flink membuat reader schema Avro "string" biasa, sedangkan
writer schema aslinya "enum" -- dan Avro TIDAK mengizinkan resolusi
enum -> string (beda dari int -> long dkk yang dipromosikan otomatis),
jadi gagal deserialize (AvroTypeException: "Found ...operation, expecting
string"). Fix-nya: field yang ada di writer schema tapi TIDAK dicantumkan
di reader schema (skema tabel ini) otomatis di-skip byte-nya oleh Avro,
tanpa error -- makanya field ini dihapus dari daftar kolom di bawah,
bukan diganti tipe lain. Kalau nilai operation-nya (INSERT/UPDATE/DELETE/
REFRESH) beneran dibutuhkan, itu perlu didekode manual di luar jalur
avro-confluent format ini (misal UDF yang membaca bytes mentah).

Jalankan:
    source .venv/bin/activate
    python jobs/hello_flink_kafka_avro.py
"""

import os

from dotenv import load_dotenv
from pyflink.table import EnvironmentSettings, TableEnvironment

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(base_dir, ".env"))

bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
topic = os.environ.get("KAFKA_TOPIC", "").strip()
schema_registry_url = os.environ.get("SCHEMA_REGISTRY_URL", "").strip()

if not bootstrap_servers:
    raise SystemExit("KAFKA_BOOTSTRAP_SERVERS masih kosong di .env.")
if not schema_registry_url:
    raise SystemExit(
        "SCHEMA_REGISTRY_URL masih kosong di .env. Isi dulu, misal:\n"
        "SCHEMA_REGISTRY_URL=http://schema-registry-host:8081"
    )

env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# Dua JAR sekaligus: connector Kafka + format avro-confluent. Dipisah koma,
# masing-masing tetap pakai prefix file://.
kafka_jar = os.path.join(base_dir, "jars", "flink-sql-connector-kafka-1.17.2.jar")
avro_jar = os.path.join(base_dir, "jars", "flink-sql-avro-confluent-registry-1.17.2.jar")
t_env.get_config().set("pipeline.jars", f"file://{kafka_jar};file://{avro_jar}")

# NOT NULL di sini bukan soal validasi data, tapi WAJIB supaya cocok dengan
# nullability skema Avro ASLI: kolom yang di skema Avro-nya TIDAK dibungkus
# union ["null", ...] (artinya field itu tidak pernah null di level Avro)
# harus dideklarasikan NOT NULL di Flink SQL juga -- kalau tidak, Flink
# membuat reader schema yang beda (mengharapkan union) dari writer schema
# aslinya (bukan union), dan Avro menolak baca (AvroTypeException: "Found
# ..., expecting union"). Dari hasil avro_schema_lookup.py: cuma
# data.RECID & data.XMLRECORD yang beneran nullable (["null","string"]);
# data, headers, headers.timestamp semuanya non-null (lihat juga catatan
# di docstring modul soal kenapa headers.operation TIDAK dicantumkan).
t_env.execute_sql(f"""
    CREATE TABLE kafka_source_avro (
        `data` ROW<RECID STRING, XMLRECORD STRING> NOT NULL,
        `headers` ROW<`timestamp` STRING NOT NULL> NOT NULL
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{topic}',
        'properties.bootstrap.servers' = '{bootstrap_servers}',
        'properties.group.id' = 'pyflink-belajar-avro',
        'scan.startup.mode' = 'earliest-offset',
        'scan.bounded.mode' = 'latest-offset',
        'format' = 'avro-confluent',
        'avro-confluent.schema-registry.url' = '{schema_registry_url}'
    )
""")

print(f"Membaca topic Avro '{topic}' dari {bootstrap_servers} ...\n")

# scan.bounded.mode = latest-offset di atas bikin job ini berhenti sendiri
# begitu sampai offset terakhir saat job dimulai -- cocok buat belajar/lihat
# preview, beda dari hello_flink_kafka.py yang genuinely unbounded (Ctrl+C).
# LIMIT 5 di sini aman dipakai justru KARENA sumbernya sudah dibatasi
# (scan.bounded.mode) -- kalau sumbernya genuinely unbounded (tanpa bounded
# mode), LIMIT saja TIDAK menjamin job berhenti (lihat catatan insiden di
# README.md soal SELECT unbounded yang nge-hang selamanya).
t_env.sql_query("SELECT * FROM kafka_source_avro LIMIT 5").execute().print()
