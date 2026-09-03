"""
Katalog connector Flink yang sudah "siap pakai" di project ini -- dipakai UI
(lihat static/index.html, panel "Daftar connector" di tab SQL) supaya user
bisa lihat contoh CREATE TABLE tanpa harus hafal syntax WITH (...) tiap
connector, lalu tinggal klik buat masukkan sebagai cell SQL baru.

Connector 'datagen', 'filesystem', 'print', 'blackhole' sudah kebundle di
pip install apache-flink (bagian dari flink-table-common), jadi selalu
"available". Connector 'kafka' & 'upsert-kafka' butuh JAR terpisah (lihat
jars/) -- keduanya SATU jar yang sama (flink-sql-connector-kafka), jadi
availability-nya dicek dari file JAR yang sama. Connector 'jdbc' butuh DUA
jar: flink-connector-jdbc (logic connector-nya) + driver JDBC sesuai
database-nya (di sini MySQL, karena .env sudah punya kredensial MySQL buat
Dinky) -- availability-nya baru true kalau DUA-DUANYA ada. Kalau JAR belum
didownload, tetap ditampilkan di UI (biar user tahu itu opsinya) tapi
ditandai belum tersedia dan tombol "pakai contoh"-nya dimatikan, lihat
hello_flink_kafka.py untuk cara download JAR-nya.

Tiap entry punya field 'kind' ('source' atau 'sink') -- dipakai UI buat
filter "Semua/Source/Sink" di panel katalog (lihat static/index.html), murni
informasi tampilan, tidak dipakai logic backend lain.

Template SQL sengaja TIDAK diberi komentar SQL (`-- ...`) -- run_job() di
flink_runner.py mendeteksi SELECT/INSERT dengan cek awal string statement
(lihat run_job()), jadi komentar di depan SELECT akan bikin salah rute
(dianggap statement biasa, hasilnya tidak pernah ditampilkan). Penjelasan
tiap connector cukup taruh di 'description', bukan di dalam SQL-nya.
"""

import os

from api.session_manager import PROJECT_DIR

KAFKA_JAR_PATH = os.path.join(PROJECT_DIR, "jars", "flink-sql-connector-kafka-1.17.2.jar")
JDBC_JAR_PATH = os.path.join(PROJECT_DIR, "jars", "flink-connector-jdbc-3.1.2-1.17.jar")
MYSQL_DRIVER_JAR_PATH = os.path.join(PROJECT_DIR, "jars", "mysql-connector-j-8.0.33.jar")
POSTGRES_DRIVER_JAR_PATH = os.path.join(PROJECT_DIR, "jars", "postgresql-42.7.4.jar")

_CATALOG = [
    {
        "key": "datagen",
        "label": "datagen (source)",
        "kind": "source",
        "description": "Generate data dummy otomatis -- tidak butuh file/topic asli. Paling gampang buat coba-coba dulu.",
        "builtin": True,
        "template": """CREATE TABLE datagen_source (
    id INT,
    nama STRING
) WITH (
    'connector' = 'datagen',
    'number-of-rows' = '5'
);

SELECT id, nama FROM datagen_source;""",
    },
    {
        "key": "filesystem-source",
        "label": "filesystem (source)",
        "kind": "source",
        "description": "Baca file CSV di disk sebagai table. Path relatif ke folder tempat uvicorn dijalankan (root project).",
        "builtin": True,
        "template": """CREATE TABLE filesystem_source (
    kalimat STRING
) WITH (
    'connector' = 'filesystem',
    'path' = 'data/kalimat.csv',
    'format' = 'csv'
);

SELECT kalimat FROM filesystem_source;""",
    },
    {
        "key": "filesystem-sink",
        "label": "filesystem (sink)",
        "kind": "sink",
        "description": "Tulis hasil query ke file CSV baru di disk. Lengkapi sendiri dengan INSERT INTO ... SELECT sesuai kebutuhan.",
        "builtin": True,
        "template": """CREATE TABLE filesystem_sink (
    kata STRING,
    jumlah BIGINT
) WITH (
    'connector' = 'filesystem',
    'path' = 'data/hasil_dari_notebook',
    'format' = 'csv'
);""",
    },
    {
        "key": "print",
        "label": "print (sink)",
        "kind": "sink",
        "description": "Cetak tiap baris ke stdout proses server (bukan ke browser) -- cek terminal uvicorn buat lihat isinya. Cocok buat debug cepat.",
        "builtin": True,
        "template": """CREATE TABLE print_sink (
    kata STRING,
    jumlah BIGINT
) WITH (
    'connector' = 'print'
);""",
    },
    {
        "key": "blackhole",
        "label": "blackhole (sink)",
        "kind": "sink",
        "description": "Buang semua data yang masuk, tidak ditulis ke mana pun -- cocok buat tes INSERT INTO ... SELECT tanpa peduli hasil akhirnya.",
        "builtin": True,
        "template": """CREATE TABLE blackhole_sink (
    kata STRING,
    jumlah BIGINT
) WITH (
    'connector' = 'blackhole'
);""",
    },
    {
        "key": "kafka-source",
        "label": "kafka (source)",
        "kind": "source",
        "description": "Baca dari topic Kafka. Broker & nama topic diambil dari .env lewat ${KAFKA_BOOTSTRAP_SERVERS}/${KAFKA_TOPIC}. 'scan.bounded.mode' sengaja diisi supaya preview SELECT di notebook ini berhenti sendiri, bukan menggantung selamanya.",
        "builtin": False,
        "template": """CREATE TABLE kafka_source (
    message STRING
) WITH (
    'connector' = 'kafka',
    'topic' = '${KAFKA_TOPIC}',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'pyflink-notebook',
    'scan.startup.mode' = 'earliest-offset',
    'scan.bounded.mode' = 'latest-offset',
    'format' = 'raw'
);

SELECT message FROM kafka_source;""",
    },
    {
        "key": "kafka-sink",
        "label": "kafka (sink)",
        "kind": "sink",
        "description": "Tulis hasil query ke topic Kafka lewat INSERT INTO ... SELECT. Broker & topic juga diambil dari .env.",
        "builtin": False,
        "template": """CREATE TABLE kafka_sink (
    message STRING
) WITH (
    'connector' = 'kafka',
    'topic' = '${KAFKA_TOPIC}',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'format' = 'raw'
);""",
    },
    {
        "key": "upsert-kafka",
        "label": "upsert-kafka (sink)",
        "kind": "sink",
        "description": (
            "Sama-sama Kafka, tapi khusus buat hasil query yang NILAINYA BERUBAH-UBAH "
            "(misal agregasi GROUP BY di streaming mode) -- 'kafka' biasa cuma bisa append "
            "(nolak kalau hasil query ada UPDATE/DELETE), 'upsert-kafka' butuh PRIMARY KEY "
            "dan menulis tiap perubahan sebagai pesan key-value (key = kolom PK, value = seluruh "
            "baris, atau tombstone/null value kalau baris itu ke-DELETE). Wajib isi 'key.format' "
            "DAN 'value.format' terpisah (beda dari 'kafka' yang cuma satu 'format'). "
            "Jar-nya SAMA dengan 'kafka' biasa, tidak perlu download tambahan."
        ),
        "builtin": False,
        "template": """CREATE TABLE upsert_kafka_sink (
    kata STRING,
    jumlah BIGINT,
    PRIMARY KEY (kata) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = '${KAFKA_TOPIC}',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'key.format' = 'json',
    'value.format' = 'json'
);""",
    },
    {
        "key": "jdbc-source",
        "label": "jdbc (source)",
        "kind": "source",
        "description": (
            "Baca dari tabel database relasional (contoh ini: MySQL, pakai kredensial "
            "MYSQL_* yang sama dengan setup Dinky di .env). PENTING: 'jdbc:mysql://${MYSQL_ADDR}/...' "
            "cuma bisa resolve MYSQL_ADDR='mysql:3306' kalau proses uvicorn ini jalan DI DALAM "
            "network Docker yang sama dengan container MySQL-nya -- kalau uvicorn jalan langsung "
            "di host (bukan di Docker), ganti MYSQL_ADDR ke 'localhost:<port_yang_dipublish>' dulu."
        ),
        "builtin": False,
        "template": """CREATE TABLE jdbc_source (
    id INT,
    nama STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MYSQL_ADDR}/${MYSQL_DATABASE}',
    'table-name' = 'nama_tabel_kamu',
    'username' = '${MYSQL_USERNAME}',
    'password' = '${MYSQL_PASSWORD}'
);

SELECT id, nama FROM jdbc_source;""",
    },
    {
        "key": "jdbc-source-postgres",
        "label": "jdbc (source, Postgres)",
        "kind": "source",
        "description": (
            "Baca dari tabel PostgreSQL, pakai kredensial POSTGRES_* di .env "
            "(punya kamu sendiri, misal Postgres local)."
        ),
        "builtin": False,
        "template": """CREATE TABLE jdbc_source_pg (
    id BIGINT,
    nama STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}',
    'table-name' = 'nama_tabel_kamu',
    'username' = '${POSTGRES_USER}',
    'password' = '${POSTGRES_PASSWORD}'
);

SELECT id, nama FROM jdbc_source_pg;""",
    },
    {
        "key": "jdbc-sink-postgres",
        "label": "jdbc (sink, Postgres)",
        "kind": "sink",
        "description": (
            "Tulis hasil query ke tabel PostgreSQL lewat INSERT INTO ... SELECT, pakai "
            "kredensial POSTGRES_* di .env. Tabelnya harus sudah ada duluan di Postgres "
            "(jdbc sink tidak auto-create tabel) -- lihat jobs/flink_sink_postgre/topic_to_postgre.py "
            "untuk contoh DDL-nya. Kalau sumber INSERT-nya dari Kafka (genuinely unbounded), "
            "submit lewat tombol Submit Job, JANGAN Run biasa -- Run biasa nunggu job selesai "
            "dan bakal menggantung selamanya."
        ),
        "builtin": False,
        "template": """CREATE TABLE jdbc_sink_pg (
    id BIGINT,
    nama STRING,
    nilai DOUBLE,
    event_time TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}',
    'table-name' = '${POSTGRES_TABLE}',
    'username' = '${POSTGRES_USER}',
    'password' = '${POSTGRES_PASSWORD}'
);""",
    },
    {
        "key": "jdbc-sink",
        "label": "jdbc (sink)",
        "kind": "sink",
        "description": (
            "Tulis hasil query ke tabel MySQL lewat INSERT INTO ... SELECT. PRIMARY KEY di DDL "
            "membuat sink ini upsert (UPDATE kalau key sudah ada) alih-alih selalu INSERT baru -- "
            "penting kalau sumbernya hasil agregasi yang nilainya berubah-ubah. Tabelnya harus "
            "sudah ada duluan di MySQL (jdbc sink tidak auto-create tabel)."
        ),
        "builtin": False,
        "template": """CREATE TABLE jdbc_sink (
    kata STRING,
    jumlah BIGINT,
    PRIMARY KEY (kata) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${MYSQL_ADDR}/${MYSQL_DATABASE}',
    'table-name' = 'hasil_wordcount',
    'username' = '${MYSQL_USERNAME}',
    'password' = '${MYSQL_PASSWORD}'
);""",
    },
]


def list_connectors() -> list[dict]:
    kafka_available = os.path.exists(KAFKA_JAR_PATH)
    jdbc_mysql_available = os.path.exists(JDBC_JAR_PATH) and os.path.exists(MYSQL_DRIVER_JAR_PATH)
    jdbc_postgres_available = os.path.exists(JDBC_JAR_PATH) and os.path.exists(POSTGRES_DRIVER_JAR_PATH)
    # kafka & upsert-kafka satu jar yang sama; jdbc-source/jdbc-sink butuh
    # connector jar + driver jar sekaligus (lihat docstring modul ini) --
    # MySQL dan Postgres punya driver jar beda, jadi dicek terpisah.
    NEEDS_KAFKA_JAR = {"kafka-source", "kafka-sink", "upsert-kafka"}
    NEEDS_JDBC_MYSQL_JAR = {"jdbc-source", "jdbc-sink"}
    NEEDS_JDBC_POSTGRES_JAR = {"jdbc-source-postgres", "jdbc-sink-postgres"}
    return [
        {
            **entry,
            "available": (
                entry["builtin"]
                or (entry["key"] in NEEDS_KAFKA_JAR and kafka_available)
                or (entry["key"] in NEEDS_JDBC_MYSQL_JAR and jdbc_mysql_available)
                or (entry["key"] in NEEDS_JDBC_POSTGRES_JAR and jdbc_postgres_available)
            ),
        }
        for entry in _CATALOG
    ]
