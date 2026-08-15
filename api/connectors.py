"""
Katalog connector Flink yang sudah "siap pakai" di project ini -- dipakai UI
(lihat static/index.html, panel "Daftar connector" di tab SQL) supaya user
bisa lihat contoh CREATE TABLE tanpa harus hafal syntax WITH (...) tiap
connector, lalu tinggal klik buat masukkan sebagai cell SQL baru.

Connector 'datagen', 'filesystem', 'print', 'blackhole' sudah kebundle di
pip install apache-flink (bagian dari flink-table-common), jadi selalu
"available". Connector 'kafka' butuh JAR terpisah (lihat jars/), makanya
availability-nya dicek dari keberadaan file JAR itu -- kalau JAR belum
didownload, tetap ditampilkan di UI (biar user tahu itu opsinya) tapi
ditandai belum tersedia dan tombol "pakai contoh"-nya dimatikan, lihat
hello_flink_kafka.py untuk cara download JAR-nya.

Template SQL sengaja TIDAK diberi komentar SQL (`-- ...`) -- run_job() di
flink_runner.py mendeteksi SELECT/INSERT dengan cek awal string statement
(lihat run_job()), jadi komentar di depan SELECT akan bikin salah rute
(dianggap statement biasa, hasilnya tidak pernah ditampilkan). Penjelasan
tiap connector cukup taruh di 'description', bukan di dalam SQL-nya.
"""

import os

from api.session_manager import PROJECT_DIR

KAFKA_JAR_PATH = os.path.join(PROJECT_DIR, "jars", "flink-sql-connector-kafka-1.17.2.jar")

_CATALOG = [
    {
        "key": "datagen",
        "label": "datagen (source)",
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
]


def list_connectors() -> list[dict]:
    kafka_available = os.path.exists(KAFKA_JAR_PATH)
    return [
        {**entry, "available": entry["builtin"] or kafka_available}
        for entry in _CATALOG
    ]
