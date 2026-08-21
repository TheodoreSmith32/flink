"""
Bantu lihat skema Avro asli dari Confluent Schema Registry SEBELUM nulis
CREATE TABLE Table API -- soalnya format 'avro-confluent' di Flink cuma
mem-validasi/deserialize pesan sesuai skema yang sudah terdaftar, TAPI kamu
tetap harus mendeklarasikan sendiri kolom-kolom tabelnya di DDL (Flink tidak
auto-generate DDL dari Schema Registry). Jadi alih-alih nebak field-nya,
script ini nge-fetch skema aslinya lewat REST API registry, biar DDL yang
ditulis di hello_flink_kafka_avro.py cocok 100% dengan field & tipe asli.

Subject di Confluent Schema Registry biasanya bernama "{topic}-value"
(TopicNameStrategy, default paling umum). Kalau topic-mu didaftarkan pakai
RecordNameStrategy (subject = nama record Avro, bukan nama topic), lookup
"{topic}-value" akan 404 -- script ini fallback dengan list semua subject
dan cari yang mirip nama topic-nya, supaya kamu bisa pilih manual.

Cuma pakai urllib bawaan Python (bukan `requests`) biar tidak nambah
dependency baru cuma buat sekali lookup skema.

Jalankan:
    source .venv/bin/activate
    python jobs/avro_schema_lookup.py
"""

import json
import os
import urllib.error
import urllib.request

from dotenv import load_dotenv

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(base_dir, ".env"))

registry_url = os.environ.get("SCHEMA_REGISTRY_URL", "").strip().rstrip("/")
topic = os.environ.get("KAFKA_TOPIC", "").strip()

if not registry_url:
    raise SystemExit(
        "SCHEMA_REGISTRY_URL masih kosong di .env. Isi dulu, misal:\n"
        "SCHEMA_REGISTRY_URL=http://schema-registry-host:8081"
    )
if not topic:
    raise SystemExit("KAFKA_TOPIC masih kosong di .env.")


def _get(path: str):
    with urllib.request.urlopen(f"{registry_url}{path}", timeout=10) as resp:
        return json.loads(resp.read())


subject = f"{topic}-value"
try:
    latest = _get(f"/subjects/{subject}/versions/latest")
except urllib.error.HTTPError as exc:
    if exc.code != 404:
        raise
    print(f"Subject '{subject}' tidak ditemukan (404). Mungkin registry-nya pakai ")
    print("RecordNameStrategy, bukan TopicNameStrategy. Subject yang tersedia:\n")
    for s in _get("/subjects"):
        print(f"  - {s}")
    print(f"\nCari yang cocok dengan topic '{topic}', lalu jalankan lagi script ini")
    print("dengan subject itu (ganti baris `subject = ...` di atas).")
    raise SystemExit(1)

schema = json.loads(latest["schema"])  # skema Avro asli, dalam bentuk dict
print(f"Subject   : {subject}")
print(f"Version   : {latest['version']}  (schema id: {latest['id']})")
print(f"Record    : {schema.get('name')} (namespace: {schema.get('namespace')})\n")

print("Field-field-nya (nama -> tipe Avro):")
for field in schema.get("fields", []):
    print(f"  - {field['name']}: {field['type']}")

print("\nSkema Avro mentah (buat dicocokkan manual ke tipe SQL Flink):")
print(json.dumps(schema, indent=2, ensure_ascii=False))
