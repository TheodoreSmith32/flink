"""
Producer transaksi palsu buat demo fraud detection -- kirim JSON terus
menerus ke topic Kafka LOKAL (docker-compose.yml, bukan broker dev Bank
Sinarmas -- lihat KAFKA_FRAUD_BOOTSTRAP_SERVERS/KAFKA_FRAUD_TOPIC di .env,
sengaja dipisah dari KAFKA_BOOTSTRAP_SERVERS/KAFKA_TOPIC yang sudah dipakai
script lain di jobs/ biar tidak numpang di broker beneran).

Sebagian besar akun cuma bertransaksi normal (1 transaksi, jeda beberapa
detik). Sesekali (lihat BURST_CHANCE), satu akun dibuat "burst" -- beberapa
transaksi beruntun dalam beberapa detik dengan amount lebih besar -- ini yang
dimaksudkan supaya kena rule count-per-window di fraud_job.py.

Jalankan (setelah `docker compose up -d`):
    source .venv/bin/activate
    python usecases/fraud_detection/generate_transactions.py
"""

import json
import os
import random
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer

base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(base_dir, ".env"))

bootstrap_servers = os.environ.get("KAFKA_FRAUD_BOOTSTRAP_SERVERS", "localhost:9094").strip()
topic = os.environ.get("KAFKA_FRAUD_TOPIC", "fraud_transactions").strip()

ACCOUNTS = [f"acc-{i:03d}" for i in range(1, 21)]
MERCHANTS = ["toko-a", "toko-b", "warung-c", "ecommerce-d", "pulsa-e"]
BURST_CHANCE = 0.08  # per iterasi, kemungkinan satu akun tiba-tiba burst
BURST_SIZE = (6, 12)  # jumlah transaksi beruntun kalau burst kejadian

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers.split(","),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

next_id = 1


def make_txn(account_id: str, amount: float) -> dict:
    global next_id
    txn = {
        "transaction_id": next_id,
        "account_id": account_id,
        "amount": round(amount, 2),
        "merchant": random.choice(MERCHANTS),
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
    }
    next_id += 1
    return txn


def send(txn: dict):
    producer.send(topic, value=txn)
    print(f"-> {txn}")


print(f"Kirim transaksi ke topic '{topic}' @ {bootstrap_servers} (Ctrl+C untuk berhenti)\n")

try:
    while True:
        if random.random() < BURST_CHANCE:
            account_id = random.choice(ACCOUNTS)
            n = random.randint(*BURST_SIZE)
            print(f"[burst] {account_id} x{n}")
            for _ in range(n):
                send(make_txn(account_id, amount=random.uniform(500_000, 2_000_000)))
                time.sleep(0.3)
        else:
            account_id = random.choice(ACCOUNTS)
            send(make_txn(account_id, amount=random.uniform(10_000, 300_000)))
            time.sleep(random.uniform(1, 3))
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    producer.close()
