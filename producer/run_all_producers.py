"""
Jalankan trip_events_producer.py dan weather_events_producer.py BARENGAN
dalam satu terminal, tanpa perlu buka 2 tab/window terpisah.

Cara kerja: masing-masing producer dijalankan apa adanya sebagai subprocess
Python terpisah (bukan di-import/digabung jadi satu proses) -- keduanya
memang didesain independen (masing-masing punya KafkaProducer, loop
tersendiri), jadi paling aman dijalankan persis seperti saat dipanggil
manual. Output tiap subprocess di-stream ke terminal ini dengan prefix
"[trip]"/"[weather]" biar kelihatan asalnya dari producer mana.

Jalankan:
    source .venv/bin/activate
    python producer/run_all_producers.py
    (Ctrl+C buat berhenti -- bakal matiin 2 producer-nya sekaligus)
"""

import subprocess
import sys
import threading
from pathlib import Path

HERE = Path(__file__).resolve().parent

PRODUCERS = [
    ("trip", HERE / "trip_events_producer.py"),
    ("weather", HERE / "weather_events_producer.py"),
]


def stream_output(prefix: str, proc: subprocess.Popen) -> None:
    for line in proc.stdout:
        print(f"[{prefix}] {line}", end="")


def main() -> None:
    procs = []
    for prefix, script in PRODUCERS:
        proc = subprocess.Popen(
            [sys.executable, str(script)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        procs.append((prefix, proc))

    threads = [
        threading.Thread(target=stream_output, args=(prefix, proc), daemon=True)
        for prefix, proc in procs
    ]
    for t in threads:
        t.start()

    print(f"Menjalankan {len(procs)} producer barengan (Ctrl+C buat berhenti semuanya)...\n")

    try:
        for _, proc in procs:
            proc.wait()
    except KeyboardInterrupt:
        print("\nMenghentikan semua producer...")
        for _, proc in procs:
            proc.terminate()
        for _, proc in procs:
            proc.wait()
        print("Dihentikan.")


if __name__ == "__main__":
    main()
