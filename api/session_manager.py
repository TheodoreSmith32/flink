"""
Session = satu "kernel" notebook sendiri-sendiri, mirip kernel Jupyter.
Sebelum bisa pakai tab SQL/Python di UI, user harus bikin (atau pilih) session
dulu lewat endpoint /sessions (lihat main.py). Tiap Session punya
TableEnvironment, job history (SQL & Python), dan namespace Python (buat
notebook Python) SENDIRI -- tabel yang dibuat di session A tidak pernah
kelihatan dari session B.

Ini beda dari desain awal yang cuma punya SATU TableEnvironment untuk
seluruh proses FastAPI (semua orang/tab berbagi state yang sama).
Konsekuensinya: bikin session sekarang ada "harga"-nya -- tiap session
pegang TableEnvironment (resource Flink) sendiri -- makanya dibatasi
SESSION_LIMIT, bukan tanpa batas, supaya tidak kebablasan bikin banyak
TableEnvironment sekaligus dan menghabiskan memori.

Sengaja TIDAK dipersist ke disk: daftar session hilang begitu proses
FastAPI di-restart, sama seperti job history sebelumnya -- konsisten
dengan desain "notebook embedded" yang sudah ada, bukan servis
multi-user/produksi.
"""

import os
import threading
import time
import uuid
from dataclasses import dataclass, field

from pyflink.table import EnvironmentSettings, TableEnvironment

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Maksimal session aktif sekaligus -- tiap session pegang TableEnvironment
# sendiri, jadi ini sekaligus jadi batas atas berapa banyak resource Flink
# yang bisa hidup bersamaan di proses ini.
SESSION_LIMIT = 5


@dataclass
class Session:
    id: str
    name: str
    # "streaming" (default) atau "batch" -- menentukan EnvironmentSettings yang dipakai
    # saat session.env dibuat lazy di get_env(). Tidak bisa diganti setelah session.env
    # dibuat (TableEnvironment tidak bisa ganti mode di tengah jalan) -- kalau butuh mode
    # lain, buat session baru.
    mode: str = "streaming"
    created_at: float = field(default_factory=time.time)
    # Lock per-session (bukan satu lock global) -- job di session A tidak
    # perlu nunggu job di session B, karena keduanya pegang TableEnvironment
    # yang berbeda.
    lock: threading.Lock = field(default_factory=threading.Lock)
    env: TableEnvironment | None = field(default=None, repr=False)
    sql_jobs: dict = field(default_factory=dict)
    py_jobs: dict = field(default_factory=dict)
    # Job Flink yang dimaksudkan jalan SELAMANYA di background (lihat
    # background_jobs.py) -- berbeda dari sql_jobs/py_jobs di atas yang
    # statusnya cepat selesai.
    background_jobs: dict = field(default_factory=dict)
    # Cell Python yang dimaksudkan jalan SELAMANYA di background thread
    # (lihat python_background_jobs.py) -- analog background_jobs di atas,
    # tapi mekanisme stop-nya kooperatif (bukan cancel() yang dijamin Flink).
    python_background_jobs: dict = field(default_factory=dict)
    # Namespace exec() buat notebook Python, persist antar cell -- lihat
    # python_runner.py. None sampai cell Python pertama di session ini jalan.
    py_globals: dict | None = field(default=None, repr=False)


_SESSIONS: dict[str, Session] = {}
_SESSIONS_LOCK = threading.Lock()


def create_session(name: str, mode: str = "streaming") -> Session:
    if mode not in ("streaming", "batch"):
        raise ValueError("mode harus 'streaming' atau 'batch'")
    with _SESSIONS_LOCK:
        if len(_SESSIONS) >= SESSION_LIMIT:
            raise ValueError(
                f"Maksimal {SESSION_LIMIT} session aktif sekaligus. Hapus session lama dulu."
            )
        session = Session(id=str(uuid.uuid4()), name=name, mode=mode)
        _SESSIONS[session.id] = session
        return session


def get_session(session_id: str) -> Session | None:
    return _SESSIONS.get(session_id)


def list_sessions() -> list[Session]:
    return sorted(_SESSIONS.values(), key=lambda s: s.created_at, reverse=True)


def delete_session(session_id: str) -> bool:
    with _SESSIONS_LOCK:
        return _SESSIONS.pop(session_id, None) is not None


def get_env(session: Session) -> TableEnvironment:
    """TableEnvironment milik SATU session, dibuat sekali (lazy) lalu
    dipakai ulang untuk semua job SQL/Python di session itu. Dipanggil dari
    flink_runner.py dan python_runner.py supaya keduanya berbagi env yang
    sama di dalam satu session -- tapi tidak pernah dengan session lain."""
    if session.env is None:
        settings = (
            EnvironmentSettings.in_batch_mode()
            if session.mode == "batch"
            else EnvironmentSettings.in_streaming_mode()
        )
        session.env = TableEnvironment.create(settings)
        # Daftarkan semua connector JAR yang tersedia dari awal (kafka,
        # jdbc + driver MySQL) sama seperti di hello_flink_kafka.py, biar
        # session ini bisa langsung CREATE TABLE ... WITH ('connector'=...)
        # tanpa langkah tambahan -- lihat api/connectors.py untuk daftar
        # connector & jar yang dicek availability-nya. Kafka & upsert-kafka
        # pakai jar yang sama; jdbc butuh connector jar + driver jar
        # sekaligus (dua-duanya harus ada baru didaftarkan).
        jar_names = [
            "flink-sql-connector-kafka-1.17.2.jar",
            "flink-connector-jdbc-3.1.2-1.17.jar",
            "mysql-connector-j-8.0.33.jar",
        ]
        jar_paths = [os.path.join(PROJECT_DIR, "jars", name) for name in jar_names]
        existing_jars = [p for p in jar_paths if os.path.exists(p)]
        if existing_jars:
            session.env.get_config().set(
                "pipeline.jars", ";".join(f"file://{p}" for p in existing_jars)
            )
    return session.env
