"""
Modul ini yang beneran "ngobrol" sama PyFlink. Dipanggil dari background task
FastAPI (lihat main.py), bukan langsung dari endpoint -- supaya POST /jobs
bisa langsung balas job_id tanpa nunggu Flink selesai jalan.

Dipakai ala NOTEBOOK: satu TableEnvironment (_ENV) dibuat SEKALI lalu dipakai
bersama untuk SEMUA job -- bukan bikin baru tiap job seperti versi awal.
Ini supaya tabel yang dibuat di satu submission ("cell" di UI) tetap kepakai
di submission berikutnya, sama seperti Jupyter/Zeppelin. Konsekuensinya:
semua job dieksekusi SATU PER SATU lewat LOCK, karena satu TableEnvironment
yang sama dipakai bareng-bareng dan tidak aman dieksekusi dari banyak
thread sekaligus.

`get_env()` dan `LOCK` sengaja diekspos publik (bukan `_get_env`/`_LOCK`)
karena dipakai bareng oleh python_runner.py (notebook Python) -- supaya
notebook SQL dan notebook Python berbagi TableEnvironment yang sama DAN
tidak pernah mengeksekusi ke sana secara bersamaan dari dua thread.
"""

import itertools
import os
import re
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum

from dotenv import load_dotenv
from pyflink.table import EnvironmentSettings, TableEnvironment

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(PROJECT_DIR, ".env"))

# Berapa baris SELECT yang ditarik ke UI. Sengaja dibatasi supaya SELECT dari
# source unbounded (misal Kafka) tidak nge-block job lain selamanya -- lihat
# _run_select() di bawah.
PREVIEW_ROW_LIMIT = 20

LOCK = threading.Lock()
_JOBS: dict[str, "Job"] = {}
_ENV = None


class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


@dataclass
class Job:
    id: str
    sql: str
    status: JobStatus = JobStatus.PENDING
    columns: list[str] | None = None
    rows: list[list] | None = None
    truncated: bool = False
    error: str | None = None
    created_at: float = field(default_factory=time.time)
    finished_at: float | None = None


def create_job(sql: str) -> Job:
    job = Job(id=str(uuid.uuid4()), sql=sql)
    _JOBS[job.id] = job
    return job


def get_job(job_id: str) -> Job | None:
    return _JOBS.get(job_id)


def list_jobs() -> list[Job]:
    return sorted(_JOBS.values(), key=lambda j: j.created_at, reverse=True)


def get_env() -> TableEnvironment:
    """Satu TableEnvironment untuk seumur hidup proses FastAPI -- dibuat
    sekali saja (lazy), dipakai ulang di tiap run_job(). Diekspos publik
    (tanpa underscore) karena python_runner.py (notebook Python) juga
    pakai TableEnvironment yang SAMA lewat fungsi ini, biar tabel yang
    dibuat lewat notebook SQL bisa dipakai dari notebook Python juga."""
    global _ENV
    if _ENV is None:
        _ENV = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        # Daftarkan connector Kafka dari awal, sama seperti di
        # hello_flink_kafka.py, biar notebook ini juga bisa CREATE TABLE
        # ... WITH ('connector'='kafka', ...) tanpa langkah tambahan.
        jar_path = os.path.join(PROJECT_DIR, "jars", "flink-sql-connector-kafka-1.17.2.jar")
        if os.path.exists(jar_path):
            _ENV.get_config().set("pipeline.jars", f"file://{jar_path}")
    return _ENV


_ENV_VAR_PATTERN = re.compile(r"\$\{(\w+)\}")


def _expand_env_vars(sql: str) -> str:
    """Ganti placeholder ${NAMA_VAR} dengan nilainya dari .env. Dipakai
    supaya hal-hal sensitif (misal alamat broker Kafka) tidak perlu diketik
    langsung di textarea browser -- cukup tulis
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}' dan nilai
    aslinya diambil dari .env saat dieksekusi di server."""

    def replace(match: re.Match) -> str:
        name = match.group(1)
        value = os.environ.get(name)
        if value is None:
            raise ValueError(f"Env var '{name}' tidak ditemukan di .env")
        return value

    return _ENV_VAR_PATTERN.sub(replace, sql)


def _split_statements(sql: str) -> list[str]:
    # Pemisah statement paling sederhana: titik koma. Belum handle titik koma
    # yang muncul di dalam string literal -- cukup buat belajar dulu.
    return [s.strip() for s in sql.split(";") if s.strip()]


def _run_select(t_env: TableEnvironment, statement: str, job: "Job") -> None:
    result = t_env.execute_sql(statement)
    job.columns = result.get_table_schema().get_field_names()

    # Kunci biar notebook ini tidak macet kalau SELECT-nya dari source
    # unbounded (Kafka dkk): tarik maksimal PREVIEW_ROW_LIMIT baris lewat
    # islice, lalu TUTUP iterator-nya. Menutup iterator lebih awal juga
    # membatalkan job Flink di baliknya (sudah dites), jadi tidak ada job
    # nganggur yang numpuk di TableEnvironment yang dipakai bersama ini.
    iterator = result.collect()
    try:
        rows = list(itertools.islice(iterator, PREVIEW_ROW_LIMIT))
    finally:
        iterator.close()

    job.rows = [list(row) for row in rows]
    # Heuristik sederhana: kalau baris yang didapat pas sejumlah limit,
    # anggap saja masih ada baris lain yang belum ditarik (bisa saja pas
    # kebetulan itu baris terakhir, tapi untuk notebook belajar ini cukup).
    job.truncated = len(rows) == PREVIEW_ROW_LIMIT


def run_job(job_id: str) -> None:
    """Dipanggil sebagai FastAPI BackgroundTask -- fungsi sync biasa, supaya
    Starlette menjalankannya di threadpool dan tidak memblokir event loop
    yang melayani request lain."""
    job = _JOBS[job_id]
    job.status = JobStatus.RUNNING

    with LOCK:
        try:
            t_env = get_env()
            statements = _split_statements(_expand_env_vars(job.sql))

            for statement in statements:
                upper = statement.strip().upper()
                if upper.startswith("SELECT"):
                    _run_select(t_env, statement, job)
                elif upper.startswith("INSERT"):
                    # INSERT INTO async secara default -- .wait() biar job
                    # ditandai selesai setelah datanya BENAR-BENAR tertulis,
                    # bukan cuma setelah job disubmit ke Flink.
                    t_env.execute_sql(statement).wait()
                else:
                    # CREATE TABLE, USE, dst sudah synchronous begitu
                    # execute_sql() kembali, tidak perlu tindakan tambahan.
                    t_env.execute_sql(statement)

            job.status = JobStatus.SUCCESS
        except Exception as exc:
            job.status = JobStatus.FAILED
            job.error = str(exc)
        finally:
            job.finished_at = time.time()
