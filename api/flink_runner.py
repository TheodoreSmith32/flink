"""
Modul ini yang beneran "ngobrol" sama PyFlink. Dipanggil dari background task
FastAPI (lihat main.py), bukan langsung dari endpoint -- supaya
POST /sessions/{id}/jobs bisa langsung balas job_id tanpa nunggu Flink
selesai jalan.

Sejak ada session_manager.py, job history dan TableEnvironment TIDAK lagi
satu untuk seluruh proses: masing-masing Session (lihat
session_manager.Session) punya job store dan TableEnvironment sendiri.
Modul ini murni operasi atas sebuah Session yang dioper sebagai parameter --
tidak lagi pegang state global sendiri seperti versi sebelum ada session.
"""

import itertools
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum

from dotenv import load_dotenv
from pyflink.table import TableEnvironment

from api import session_manager
from api.session_manager import Session

load_dotenv(os.path.join(session_manager.PROJECT_DIR, ".env"))

# Berapa baris SELECT yang ditarik ke UI. Sengaja dibatasi supaya SELECT dari
# source unbounded (misal Kafka) tidak nge-block job lain di session yang
# sama selamanya -- lihat _run_select() di bawah.
PREVIEW_ROW_LIMIT = 20


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


def create_job(session: Session, sql: str) -> Job:
    job = Job(id=str(uuid.uuid4()), sql=sql)
    session.sql_jobs[job.id] = job
    return job


def get_job(session: Session, job_id: str) -> Job | None:
    return session.sql_jobs.get(job_id)


def list_jobs(session: Session) -> list[Job]:
    return sorted(session.sql_jobs.values(), key=lambda j: j.created_at, reverse=True)


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

    # Kunci biar session ini tidak macet kalau SELECT-nya dari source
    # unbounded (Kafka dkk): tarik maksimal PREVIEW_ROW_LIMIT baris lewat
    # islice, lalu TUTUP iterator-nya. Menutup iterator lebih awal juga
    # membatalkan job Flink di baliknya (sudah dites), jadi tidak ada job
    # nganggur yang numpuk di TableEnvironment session ini.
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


def _describe_table(t_env: TableEnvironment, name: str, kind: str) -> dict:
    """Introspeksi SATU table/view: kolom (nama+tipe) + DDL asli (lewat SHOW
    CREATE TABLE/VIEW). Dipakai bareng oleh list_tables() (semua table) dan
    describe_table() (satu table, dipanggil sebagai tool oleh LLM agent di
    llm_runner.generate_sql() -- lihat itu untuk konteks kenapa)."""
    columns = []
    try:
        schema = t_env.from_path(name).get_schema()
        columns = [
            {"name": field_name, "type": str(field_type)}
            for field_name, field_type in zip(
                schema.get_field_names(), schema.get_field_data_types()
            )
        ]
    except Exception:
        pass

    # SHOW CREATE TABLE vs SHOW CREATE VIEW beda statement di Flink --
    # yang satu dipanggil ke object jenis lain langsung error.
    ddl = None
    try:
        show_stmt = "SHOW CREATE VIEW" if kind == "VIEW" else "SHOW CREATE TABLE"
        rows = list(t_env.execute_sql(f"{show_stmt} `{name}`").collect())
        if rows:
            ddl = rows[0][0]
    except Exception:
        pass

    return {"name": name, "kind": kind, "columns": columns, "ddl": ddl}


def list_tables(session: Session) -> list[dict]:
    """Daftar semua table & view yang ada di TableEnvironment session ini,
    lengkap dengan kolom (nama+tipe) dan DDL aslinya -- dipakai panel "Tabel
    di session ini" di UI biar user gampang inget apa aja yang sudah dibuat,
    tanpa harus nulis SHOW TABLES / DESCRIBE manual di cell.

    Dipanggil langsung dari endpoint (bukan lewat job_id + polling seperti
    run_job()) karena baca metadata table itu cepat -- beda dengan
    menjalankan SQL/Python yang bisa makan waktu.
    """
    t_env = session_manager.get_env(session)
    view_names = set(t_env.list_views())
    entries = [
        _describe_table(t_env, name, "VIEW" if name in view_names else "TABLE")
        for name in t_env.list_tables()
    ]
    return sorted(entries, key=lambda e: e["name"])


def describe_table(session: Session, name: str) -> dict | None:
    """Introspeksi satu table/view by name, atau None kalau tidak ada.
    Dipanggil dari llm_runner.generate_sql()'s `describe_table` tool -- lewat
    ini agent LLM bisa cek skema ASLI di session sebelum nulis SQL, bukan
    menebak dari instruksi umum di system prompt. Caller (llm_runner) yang
    pegang session.lock, bukan fungsi ini."""
    t_env = session_manager.get_env(session)
    if name not in t_env.list_tables():
        return None
    view_names = set(t_env.list_views())
    return _describe_table(t_env, name, "VIEW" if name in view_names else "TABLE")


def preview_select(session: Session, sql: str, limit: int = 5) -> dict:
    """SELECT terbatas (read-only) yang dipanggil dari llm_runner.generate_sql()'s
    `preview_rows` tool, supaya agent LLM bisa lihat CONTOH data asli sebelum
    menjawab -- bukan cuma nama kolom. Sengaja menolak statement selain SELECT
    (agent generate-SQL ini cuma boleh membaca, tidak pernah mengubah session).
    Caller (llm_runner) yang pegang session.lock, bukan fungsi ini."""
    if not sql.strip().upper().startswith("SELECT"):
        raise ValueError("preview_rows cuma boleh dipakai untuk statement SELECT")

    t_env = session_manager.get_env(session)
    result = t_env.execute_sql(sql)
    columns = result.get_table_schema().get_field_names()

    iterator = result.collect()
    try:
        rows = list(itertools.islice(iterator, min(limit, PREVIEW_ROW_LIMIT)))
    finally:
        iterator.close()

    return {"columns": columns, "rows": [list(row) for row in rows]}


def run_job(session: Session, job_id: str) -> None:
    """Dipanggil sebagai FastAPI BackgroundTask -- fungsi sync biasa, supaya
    Starlette menjalankannya di threadpool dan tidak memblokir event loop
    yang melayani request lain."""
    job = session.sql_jobs[job_id]
    job.status = JobStatus.RUNNING

    with session.lock:
        try:
            t_env = session_manager.get_env(session)
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
