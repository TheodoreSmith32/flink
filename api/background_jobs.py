"""
Background job = job Flink yang DIMAKSUDKAN jalan SELAMANYA di belakang layar
(misal: INSERT INTO ... SELECT dari source unbounded seperti Kafka), beda
dari job notebook biasa di flink_runner.py yang statusnya cepat selesai
(SUCCESS/FAILED dalam hitungan detik). Modul ini TIDAK PERNAH memanggil
.wait() -- kalau dipanggil ke situ, proses submit-nya sendiri yang tidak akan
pernah balik untuk job yang memang tidak berhenti.

Mekanismenya ternyata sudah disediakan Flink sendiri tanpa perlu cluster
beneran: setiap `execute_sql()`/`StatementSet.execute()` untuk INSERT
balikin `TableResult.get_job_client()` -- objek ini punya Job ID asli,
`get_job_status()`, dan `cancel()`, PERSIS seperti yang dipakai Flink Web
UI/Job Manager sungguhan. Modul ini cuma membungkus itu supaya bisa dilacak
per session dan ditampilkan di UI (panel "Background Jobs").

PERINGATAN (tampilkan juga di UI): ini masih mode EMBEDDED -- TableEnvironment
tiap session hidup DI DALAM proses FastAPI ini, bukan Flink session cluster
terpisah. Kalau proses uvicorn mati/di-restart, SEMUA background job di sini
ikut mati seketika. "Job Manager" di sini nama fitur, bukan klaim persistence
tingkat cluster sungguhan (itu baru dijembatani nanti oleh setup Dinky +
Flink cluster yang masih di roadmap, lihat README.md).

Sengaja dibatasi ke statement INSERT INTO ... SELECT (SQL) saja, TIDAK untuk
cell Python: loop Python (`while True: ...` lewat exec()) tidak punya
mekanisme cancel yang aman seperti job_client.cancel() -- beda dengan job
Flink asli yang memang didesain buat itu.
"""

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum

from api import flink_runner, session_manager
from api.session_manager import Session


class BackgroundJobStatus(str, Enum):
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
    UNKNOWN = "UNKNOWN"


@dataclass
class BackgroundJob:
    id: str
    name: str
    sql: str
    status: BackgroundJobStatus = BackgroundJobStatus.RUNNING
    flink_job_id: str | None = None
    error: str | None = None
    submitted_at: float = field(default_factory=time.time)
    # JobClient asli dari Flink -- TIDAK diserialisasi ke JSON (lihat to_dict()),
    # cuma dipakai internal buat cek status/cancel.
    job_client: object | None = field(default=None, repr=False, compare=False)


def _split_statements(sql: str) -> list[str]:
    return [s.strip() for s in sql.split(";") if s.strip()]


def _map_status(raw_status) -> BackgroundJobStatus:
    name = str(raw_status).upper()
    if "RUNNING" in name or "CREATED" in name or "RESTARTING" in name or "INITIALIZING" in name:
        return BackgroundJobStatus.RUNNING
    if "FINISHED" in name:
        return BackgroundJobStatus.FINISHED
    if "CANCEL" in name:
        return BackgroundJobStatus.CANCELED
    if "FAILED" in name:
        return BackgroundJobStatus.FAILED
    return BackgroundJobStatus.UNKNOWN


def _refresh_status(job: BackgroundJob) -> None:
    # Job yang gagal submit dari awal (job_client sendiri gak ada), atau yang
    # statusnya sudah final (bukan RUNNING lagi), gak perlu -- dan sering kali
    # gak BISA -- dicek ulang ke Flink. Lihat catatan panjang di stop() soal
    # kenapa "gak bisa dicek ulang" itu nyata, bukan cuma teoretis.
    if job.job_client is None or job.status != BackgroundJobStatus.RUNNING:
        return
    try:
        job.status = _map_status(job.job_client.get_job_status().result())
    except Exception:
        # DITEMUKAN lewat testing pakai PyFlink asli (bukan cuma dugaan):
        # tiap statement INSERT yang di-submit lewat submit() di bawah jalan
        # di MiniCluster-nya SENDIRI-SENDIRI (embedded/local mode ini TIDAK
        # berbagi satu cluster per session seperti yang tadinya diasumsikan).
        # Begitu job itu berhenti apapun sebabnya (selesai normal, error,
        # ATAU di-cancel), MiniCluster-nya ikut tutup, dan get_job_status()
        # sesudahnya SELALU gagal dengan
        # "IllegalStateException: MiniCluster ... already shut down" --
        # bukan cuma race condition sesaat. Job LAIN yang masih RUNNING di
        # session yang sama tetap aman (mini-cluster mereka sendiri-sendiri).
        # Kita gak bisa lagi bedain dari sini apakah job ini tadinya selesai
        # normal atau gagal -- jadi UNKNOWN, dengan pesan singkat, BUKAN
        # stack trace Java mentah yang bikin serem tampilannya di UI.
        job.status = BackgroundJobStatus.UNKNOWN
        job.error = (
            "Status job ini sudah tidak bisa dicek lagi -- cluster job ini "
            "sendiri sudah berhenti (biasanya berarti job sudah "
            "selesai/gagal di luar kendali stop() kita)."
        )


def to_dict(job: BackgroundJob) -> dict:
    return {
        "id": job.id,
        "name": job.name,
        "sql": job.sql,
        "status": job.status,
        "flink_job_id": job.flink_job_id,
        "error": job.error,
        "submitted_at": job.submitted_at,
    }


def submit(session: Session, sql: str, name: str) -> BackgroundJob:
    """Submit satu atau lebih statement INSERT INTO sebagai SATU job yang
    jalan selamanya. Kalau lebih dari satu INSERT INTO ada di sql yang sama,
    digabung jadi SATU job lewat StatementSet (t_env.create_statement_set())
    -- mereka berbagi pembacaan source yang sama, bukan jadi job terpisah
    sendiri-sendiri. Statement non-INSERT (CREATE TABLE, USE, dst) di sql
    yang sama dijalankan dulu secara sync seperti biasa sebelum itu.
    """
    job = BackgroundJob(id=str(uuid.uuid4()), name=name, sql=sql)
    session.background_jobs[job.id] = job

    with session.lock:
        try:
            t_env = session_manager.get_env(session)
            # Reuse ekspansi ${NAMA_VAR} yang sama dengan notebook cell biasa,
            # supaya job background juga bisa rujuk .env (misal broker Kafka).
            statements = _split_statements(flink_runner._expand_env_vars(sql))

            insert_statements = []
            for statement in statements:
                if statement.strip().upper().startswith("INSERT"):
                    insert_statements.append(statement)
                else:
                    t_env.execute_sql(statement)

            if not insert_statements:
                raise ValueError(
                    "Submit Job butuh minimal satu statement 'INSERT INTO ... SELECT ...'"
                )

            # Nama job supaya kelihatan di status Flink (bukan cuma UUID kita).
            t_env.get_config().set("pipeline.name", name)

            if len(insert_statements) == 1:
                table_result = t_env.execute_sql(insert_statements[0])
            else:
                statement_set = t_env.create_statement_set()
                for statement in insert_statements:
                    statement_set.add_insert_sql(statement)
                table_result = statement_set.execute()

            job_client = table_result.get_job_client()
            job.job_client = job_client
            if job_client is not None:
                job.flink_job_id = str(job_client.get_job_id())
        except Exception as exc:
            job.status = BackgroundJobStatus.FAILED
            job.error = str(exc)

    return job


def list_jobs(session: Session) -> list[dict]:
    jobs = sorted(session.background_jobs.values(), key=lambda j: j.submitted_at, reverse=True)
    for job in jobs:
        _refresh_status(job)
    return [to_dict(job) for job in jobs]


def stop(session: Session, job_id: str) -> bool:
    job = session.background_jobs.get(job_id)
    if job is None:
        return False
    if job.job_client is None:
        raise ValueError("Job ini gagal disubmit (tidak pernah dapat JobClient), tidak bisa di-cancel")

    job.job_client.cancel().result()
    # Set status CANCELED langsung di sini -- JANGAN panggil _refresh_status()
    # sesudahnya. cancel() yang sukses tanpa exception SUDAH cukup bukti job
    # beneran ke-cancel; _refresh_status() malah bakal nabrak error "MiniCluster
    # sudah shutdown" (lihat catatan panjang di _refresh_status()) karena
    # MiniCluster job ini sendiri ikut tutup begitu dia berhenti, dan salah
    # nampilin job yang SUKSES di-cancel sebagai UNKNOWN.
    job.status = BackgroundJobStatus.CANCELED
    return True
