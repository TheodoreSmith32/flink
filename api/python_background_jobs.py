"""
Background Python job = cell Python yang DIMAKSUDKAN jalan SELAMANYA di
belakang layar (misal: loop polling, konsumsi manual, print berkala), analog
dengan background_jobs.py untuk SQL -- tapi MEKANISMENYA BEDA karena exec()
Python bukan job Flink:

1. TIDAK ADA cancel() yang dijamin seperti job_client.cancel() di Flink.
   "Stop" di sini cuma REQUEST kooperatif: kita set sebuah threading.Event
   (`stop_event`) yang disuntikkan ke namespace exec(), dan KODE USER SENDIRI
   yang harus mengecek itu di dalam loop-nya, misalnya:

       while not stop_event.is_set():
           ...

   Kalau kode tidak pernah mengecek `stop_event` (atau lagi ke-block di
   pemanggilan blocking tanpa timeout), job ini TETAP jalan terus di
   background walau status sudah menandai stop diminta -- beda dengan SQL
   yang cancel()-nya dijamin oleh Flink sendiri.

2. Supaya loop selamanya ini TIDAK memblokir cell lain di session yang sama,
   thread background ini SENGAJA TIDAK memegang session.lock selama
   berjalan -- beda dari cell Python biasa di python_runner.py yang selalu
   pegang lock itu selama run_job(). Konsekuensinya: kalau job ini dan cell
   lain di session yang sama SAMA-SAMA membaca/mengubah t_env atau namespace
   bersamaan, race condition mungkin terjadi (objek PyFlink/Python di
   namespace tidak dijamin thread-safe). Cocok dipakai untuk hal yang relatif
   independen (misal panggil API luar, print berkala, baca t_env sesekali),
   BUKAN untuk memanipulasi tabel yang sama secara intens dengan cell lain
   yang berjalan bersamaan.

PERINGATAN KEAMANAN: sama seperti python_runner.py, exec() menjalankan kode
Python APA ADANYA -- jangan expose service ini ke luar localhost tanpa
autentikasi.
"""

import contextlib
import io
import threading
import time
import traceback
import uuid
from dataclasses import dataclass, field
from enum import Enum

from api import python_runner
from api.session_manager import Session


class PyBackgroundJobStatus(str, Enum):
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"


@dataclass
class PyBackgroundJob:
    id: str
    name: str
    code: str
    status: PyBackgroundJobStatus = PyBackgroundJobStatus.RUNNING
    # True begitu stop() diminta -- BUKAN berarti kode-nya sudah beneran
    # berhenti, cuma tanda request sudah dikirim (lihat docstring modul ini).
    stop_requested: bool = False
    error: str | None = None
    stdout: str = ""
    submitted_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    # Thread & Event asli -- TIDAK diserialisasi ke JSON (lihat to_dict()).
    thread: threading.Thread | None = field(default=None, repr=False, compare=False)
    stop_event: threading.Event = field(default_factory=threading.Event, repr=False, compare=False)


def to_dict(job: PyBackgroundJob) -> dict:
    return {
        "id": job.id,
        "name": job.name,
        "code": job.code,
        "status": job.status,
        "stop_requested": job.stop_requested,
        "error": job.error,
        "stdout": job.stdout,
        "submitted_at": job.submitted_at,
        "finished_at": job.finished_at,
    }


def submit(session: Session, code: str, name: str) -> PyBackgroundJob:
    job = PyBackgroundJob(id=str(uuid.uuid4()), name=name, code=code)
    session.python_background_jobs[job.id] = job

    # Namespace exec() yang SAMA dengan notebook Python biasa (session.py_globals)
    # -- dibuat lazy di sini (sekali, dikunci sebentar) supaya `t_env` sudah
    # pasti ada sebelum thread mulai jalan tanpa lock. `stop_event` disuntikkan
    # dengan nama tetap "stop_event" -- kalau ada job background Python lain
    # yang masih berjalan di session yang sama, entry ini akan menimpa punya
    # job sebelumnya (namespace dipakai bersama), jadi kode lama yang masih
    # cek `stop_event` bakal ikut kena stop_event job yang baru. Cukup untuk
    # notebook belajar satu-per-satu; hindari jalankan >1 job background
    # Python bersamaan di session yang sama kalau mau stop-nya presisi.
    with session.lock:
        namespace = python_runner._get_globals(session)
    namespace["stop_event"] = job.stop_event

    def _run() -> None:
        buffer = io.StringIO()
        try:
            with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(buffer):
                exec(compile(job.code, "<background-cell>", "exec"), namespace)
            job.status = (
                PyBackgroundJobStatus.CANCELED
                if job.stop_event.is_set()
                else PyBackgroundJobStatus.FINISHED
            )
        except Exception:
            job.status = PyBackgroundJobStatus.FAILED
            job.error = traceback.format_exc()
        finally:
            job.stdout = buffer.getvalue()
            job.finished_at = time.time()

    thread = threading.Thread(target=_run, daemon=True)
    job.thread = thread
    thread.start()
    return job


def list_jobs(session: Session) -> list[dict]:
    jobs = sorted(session.python_background_jobs.values(), key=lambda j: j.submitted_at, reverse=True)
    return [to_dict(job) for job in jobs]


def stop(session: Session, job_id: str) -> bool:
    job = session.python_background_jobs.get(job_id)
    if job is None:
        return False
    if job.status != PyBackgroundJobStatus.RUNNING:
        raise ValueError("Job ini sudah tidak RUNNING, tidak perlu/tidak bisa di-stop lagi")

    # Cuma set flag -- lihat docstring modul ini kenapa ini tidak dijamin
    # langsung berhenti seperti stop() di background_jobs.py (SQL).
    job.stop_requested = True
    job.stop_event.set()
    return True
