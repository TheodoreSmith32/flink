"""
Notebook Python: cell isi kode Python biasa (bukan cuma string SQL),
dieksekusi lewat exec() di satu namespace yang persist antar cell -- sama
seperti variabel yang tetap ada di cell Jupyter berikutnya.

Sejak ada session_manager.py, namespace ini (session.py_globals) milik SATU
session, bukan lagi satu untuk seluruh proses -- lihat session_manager.Session.
Namespace-nya sudah diisi `t_env`, TableEnvironment session yang SAMA
dipakai notebook SQL di flink_runner.py (lewat session_manager.get_env()).
Jadi kode di sini bisa langsung, misalnya:

    t_env.execute_sql("CREATE TABLE ...")
    df = t_env.sql_query("SELECT * FROM ...").to_pandas()
    print(df)

...tanpa perlu setup ulang, dan tabel yang dibuat lewat notebook SQL di
session yang sama bisa langsung dipakai di sini juga (dan sebaliknya) --
tapi tidak pernah dengan session lain.

Eksekusi dipagari session.lock yang SAMA dengan notebook SQL -- bukan lock
terpisah -- karena keduanya menyentuh TableEnvironment yang sama dan tidak
aman dijalankan bersamaan dari dua thread berbeda.

Output cell HANYA dari print() (di-redirect lewat contextlib.redirect_stdout),
TIDAK ada auto-display ekspresi terakhir seperti Jupyter -- lebih sederhana
dan predictable buat belajar.

`TableResult.print()` bawaan PyFlink DITEMPEL (lihat _notebook_print di
bawah) supaya `.execute().print()` ikut ke-capture juga -- versi aslinya
nulis langsung ke stdout JVM lewat py4j, lolos dari redirect_stdout di atas.

PERINGATAN KEAMANAN: exec() menjalankan Python APA ADANYA -- termasuk baca/
tulis file, `os.system(...)`, dst. Ini oke untuk dipakai sendiri di
localhost (default uvicorn hanya bind ke 127.0.0.1), tapi JANGAN expose
service ini ke jaringan/internet tanpa autentikasi -- siapa pun yang bisa
akses endpoint /sessions/{id}/py-jobs otomatis bisa menjalankan kode apa
saja di mesin ini.
"""

import contextlib
import io
import itertools
import time
import traceback
import uuid
from dataclasses import dataclass, field
from enum import Enum

from pyflink.table.table_result import TableResult

from api import session_manager
from api.session_manager import Session

# Sama seperti flink_runner.PREVIEW_ROW_LIMIT (SELECT preview) -- supaya
# .execute().print() dari source unbounded (Kafka dkk) tidak nge-block cell
# ini selamanya.
PREVIEW_ROW_LIMIT = 20


def _notebook_print(self: TableResult) -> None:
    """Pengganti TableResult.print() bawaan PyFlink, ditempel di bawah modul
    ini. Versi asli manggil self._j_table_result.print() -- nulis langsung
    ke stdout JVM lewat py4j, yang TIDAK ke-capture oleh
    contextlib.redirect_stdout() di run_job() (nyasar ke stdout proses
    uvicorn, bukan ke hasil cell). Versi ini narik hasil balik ke Python
    (collect()) baru print() Python biasa, jadi user bisa nulis
    `.execute().print()` seperti biasa dan hasilnya tetap keluar di cell --
    gak perlu tau bedanya print() vs .print()."""
    iterator = self.collect()
    try:
        rows = list(itertools.islice(iterator, PREVIEW_ROW_LIMIT))
    finally:
        iterator.close()

    for row in rows:
        print(row)
    if len(rows) == PREVIEW_ROW_LIMIT:
        print(
            f"... (dipotong, cuma {PREVIEW_ROW_LIMIT} baris pertama -- "
            "pakai .to_pandas() kalau butuh semuanya)"
        )


# Ditempel sekali di sini, tidak pernah di-restore -- modul ini cuma dipakai
# di proses FastAPI (script CLI jobs/*.py jalan di proses Python terpisah,
# jadi tidak kena patch ini).
TableResult.print = _notebook_print


class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


@dataclass
class PyJob:
    id: str
    code: str
    status: JobStatus = JobStatus.PENDING
    stdout: str = ""
    error: str | None = None
    created_at: float = field(default_factory=time.time)
    finished_at: float | None = None


def create_job(session: Session, code: str) -> PyJob:
    job = PyJob(id=str(uuid.uuid4()), code=code)
    session.py_jobs[job.id] = job
    return job


def get_job(session: Session, job_id: str) -> PyJob | None:
    return session.py_jobs.get(job_id)


def list_jobs(session: Session) -> list[PyJob]:
    return sorted(session.py_jobs.values(), key=lambda j: j.created_at, reverse=True)


def _get_globals(session: Session) -> dict:
    """Namespace persist antar cell dalam satu session, mulai berisi
    `t_env` yang sama dengan notebook SQL di session itu. Dibuat sekali
    (lazy) per session."""
    if session.py_globals is None:
        session.py_globals = {"t_env": session_manager.get_env(session)}
    return session.py_globals


def run_job(session: Session, job_id: str) -> None:
    """Dipanggil sebagai FastAPI BackgroundTask -- fungsi sync biasa, supaya
    Starlette menjalankannya di threadpool dan tidak memblokir event loop
    yang melayani request lain."""
    job = session.py_jobs[job_id]
    job.status = JobStatus.RUNNING

    with session.lock:
        namespace = _get_globals(session)
        buffer = io.StringIO()
        try:
            with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(buffer):
                exec(compile(job.code, "<cell>", "exec"), namespace)
            job.status = JobStatus.SUCCESS
        except Exception:
            job.status = JobStatus.FAILED
            # traceback lengkap, bukan cuma str(exc) -- biar kelihatan baris
            # mana di cell yang error, sama seperti traceback Python biasa.
            job.error = traceback.format_exc()
        finally:
            job.stdout = buffer.getvalue()
            job.finished_at = time.time()
