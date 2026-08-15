"""
UI notebook buat submit SQL ATAU kode Python ke Flink tanpa nulis script
terpisah tiap kali. Halaman di "/" punya panel "LLM Chat" (ngobrol bebas ke
Gemini, lihat llm_runner.py) di sisi kiri, dan notebook SQL/Python di sisi
kanan (lihat static/index.html).

Sebelum bisa pakai notebook SQL/Python, user harus bikin (atau pilih)
SESSION dulu lewat /sessions (lihat session_manager.py) -- tiap session
punya TableEnvironment sendiri, jadi tabel/state satu session tidak bocor
ke session lain. Tab SQL dan tab Python DALAM SATU session tetap berbagi
TableEnvironment yang sama (lihat session_manager.get_env()), jadi tabel
yang dibuat di satu cell/tab tetap kepakai di cell/tab lain -- selama masih
di session yang sama. Tab "LLM Chat" sengaja TIDAK terikat session apa pun
(endpoint /chat dieksekusi langsung, bukan lewat BackgroundTask+session
lock) -- jadi gak akan pernah ke-block oleh SQL/Python job yang nyangkut,
dan sebaliknya.

Alurnya per cell (sama untuk kedua tab, cuma beda endpoint):
1. Browser POST ke /sessions/{id}/jobs (SQL) atau /sessions/{id}/py-jobs
   (Python) -> endpoint langsung balas job_id (TIDAK nunggu Flink/Python
   selesai), lalu daftarkan run_job() sebagai BackgroundTask.
2. Browser polling GET /sessions/{id}/jobs/{job_id} atau
   /sessions/{id}/py-jobs/{job_id} tiap beberapa detik sampai statusnya
   SUCCESS/FAILED, lalu tampilkan hasilnya di bawah cell.

Kenapa harus job_id + polling, bukan langsung nunggu hasil di response POST?
Karena job-nya bisa makan waktu, dan HTTP request tidak boleh menggantung
lama-lama. SELECT dari source unbounded (misal Kafka) sendiri sudah dibatasi
di flink_runner.py (preview beberapa baris lalu berhenti), jadi tidak akan
menggantung selamanya.

PERINGATAN KEAMANAN: tab Python menjalankan kode APA ADANYA lewat exec()
(lihat python_runner.py) -- jangan expose service ini ke luar localhost
tanpa autentikasi.

Jalankan:
    source .venv/bin/activate
    uvicorn api.main:app --reload --app-dir .
Lalu buka http://localhost:8000
"""

import os

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from api import connectors, flink_runner, llm_runner, python_runner, session_manager

app = FastAPI(title="PyFlink SQL Runner")

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


class CreateSessionRequest(BaseModel):
    name: str


class SubmitSqlRequest(BaseModel):
    sql: str


class SubmitPyRequest(BaseModel):
    code: str


class ChatRequest(BaseModel):
    message: str


def _require_session(session_id: str) -> session_manager.Session:
    session = session_manager.get_session(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session tidak ditemukan")
    return session


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.get("/connectors")
def get_connectors():
    return connectors.list_connectors()


@app.post("/sessions")
def create_session(req: CreateSessionRequest):
    if not req.name.strip():
        raise HTTPException(status_code=400, detail="Nama session tidak boleh kosong")
    try:
        session = session_manager.create_session(req.name.strip())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"id": session.id, "name": session.name, "created_at": session.created_at}


@app.get("/sessions")
def list_sessions():
    return [
        {"id": s.id, "name": s.name, "created_at": s.created_at}
        for s in session_manager.list_sessions()
    ]


@app.delete("/sessions/{session_id}")
def delete_session(session_id: str):
    if not session_manager.delete_session(session_id):
        raise HTTPException(status_code=404, detail="Session tidak ditemukan")
    return {"status": "ok"}


@app.post("/sessions/{session_id}/jobs")
def submit_job(session_id: str, req: SubmitSqlRequest, background_tasks: BackgroundTasks):
    session = _require_session(session_id)
    if not req.sql.strip():
        raise HTTPException(status_code=400, detail="SQL tidak boleh kosong")

    job = flink_runner.create_job(session, req.sql)
    background_tasks.add_task(flink_runner.run_job, session, job.id)
    return {"job_id": job.id, "status": job.status}


@app.get("/sessions/{session_id}/jobs/{job_id}")
def get_job(session_id: str, job_id: str):
    session = _require_session(session_id)
    job = flink_runner.get_job(session, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job tidak ditemukan")
    return job


@app.get("/sessions/{session_id}/jobs")
def list_jobs(session_id: str):
    session = _require_session(session_id)
    return flink_runner.list_jobs(session)


@app.get("/sessions/{session_id}/tables")
def list_tables(session_id: str):
    session = _require_session(session_id)
    with session.lock:
        try:
            return flink_runner.list_tables(session)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))


@app.post("/sessions/{session_id}/py-jobs")
def submit_py_job(session_id: str, req: SubmitPyRequest, background_tasks: BackgroundTasks):
    session = _require_session(session_id)
    if not req.code.strip():
        raise HTTPException(status_code=400, detail="Kode tidak boleh kosong")

    job = python_runner.create_job(session, req.code)
    background_tasks.add_task(python_runner.run_job, session, job.id)
    return {"job_id": job.id, "status": job.status}


@app.get("/sessions/{session_id}/py-jobs/{job_id}")
def get_py_job(session_id: str, job_id: str):
    session = _require_session(session_id)
    job = python_runner.get_job(session, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job tidak ditemukan")
    return job


@app.get("/sessions/{session_id}/py-jobs")
def list_py_jobs(session_id: str):
    session = _require_session(session_id)
    return python_runner.list_jobs(session)


@app.post("/chat")
def send_chat(req: ChatRequest):
    if not req.message.strip():
        raise HTTPException(status_code=400, detail="Pesan tidak boleh kosong")

    try:
        reply = llm_runner.ask(req.message)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {"reply": reply}


@app.get("/chat")
def get_chat_history():
    return llm_runner.history()


@app.delete("/chat")
def clear_chat():
    llm_runner.reset()
    return {"status": "ok"}
