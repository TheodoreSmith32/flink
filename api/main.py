"""
UI notebook buat submit SQL ATAU kode Python ke Flink tanpa nulis script
terpisah tiap kali. Halaman di "/" punya 3 tab (lihat static/index.html):
"LLM Chat" (ngobrol bebas ke Gemini, lihat llm_runner.py), "SQL" (cell isi
statement SQL), dan "Python" (cell isi kode Python biasa). Tab SQL dan
Python berbagi TableEnvironment yang SAMA (lihat flink_runner.get_env()),
jadi tabel yang dibuat di satu cell/tab tetap kepakai di cell/tab lain.
Tab "LLM Chat" sengaja TIDAK ikut berbagi apapun dengan Flink (endpoint
/chat dieksekusi langsung, bukan lewat BackgroundTask+LOCK) -- jadi gak
akan pernah ke-block oleh SQL/Python job yang nyangkut, dan sebaliknya.

Alurnya per cell (sama untuk kedua tab, cuma beda endpoint):
1. Browser POST ke /jobs (SQL) atau /py-jobs (Python) -> endpoint langsung
   balas job_id (TIDAK nunggu Flink/Python selesai), lalu daftarkan
   run_job() sebagai BackgroundTask.
2. Browser polling GET /jobs/{id} atau /py-jobs/{id} tiap beberapa detik
   sampai statusnya SUCCESS/FAILED, lalu tampilkan hasilnya di bawah cell.

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

from api import flink_runner, llm_runner, python_runner

app = FastAPI(title="PyFlink SQL Runner")

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


class SubmitSqlRequest(BaseModel):
    sql: str


class SubmitPyRequest(BaseModel):
    code: str


class ChatRequest(BaseModel):
    message: str


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.post("/jobs")
def submit_job(req: SubmitSqlRequest, background_tasks: BackgroundTasks):
    if not req.sql.strip():
        raise HTTPException(status_code=400, detail="SQL tidak boleh kosong")

    job = flink_runner.create_job(req.sql)
    background_tasks.add_task(flink_runner.run_job, job.id)
    return {"job_id": job.id, "status": job.status}


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    job = flink_runner.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job tidak ditemukan")
    return job


@app.get("/jobs")
def list_jobs():
    return flink_runner.list_jobs()


@app.post("/py-jobs")
def submit_py_job(req: SubmitPyRequest, background_tasks: BackgroundTasks):
    if not req.code.strip():
        raise HTTPException(status_code=400, detail="Kode tidak boleh kosong")

    job = python_runner.create_job(req.code)
    background_tasks.add_task(python_runner.run_job, job.id)
    return {"job_id": job.id, "status": job.status}


@app.get("/py-jobs/{job_id}")
def get_py_job(job_id: str):
    job = python_runner.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job tidak ditemukan")
    return job


@app.get("/py-jobs")
def list_py_jobs():
    return python_runner.list_jobs()


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
