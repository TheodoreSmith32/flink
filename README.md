# PyFlink — Belajar dari Nol

Project belajar pribadi untuk PyFlink (Apache Flink Table API). Berisi
serangkaian script kecil yang disusun bertahap, dari yang paling sederhana
(batch, data hardcode) sampai yang mendekati pemakaian nyata (source/sink
file, Kafka, dan sebuah notebook web buat submit SQL/Python interaktif).

## Daftar Isi

1. [Overview](#overview)
2. [Struktur Project](#struktur-project)
3. [Tech Stack](#tech-stack)
4. [Prerequisites](#prerequisites)
5. [Getting Started](#getting-started)
6. [Environment Variables](#environment-variables)
7. [Integrations](#integrations)
8. [Usage](#usage)
9. [Istilah Penting](#istilah-penting)
10. [Langkah Berikutnya](#langkah-berikutnya)

---

## Overview

Urutan file di project ini sengaja dari yang paling sederhana ke yang makin
dekat dengan pemakaian nyata. Baca urut dari atas ke bawah pada bagian
[Usage](#usage), dan coba jalankan tiap script sebelum lanjut ke yang
berikutnya:

1. `hello_flink.py` — Table API dasar, mode **BATCH**.
2. `hello_flink_streaming.py` — mode **STREAMING**.
3. `hello_flink_file.py` — **source** dari file (`data/kalimat.csv`).
4. `hello_flink_sink.py` — **sink** ke file.
5. `hello_flink_to_csv.py` — CSV lewat pandas, tanpa sink table.
6. `hello_flink_kafka.py` — **source** dari Kafka.
7. `api/` — service FastAPI dengan UI notebook (tab SQL, Python, dan LLM
   Chat) untuk submit query interaktif dari browser.

## Struktur Project

```
.
├── api/                          # Service notebook (FastAPI + UI web)
│   ├── main.py                   # Endpoint FastAPI (sessions, jobs, py-jobs, background-jobs, chat)
│   ├── session_manager.py        # Session Manager -- TableEnvironment terisolasi per session
│   ├── flink_runner.py           # Eksekusi job SQL notebook, list/describe table
│   ├── python_runner.py          # Eksekusi cell Python (exec), state persist per session
│   ├── background_jobs.py        # "Submit Job" -- job INSERT INTO yang jalan selamanya di background
│   ├── llm_runner.py             # Chat ke Gemini (google-genai) + agent generate-SQL
│   ├── connectors.py             # Katalog connector siap-pakai (template CREATE TABLE)
│   └── static/index.html         # UI notebook (Session bar, tab SQL/Python, LLM Chat, Background Jobs)
├── data/                         # Data contoh + hasil output sink/CSV
│   └── kalimat.csv               # Source data untuk hello_flink_file.py
├── jars/                         # JAR connector tambahan
│   └── flink-sql-connector-kafka-1.17.2.jar
├── hello_flink.py                # 1. Table API dasar (BATCH)
├── hello_flink_streaming.py      # 2. Mode STREAMING
├── hello_flink_file.py           # 3. Source dari file
├── hello_flink_sink.py           # 4. Sink ke file
├── hello_flink_to_csv.py         # 5. CSV tanpa sink table
├── hello_flink_kafka.py          # 6. Source dari Kafka
├── requirements.txt
├── template.env                  # Contoh .env (isi .env asli jangan di-commit)
└── .env                          # Kredensial lokal (di-gitignore)
```

## Tech Stack

| Teknologi | Versi | Kegunaan |
|---|---|---|
| Python | 3.10 | Bahasa utama project (lihat `.venv`) |
| apache-flink | 1.17.2 | Table API — batch & streaming |
| fastapi | 0.141.1 | Server notebook web di `api/` |
| uvicorn | 0.52.1 | ASGI server untuk FastAPI |
| pandas | 2.3.3 | `.to_pandas()` di `hello_flink_to_csv.py` |
| pyarrow | 11.0.0 | Dibutuhkan `.to_pandas()` |
| python-dotenv | 1.2.2 | Baca konfigurasi dari `.env` |
| google-genai | 2.17.0 | Tab "LLM Chat" (Gemini) di `api/` |

Rencana ke depan (belum diimplementasikan): **Dinky + Flink session
cluster** lewat Docker, lihat `template.env` dan [Langkah
Berikutnya](#langkah-berikutnya).

## Prerequisites

- Python 3.10 (virtualenv sudah disiapkan di `.venv`)
- **Java 11** — Flink 1.17 **tidak jalan di Java 21** (default JVM di banyak
  sistem terbaru). `.venv/bin/activate` sudah dimodifikasi supaya otomatis
  set `JAVA_HOME` ke Java 11 saat venv diaktifkan, dan mengembalikannya lagi
  saat `deactivate` — jadi tidak perlu set `JAVA_HOME` manual.

## Getting Started

```bash
source .venv/bin/activate
pip install -r requirements.txt   # kalau dependency belum terpasang
python hello_flink.py             # contoh paling sederhana, mode BATCH
```

Tidak perlu langkah instalasi Java/JAVA_HOME manual — sudah ditangani oleh
`.venv/bin/activate` seperti dijelaskan di [Prerequisites](#prerequisites).

## Environment Variables

Isi `.env` di root project (contoh nilainya ada di `template.env`). Jangan
commit `.env` asli — sudah masuk `.gitignore`.

| Nama | Deskripsi | Default | Wajib |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Alamat broker Kafka (multi-broker dipisah koma tanpa spasi) | `<PLACEHOLDER>` | Ya, untuk `hello_flink_kafka.py` & tab notebook yang baca Kafka |
| `KAFKA_TOPIC` | Nama topic Kafka yang dibaca | `<PLACEHOLDER>` | Ya, sama seperti di atas |
| `GEMINI_API_KEY` | API key untuk tab "LLM Chat" (Gemini). Buat di https://aistudio.google.com/apikey | `<PLACEHOLDER>` | Ya, untuk tab LLM Chat di `api/` |
| `GEMINI_MODEL` | Nama model Gemini yang dipakai | `gemini-flash-latest` | Tidak |
| `DINKY_VERSION` | Versi image Dinky (rencana setup Docker) | `1.2.5` | Tidak (belum dipakai kode saat ini) |
| `FLINK_VERSION` | Versi mayor Flink untuk Dinky | `1.17` | Tidak (belum dipakai kode saat ini) |
| `FLINK_FULL_VERSION` | Versi lengkap Flink untuk Dinky | `1.17.2` | Tidak (belum dipakai kode saat ini) |
| `TZ` | Timezone container Dinky | `Asia/Jakarta` | Tidak (belum dipakai kode saat ini) |
| `DB_ACTIVE` | Metadata DB Dinky: `h2`, `mysql`, atau `postgresql` | `mysql` | Tidak (belum dipakai kode saat ini) |
| `MYSQL_ADDR` | Alamat MySQL untuk metadata Dinky | `<PLACEHOLDER>` | Tidak (belum dipakai kode saat ini) |
| `MYSQL_DATABASE` | Nama database MySQL Dinky | `<PLACEHOLDER>` | Tidak (belum dipakai kode saat ini) |
| `MYSQL_USERNAME` | Username MySQL Dinky | `<PLACEHOLDER>` | Tidak (belum dipakai kode saat ini) |
| `MYSQL_PASSWORD` | Password MySQL Dinky | `<PLACEHOLDER>` | Tidak (belum dipakai kode saat ini) |
| `MYSQL_ROOT_PASSWORD` | Root password MySQL Dinky | `<PLACEHOLDER>` | Tidak (belum dipakai kode saat ini) |

> Variabel `DINKY_*`, `FLINK_*`, `TZ`, `DB_ACTIVE`, `MYSQL_*` sudah ada di
> `template.env` untuk setup **Dinky + Flink cluster** via Docker yang masih
> direncanakan (lihat [Langkah Berikutnya](#langkah-berikutnya)) — belum
> dibaca oleh kode Python manapun di project ini saat ini.

## Integrations

| Service | Kegunaan | Koneksi |
|---|---|---|
| Kafka | Source unbounded untuk `hello_flink_kafka.py` dan tab notebook | Connector Flink `kafka` via JAR di `jars/flink-sql-connector-kafka-1.17.2.jar`, didaftarkan lewat `pipeline.jars`; alamat broker & topic dari `.env` |
| Google Gemini | Tab "LLM Chat" di notebook `api/` | SDK `google-genai`, autentikasi via `GEMINI_API_KEY` di `.env` |

## Usage

### 1. `hello_flink.py` — Table API dasar (mode BATCH)

Contoh paling sederhana. Intinya:

1. Data mentah (list kalimat) di-hardcode di Python.
2. Kalimat dipecah jadi kata-kata di Python (`.split(" ")`) — bukan di
   Flink — supaya query SQL-nya simpel.
3. Kata-kata itu dimasukkan ke Flink lewat `t_env.from_elements(...)`,
   jadi sebuah **table** bernama `kata_table`.
4. Dihitung dengan SQL biasa: `SELECT kata, COUNT(*) ... GROUP BY kata`.
5. `.execute().print()` menjalankan query dan mencetak hasilnya ke
   terminal.

Mode yang dipakai: `EnvironmentSettings.in_batch_mode()` — cocok untuk
data yang sudah "selesai"/tetap (bounded), seperti file atau tabel
database. Hasilnya dihitung sekali sampai tuntas, baru ditampilkan.

```bash
source .venv/bin/activate
python hello_flink.py
```

### 2. `hello_flink_streaming.py` — mode STREAMING

Sama persis dengan `hello_flink.py`, hanya satu baris yang beda:

```python
env_settings = EnvironmentSettings.in_streaming_mode()  # bukan in_batch_mode()
```

Datanya masih tetap list Python yang sama (jadi tetap "bounded" —
streamnya ada ujungnya), tapi **cara Flink mengeksekusi beda**:

- Di batch: tunggu semua data selesai diproses, baru tampilkan hasil final.
- Di streaming: hasil per baris muncul begitu ada perubahan, dan baris
  yang sama bisa **muncul lebih dari sekali** di output — baris lama
  "ditarik" (retract) lalu diganti baris baru dengan angka yang sudah
  ter-update. Ini disebut **changelog / retraction**, dan merupakan ciri
  khas dari agregasi (`COUNT`, `GROUP BY`, dll) di streaming.

```bash
source .venv/bin/activate
python hello_flink_streaming.py
```

### 3. `hello_flink_file.py` — SOURCE dari file

Sampai sini, data masih hardcode di Python. Script ini menggantinya
dengan **source** sungguhan: baca dari `data/kalimat.csv` lewat konektor
`filesystem` milik Flink.

```sql
CREATE TABLE kalimat_table (
    kalimat STRING
) WITH (
    'connector' = 'filesystem',
    'path' = '...../data/kalimat.csv',
    'format' = 'csv'
)
```

Ini pola **DDL** (`CREATE TABLE ... WITH (...)`) yang bakal terus dipakai
untuk connector apa pun (Kafka, database, dst) — cuma nilai `connector`
dan opsi lain yang berubah.

Alur selanjutnya sama seperti sebelumnya: baris hasil `SELECT` ditarik
ke Python (`.collect()`), dipecah jadi kata, dimasukkan lagi sebagai
`kata_table`, lalu di-`COUNT` seperti biasa.

> Kenapa masih ditarik ke Python untuk di-split? Karena split kata per
> spasi belum ditulis sebagai SQL/UDF di tahap ini — itu ada di roadmap
> [Python UDF](#langkah-berikutnya).

```bash
source .venv/bin/activate
python hello_flink_file.py
```

### 4. `hello_flink_sink.py` — SINK ke file

Kalau source itu Flink **membaca** dari suatu tempat, sink itu Flink
**menulis** ke suatu tempat. Connector-nya bisa sama (`filesystem`),
bedanya cuma arah pakainya.

```sql
CREATE TABLE hasil_table (
    kata STRING,
    jumlah BIGINT
) WITH (
    'connector' = 'filesystem',
    'path' = '...../data/hasil_jumlah_kata',
    'format' = 'csv'
)
```

Yang benar-benar menjalankan job dan menulis ke disk adalah:

```python
t_env.execute_sql("""
    INSERT INTO hasil_table
    SELECT kata, COUNT(*) AS jumlah
    FROM kata_table
    GROUP BY kata
""").wait()
```

Poin penting:

- **`INSERT INTO ... SELECT`** menggantikan `.execute().print()` sebagai
  cara "menjalankan" query — bedanya hasilnya ditulis ke tabel sink,
  bukan dicetak.
- **`.wait()` wajib** di sini. `execute_sql()` untuk `INSERT` bersifat
  **async** — dia submit job ke Flink lalu langsung lanjut ke baris
  berikutnya di Python, sementara job-nya jalan di belakang layar.
  Tanpa `.wait()`, kode yang membaca ulang folder output bisa jalan
  duluan sebelum Flink selesai menulis.
- Folder tujuan dibersihkan (`shutil.rmtree`) di awal script, karena
  Flink menolak menulis ke folder sink yang sudah ada isinya.
- Flink menulis sink filesystem sebagai **beberapa file di dalam
  folder**, bukan satu file tunggal — makanya script membaca semua file
  di folder tersebut, bukan satu file dengan nama tetap.

```bash
source .venv/bin/activate
python hello_flink_sink.py
```

### 5. `hello_flink_to_csv.py` — CSV tanpa sink table

Section sebelumnya (`hello_flink_sink.py`) nulis hasil ke file dengan cara
"Flink banget": `CREATE TABLE ... WITH ('connector'='filesystem', ...)`
buat sink, lalu `INSERT INTO ... SELECT`. Itu pola yang benar untuk data
besar/real-world, tapi ada overhead: harus bersihkan folder tujuan dulu,
hasilnya jadi folder isi beberapa file part, dan harus ingat `.wait()`.

Kalau tujuannya cuma "generate CSV doang" dari hasil query yang **kecil**
(muat di memory), ada jalan pintas: skip bikin sink table sama sekali.

```python
hasil = t_env.sql_query("SELECT kata, COUNT(*) AS jumlah FROM kata_table GROUP BY kata")
df = hasil.to_pandas()      # <- tarik hasil Table API jadi pandas DataFrame
df.to_csv(output_path, index=False)   # <- nulis CSV pakai pandas biasa, bukan Flink
```

Flink cuma dipakai untuk bagian **hitungnya** (SQL: `SELECT`, `GROUP BY`,
dst); bagian nulis file dikerjakan pandas. Untungnya:

- Hasilnya **satu file CSV**, bukan folder berisi banyak part file.
- Tidak perlu bersihkan folder tujuan dulu tiap run.
- Tidak perlu `.wait()` — `.to_pandas()` sudah otomatis nunggu job selesai
  sebelum data dikembalikan.

Trade-off-nya: `.to_pandas()` menarik SEMUA baris hasil ke memory Python
sekaligus, jadi cuma masuk akal untuk hasil yang kecil/sudah teragregasi.
Untuk data besar atau stream yang beneran tidak berhenti, sink
`filesystem`/Kafka/dst di `hello_flink_sink.py` tetap caranya yang benar
(datanya ditulis Flink langsung, tidak pernah numpuk semua di memory
Python).

`pandas` dan `pyarrow` dibutuhkan untuk `.to_pandas()` (sudah ada di
`requirements.txt`).

```bash
source .venv/bin/activate
python hello_flink_to_csv.py
```

### 6. `hello_flink_kafka.py` — SOURCE dari Kafka

Konsepnya identik dengan `hello_flink_file.py`: `CREATE TABLE ... WITH
(...)`, cuma `'connector'` sekarang `'kafka'`, bukan `'filesystem'`. Yang
beda justru hal-hal di sekitarnya, karena Kafka topic itu **unbounded**
(tidak ada ujungnya) sementara file sebelumnya **bounded**:

- **Butuh JAR connector tambahan.** `pip install apache-flink` tidak
  membawa connector Kafka — itu didownload terpisah ke
  `jars/flink-sql-connector-kafka-1.17.2.jar` (versi HARUS cocok dengan
  versi `apache-flink` di `requirements.txt`), lalu didaftarkan ke Flink
  lewat `t_env.get_config().set("pipeline.jars", f"file://{jar_path}")`
  sebelum `CREATE TABLE`.
- **Alamat broker & nama topic dari `.env`**, bukan hardcode di script
  (`KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, dibaca pakai
  `python-dotenv`). Alasannya sederhana: alamat broker itu detail
  environment/kredensial, bukan sesuatu yang seharusnya ikut nempel di
  kode.
- **Script ini tidak pernah selesai sendiri** selama topic-nya hidup —
  beda dengan `hello_flink_streaming.py` yang walau "streaming mode",
  datanya tetap bounded (list Python yang ada habisnya). Di sini
  sumbernya beneran tidak berhenti, jadi `.execute().print()` akan terus
  mencetak baris baru sampai di-`Ctrl+C`.
- **`format = 'raw'`** dipakai karena skema pesan di topic belum tentu
  diketahui — seluruh isi pesan dibaca apa adanya jadi satu kolom
  STRING. Kalau nanti tahu formatnya (misal JSON), `format` bisa diganti
  supaya field-field-nya otomatis jadi kolom terpisah, bukan satu string
  mentah.

Sebelum jalan, isi dulu `.env` (lihat [Environment
Variables](#environment-variables)):
```
KAFKA_BOOTSTRAP_SERVERS=broker1:9092,broker2:9092
KAFKA_TOPIC=topic_kafka
```

```bash
source .venv/bin/activate
python hello_flink_kafka.py
```

### 7. `api/` — UI notebook buat submit SQL

Sampai `hello_flink_sink.py`, tiap kali mau coba SQL baru kita harus edit
script Python dan jalankan ulang dari terminal. Folder `api/` mengubah itu
jadi sebuah **service** yang jalan terus: buka halaman web, tulis SQL,
klik Run, hasilnya muncul di browser.

Isinya:

- **`api/flink_runner.py`** — "otak"-nya. Nyimpen semua job di dictionary
  in-memory (`_JOBS`). Motong SQL jadi beberapa statement lewat titik
  koma, lalu jalanin satu-satu ke Flink. Kalau statement-nya `SELECT`,
  hasilnya ditarik (dibatasi, lihat poin preview di bawah) dan disimpan di
  `job.rows`/`job.columns`. Kalau `INSERT INTO`, dipanggil `.wait()`
  seperti di `hello_flink_sink.py`.
- **`api/main.py`** — server FastAPI-nya. Tiga endpoint:
  - `POST /jobs` — terima SQL, langsung balas `job_id` (tidak nunggu Flink
    selesai), lalu daftarkan `run_job()` sebagai **BackgroundTask**.
  - `GET /jobs/{job_id}` — cek status job (`PENDING` → `RUNNING` →
    `SUCCESS`/`FAILED`) plus hasilnya kalau sudah selesai.
  - `GET /jobs` — daftar semua job yang pernah disubmit.
- **`api/static/index.html`** — UI ala **notebook** (mirip Jupyter):
  beberapa "cell" SQL yang bisa ditambah satu-satu (tombol "+ Tambah
  cell"), masing-masing punya tombol Run sendiri dan hasilnya muncul
  persis di bawah cell itu. Tiap cell submit ke `/jobs` secara independen
  dan polling hasilnya sendiri-sendiri.

Poin penting:

- **Kenapa job_id + polling, bukan langsung tunggu hasil di response
  POST?** Karena job Flink bisa lama, dan HTTP request tidak boleh
  menggantung lama-lama nunggu. Pola "submit → dapat ID → polling status"
  ini juga yang dipakai kalau nanti sungguhan connect ke Flink cluster
  (mirip cara kerja Flink REST API sendiri).
- **Satu `TableEnvironment` dipakai bersama untuk SEMUA cell** (dibuat
  sekali secara lazy di `flink_runner._get_env()`, bukan dibuat baru
  tiap job seperti versi awal). Ini yang bikin tabel yang dibuat di satu
  cell tetap "diingat" dan bisa dipakai di cell lain — persis seperti
  notebook beneran. Konsekuensinya, job tetap dieksekusi satu-satu lewat
  `threading.Lock`, karena satu `TableEnvironment` yang sama dipakai
  bareng-bareng dan tidak aman dieksekusi dari banyak thread sekaligus.
- **Preview baris dibatasi (`PREVIEW_ROW_LIMIT = 20`).** Kalau SELECT-nya
  dari source **unbounded** (misal Kafka, seperti di
  `hello_flink_kafka.py`), narik SEMUA hasilnya lewat `.collect()` biasa
  akan menggantung selamanya — dan karena environment-nya dipakai
  bersama, itu bakal nge-block SEMUA cell lain juga. Solusinya: tarik
  maksimal 20 baris lewat `itertools.islice()`, lalu **tutup iterator-nya
  lebih awal**. Menutup iterator ternyata juga membatalkan job Flink di
  baliknya (sudah dites langsung), jadi tidak ada job unbounded yang
  numpuk nganggur di belakang layar. Kalau hasilnya kena batas ini,
  `job.truncated` jadi `true` dan UI kasih catatan kecil di bawah tabel.
- **`${NAMA_VAR}` di SQL diganti nilainya dari `.env` saat dieksekusi**
  (lihat `_expand_env_vars()`), misalnya:
  ```sql
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}'
  ```
  Ini konsisten dengan alasan `KAFKA_BOOTSTRAP_SERVERS` disimpan di
  `.env` sejak awal (lihat [section 6](#6-hello_flink_kafkapy--source-dari-kafka))
  — broker Kafka tidak perlu diketik ulang langsung di textarea browser.
  JAR connector Kafka juga otomatis didaftarkan tiap kali environment ini
  dibuat, jadi `CREATE TABLE ... WITH ('connector'='kafka', ...)` bisa
  langsung dicoba dari notebook ini tanpa langkah tambahan.
- Ini masih versi **embedded**: Flink-nya hidup selama proses FastAPI
  hidup, bukan cluster terpisah. Kalau server FastAPI mati, tabel & job
  history ikut hilang. Versi "tetap jalan walau server restart" itu yang
  nanti dijembatani oleh setup **Dinky + Flink cluster** (lihat
  `template.env` dan [Langkah Berikutnya](#langkah-berikutnya)).

Jalankan:

```bash
source .venv/bin/activate
uvicorn api.main:app --reload --app-dir .
```

Lalu buka `http://localhost:8000`. Contoh alur 2 cell yang saling
nyambung (coba di cell terpisah, buktikan tabelnya "diingat"):

```sql
-- Cell 1
CREATE TABLE t (a INT, b STRING) WITH ('connector'='datagen','number-of-rows'='5');
```
```sql
-- Cell 2 (submission terpisah, tabel dari cell 1 tetap kepakai)
SELECT a, b FROM t;
```

Contoh baca Kafka lewat notebook (broker diambil dari `.env`):

```sql
CREATE TABLE kafka_source (message STRING) WITH (
    'connector' = 'kafka',
    'topic' = '${KAFKA_TOPIC}',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'pyflink-belajar',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'raw'
);
SELECT message FROM kafka_source;
```

#### Tab Python di `api/` — notebook kode, bukan cuma SQL

Halaman notebook (`api/static/index.html`) punya tab kedua: "Python
Notebook", untuk kasus yang kurang cocok ditulis SQL (misalnya
`df.describe()` dari hasil `to_pandas()`, atau logic yang lebih gampang
ditulis Python). Cara kerjanya mirip tab SQL, cuma isi cell-nya kode
Python biasa, dieksekusi lewat `exec()` di server
(`api/python_runner.py`):

- **Endpoint terpisah**, pola sama: `POST /py-jobs` (submit kode, balas
  `job_id`), `GET /py-jobs/{id}` (polling status + `stdout`/`error`),
  `GET /py-jobs` (daftar semua).
- **Variabel persist antar cell**, sama seperti Jupyter — `x = 5` di satu
  cell, `print(x)` di cell lain (submission terpisah) tetap kebaca. Ini
  jalan lewat satu dictionary Python (`_GLOBALS`) yang dipakai bareng
  sebagai namespace `exec()`, dibuat sekali lalu dipakai ulang.
- **Berbagi `TableEnvironment` yang SAMA dengan tab SQL** — variabel
  `t_env` sudah otomatis tersedia di tiap cell Python, dan sudah dites:
  tabel yang dibuat lewat tab SQL bisa langsung di-`sql_query()` dari tab
  Python, dan sebaliknya.
- **Lock yang dipakai juga SAMA** dengan tab SQL (`flink_runner.LOCK`,
  bukan lock terpisah) — karena keduanya menyentuh `t_env` yang sama,
  tidak boleh dieksekusi bersamaan dari dua thread berbeda.
- **Output cuma dari `print()`** (di-redirect lewat
  `contextlib.redirect_stdout`) — sengaja TIDAK ada auto-display
  ekspresi terakhir seperti Jupyter (mis. cuma nulis `df` doang tanpa
  `print()`), biar perilakunya predictable dan implementasinya simpel.
- **Error ditampilkan sebagai traceback lengkap** (`traceback.format_exc()`),
  bukan cuma `str(exc)` — jadi kelihatan baris mana di cell yang error,
  sama seperti traceback Python biasa. Sudah dites: `stdout` sebelum baris
  yang error tetap tersimpan/ditampilkan.

> **Peringatan keamanan:** tab ini menjalankan Python **apa adanya** lewat
> `exec()` — termasuk baca/tulis file, `os.system(...)`, dst. Ini oke
> untuk dipakai sendiri di localhost (default `uvicorn` cuma bind ke
> `127.0.0.1`), tapi JANGAN expose service ini ke jaringan/internet tanpa
> autentikasi — siapa pun yang bisa akses endpoint `/py-jobs` otomatis
> bisa menjalankan kode apa saja di mesin ini.

Contoh (jalankan di tab Python setelah tabel `t` dibuat lewat tab SQL):

```python
df = t_env.sql_query("SELECT a, b FROM t").to_pandas()
print(df)
print("jumlah baris:", len(df))
```

#### Tab LLM Chat di `api/` — ngobrol ke Gemini

Tab ketiga di halaman notebook, ditaruh paling kiri. Beda dengan tab
SQL/Python, tab ini **berdiri sendiri total** — tidak menyentuh `t_env`,
`flink_runner.LOCK`, atau apapun yang berkaitan dengan Flink. Alasannya:
LLM chat itu gak ada hubungan sama sekali sama eksekusi query Flink, jadi
kenapa harus ikut nyangkut kalau ada SQL/Python job yang lagi macet (atau
sebaliknya)?

Isinya (`api/llm_runner.py`):

- Pakai SDK resmi `google-genai`, bukan SDK lama `google-generativeai`
  (yang sudah gak banyak di-update lagi).
- **Multi-turn**: pakai `client.chats.create(...)`, sebuah objek "chat
  session" dari SDK yang otomatis nyimpen history dan ngirim ulang
  semuanya ke Gemini tiap kali `send_message()` dipanggil — jadi
  percakapannya nyambung, gak berdiri sendiri-sendiri tiap pesan.
- History cuma hidup di memory proses ini (variabel modul `_chat`) —
  hilang kalau server di-restart, sama seperti job history SQL/Python.
- **API key dari `.env`** (`GEMINI_API_KEY`), bukan hardcode — pola yang
  sama dengan `KAFKA_BOOTSTRAP_SERVERS`. Nama model juga dari `.env`
  (`GEMINI_MODEL`, default `gemini-flash-latest` — alias yang selalu
  nunjuk ke model flash terbaru yang direkomendasikan Google, bukan versi
  spesifik seperti `gemini-2.5-flash` yang bisa di-deprecate untuk API
  key baru), biar gampang ganti tanpa edit kode.

Endpoint-nya (`api/main.py`) **sengaja sinkron, TIDAK pakai pola
job_id+polling** seperti `/jobs`/`/py-jobs`:

- `POST /chat` — kirim `{"message": "..."}`, langsung balas
  `{"reply": "..."}`.
- `GET /chat` — ambil history percakapan (dipanggil pas halaman dibuka,
  biar chat gak keliatan kosong kalau di-refresh).
- `DELETE /chat` — reset percakapan (tombol "Reset percakapan" di UI).

Kenapa boleh sinkron padahal `/jobs` sengaja async+polling? Karena alasan
`/jobs` pakai pola itu adalah job Flink bisa lama DAN bisa saling
nge-block lewat `LOCK` bersama. Panggilan ke Gemini cuma makan beberapa
detik dan gak menyentuh `LOCK` apapun, jadi request biasa (tunggu
responsnya langsung) sudah cukup — gak perlu kerumitan job_id/polling
buat sesuatu yang gak ada masalah yang perlu diselesaikan lewat itu.

Sebelum jalan, isi `.env` (lihat [Environment
Variables](#environment-variables)):
```
GEMINI_API_KEY=...
GEMINI_MODEL=gemini-flash-latest
```
Bikin API key di https://aistudio.google.com/apikey kalau belum punya.

#### Session Manager -- multi-session, mode batch/streaming

Sebelum bisa pakai tab SQL/Python, user harus bikin (atau pilih) **session**
dulu lewat panel di atas notebook (`api/session_manager.py`). Ini beda dari
desain awal yang cuma punya SATU `TableEnvironment` untuk seluruh proses
FastAPI (semua tab/user berbagi state yang sama):

- Tiap session punya `TableEnvironment`, job history (SQL & Python), dan
  namespace Python **sendiri-sendiri** -- tabel yang dibuat di session A
  TIDAK PERNAH kelihatan dari session B. Sudah dites langsung: bikin tabel di
  satu session, query dari session lain, hasilnya kosong.
- Tab SQL dan tab Python DALAM SATU session tetap berbagi `TableEnvironment`
  yang sama seperti sebelumnya.
- **Mode dipilih saat bikin session**: `streaming` (default,
  `EnvironmentSettings.in_streaming_mode()`) atau `batch`
  (`in_batch_mode()`). Tidak bisa diganti setelah session dibuat -- kalau
  butuh mode lain, buat session baru. Batch cocok buat backfill/testing data
  historis; hasil agregat (COUNT/SUM/dst) langsung satu baris final, tidak
  ada baris update/retraction seperti streaming.
- Jumlah session aktif dibatasi (`SESSION_LIMIT` di `session_manager.py`,
  default 5) -- tiap session pegang `TableEnvironment` (resource Flink)
  sendiri, jadi tidak boleh dibuat tanpa batas.
- Panel **"Tabel di session ini"** nampilin semua table/view + kolom + DDL
  asli (lewat `SHOW CREATE TABLE`/`VIEW`) di session yang aktif, auto-refresh
  tiap ada cell yang sukses.
- Panel **"Daftar connector yang tersedia"** (`api/connectors.py`) nampilin
  connector siap-pakai (datagen, filesystem, print, blackhole, Kafka) dengan
  template `CREATE TABLE` yang tinggal diklik jadi cell baru.

#### Submit Job -- job Flink yang jalan SELAMANYA di background

Cell SQL biasa (tombol **Run**) itu buat coba-coba: begitu hasilnya
selesai/preview, jobnya berhenti. Kadang butuh sebaliknya -- misal
`INSERT INTO ... SELECT` dari Kafka yang memang harus terus jalan. Tombol
**Submit Job** (`api/background_jobs.py`) buat itu:

- Statement `INSERT INTO` di cell disubmit TANPA `.wait()` -- job-nya jalan
  di belakang layar selamanya, tidak menggantung request/cell manapun.
- Setiap job dapat **Flink Job ID asli** dan bisa dicek statusnya
  (`RUNNING`/`FINISHED`/`FAILED`/`CANCELED`) lewat `TableResult.get_job_client()`
  bawaan Flink -- mekanisme yang sama dipakai Flink Web UI/JobManager
  sungguhan, cuma di sini ditampilkan di panel **"Background Jobs"**
  (auto-refresh tiap 4 detik) dengan tombol **Stop**.
- Kalau satu cell isi **lebih dari satu** `INSERT INTO`, semuanya digabung
  jadi **SATU job** lewat `t_env.create_statement_set()` -- berbagi
  pembacaan source yang sama, bukan jadi job terpisah sendiri-sendiri.
- Sengaja dibatasi ke cell SQL saja, TIDAK untuk cell Python: loop Python
  (`while True: ...` lewat `exec()`) tidak punya mekanisme cancel yang aman
  seperti `job_client.cancel()`.
- **Temuan penting (dites langsung pakai PyFlink asli):** tiap job `INSERT`
  ternyata jalan di **MiniCluster-nya sendiri-sendiri** (bukan satu cluster
  dibagi rata per session). Begitu job berhenti (termasuk karena di-cancel),
  MiniCluster job itu ikut mati, dan `get_job_status()` sesudahnya SELALU
  gagal dengan `IllegalStateException: MiniCluster ... already shut down`.
  Job lain yang masih `RUNNING` di session yang sama tidak terpengaruh sama
  sekali. Makanya `stop()` langsung nandain status `CANCELED` begitu
  `cancel()` sukses, BUKAN nge-query ulang ke cluster yang sudah tutup.
- **Keterbatasan yang jujur perlu diketahui**: ini masih proses embedded --
  kalau `uvicorn` mati/di-restart, SEMUA background job ikut mati. Bukan
  Flink session cluster beneran (lihat [Langkah Berikutnya](#langkah-berikutnya)).

#### Generate SQL (grounded) -- agent LLM yang beneran baca skema session

Selain fence-detection biasa di LLM Chat (blok ` ```sql `/` ```python ` dapat
tombol "Kirim ke Notebook"), ada mode kedua: checkbox **"Generate SQL
(grounded ke session aktif)"**. Bedanya dengan chat bebas:

- Ini **agent ringan** (bukan chat biasa) -- `llm_runner.generate_sql()`
  pakai native function-calling `google-genai` dengan dua tools READ-ONLY
  yang nempel ke `TableEnvironment` session yang aktif:
  `describe_table(name)` (lihat daftar tabel atau skema+DDL satu tabel) dan
  `preview_rows(sql, limit)` (jalankan SELECT terbatas, lihat contoh data
  asli). Model dipaksa PAKAI tools ini dulu sebelum jawab, bukan menebak
  nama tabel/kolom dari instruksi umum.
- Tidak ada tool buat `INSERT`/`CREATE`/`DROP` -- agent ini cuma bisa
  MEMBACA, tidak pernah bisa mengubah session dengan sendirinya.
- Hasilnya tetap cuma disisipkan sebagai cell baru (lewat jalur render yang
  sama dengan fence-detection) -- user tetap harus klik Run sendiri.

## Istilah Penting

| Istilah | Arti singkat |
|---|---|
| **Table API** | Cara pakai Flink lewat konsep tabel + SQL, bukan operasi stream level rendah. Yang dipakai di semua contoh ini. |
| **Batch vs Streaming mode** | Batch = data dianggap sudah lengkap, dihitung sekali sampai selesai. Streaming = data bisa terus masuk, hasil bisa berubah (changelog). |
| **Bounded vs unbounded stream** | Bounded = stream yang ada ujungnya (misal: baca file lalu selesai). Unbounded = stream tanpa akhir (misal: Kafka topic yang terus menerima data). Semua contoh sejauh ini masih bounded. |
| **Connector** | "Plugin" yang menentukan Flink baca/tulis dari mana (`filesystem`, `kafka`, `jdbc`, dst). |
| **Source** | Table yang datanya **dibaca** dari connector. |
| **Sink** | Table yang datanya **ditulis** ke connector. |
| **Changelog / retraction** | Di streaming, baris hasil agregasi bisa "ditarik kembali" dan diganti versi baru saat data berubah. |
| **`.wait()`** | Memaksa Python menunggu sampai job Flink (biasanya `INSERT INTO`) selesai, karena default-nya async. |
| **`.to_pandas()`** | Menarik hasil sebuah `Table` jadi pandas DataFrame di memory Python. Praktis untuk hasil kecil (skip bikin sink table), tapi tidak cocok untuk data besar/stream tanpa akhir karena semuanya ditarik sekaligus ke memory. |
| **`pipeline.jars`** | Config buat mendaftarkan JAR connector (misal Kafka) yang tidak ikut ter-bundle di `pip install apache-flink`, lewat `t_env.get_config().set("pipeline.jars", "file://...")`. |
| **`format = 'raw'`** | Opsi connector yang membaca seluruh isi pesan (misal dari Kafka) apa adanya sebagai satu kolom STRING, tanpa asumsi struktur (JSON/Avro/dst). Dipakai saat skema pesan belum diketahui. |
| **Notebook (di `api/`)** | Beberapa cell SQL yang berbagi satu `TableEnvironment` yang sama, jadi tabel yang dibuat di satu cell tetap bisa dipakai di cell lain — beda dengan versi awal `api/` yang bikin `TableEnvironment` baru tiap job. |
| **Preview (row limit)** | Pembatasan jumlah baris yang ditarik dari SELECT (`PREVIEW_ROW_LIMIT` di `flink_runner.py`) supaya SELECT dari source unbounded tidak menggantung selamanya dan tidak nge-block job lain di notebook yang sama. |
| **`exec()`** | Fungsi Python bawaan buat menjalankan string kode Python. Dipakai di `python_runner.py` untuk mengeksekusi isi cell tab Python — ini yang bikin kode APAPUN bisa dijalankan (lihat peringatan keamanan di [Tab Python](#tab-python-di-api--notebook-kode-bukan-cuma-sql)). |
| **Session** | Satu "kernel" notebook sendiri-sendiri (mirip kernel Jupyter) -- punya `TableEnvironment`, job history, dan namespace Python sendiri, terisolasi dari session lain. Lihat [Session Manager](#session-manager----multi-session-mode-batchstreaming). |
| **`JobClient`** | Objek dari Flink yang didapat lewat `TableResult.get_job_client()` setelah `execute_sql()` sebuah `INSERT` -- punya Job ID asli, `get_job_status()`, dan `cancel()`. Ada walau masih embedded/tanpa cluster beneran; ini yang dipakai fitur [Submit Job](#submit-job----job-flink-yang-jalan-selamanya-di-background). |
| **`StatementSet`** | `t_env.create_statement_set()` -- cara menggabungkan beberapa `INSERT INTO` jadi SATU job Flink yang berbagi pembacaan source, bukan job terpisah sendiri-sendiri. |
| **MiniCluster per job** | Temuan lewat testing: tiap statement `INSERT` yang dieksekusi di mode embedded ini jalan di MiniCluster-nya SENDIRI, bukan berbagi satu cluster per session/proses. Begitu job itu berhenti (selesai/gagal/cancel), MiniCluster-nya ikut mati dan tidak bisa dicek statusnya lagi -- job lain tidak terpengaruh. |

## Langkah Berikutnya

Urutan yang direkomendasikan dari sini:

1. ~~Source dari file~~ ✅ (`hello_flink_file.py`)
2. ~~Sink ke file~~ ✅ (`hello_flink_sink.py`)
3. ~~UI + API buat submit SQL~~ ✅ (`api/`)
4. ~~CSV tanpa sink table~~ ✅ (`hello_flink_to_csv.py`)
5. ~~Kafka connector~~ ✅ (`hello_flink_kafka.py`) — dikerjakan lebih dulu
   dari event time/windowing di bawah karena kebutuhannya muncul duluan;
   masih pakai `format = 'raw'` (baca pesan mentah), belum event time asli.
6. ~~Notebook Python di `api/`~~ ✅ (`api/python_runner.py` + tab "Python
   Notebook") — dikerjakan di luar urutan juga, karena kebutuhannya
   (variabel/logic Python, bukan cuma SQL) muncul begitu tabel yang dibuat
   lewat CLI script dan lewat UI ternyata tidak nyambung satu sama lain.
7. ~~Session Manager~~ ✅ (`api/session_manager.py`) — multi-session dengan
   `TableEnvironment` terisolasi, mode batch/streaming dipilih per session.
8. ~~Submit Job / background jobs~~ ✅ (`api/background_jobs.py`) — job
   `INSERT INTO` bisa disubmit jalan selamanya di background, dilacak +
   bisa di-Stop lewat `JobClient` bawaan Flink.
9. ~~Generate SQL (grounded)~~ ✅ (`llm_runner.generate_sql()`) — agent LLM
   ringan dengan tools `describe_table`/`preview_rows` yang beneran baca
   skema session aktif, bukan menebak.
10. **Event time & watermark** — konsep inti streaming yang belum
   tersentuh sama sekali di contoh sekarang (source Kafka di atas masih
   pakai processing time, bukan event time dari pesannya sendiri). Ini
   kunci untuk paham kenapa Flink beda dari batch biasa saat datanya
   benar-benar tidak pernah berhenti.
11. **Windowing** (tumbling/sliding window) — begitu ada event time,
   agregasi per interval waktu (per menit/jam) jadi langkah lanjutan
   yang wajar. Cocok dicoba di atas topic Kafka yang sudah bisa dibaca.
12. **Format pesan Kafka** — kalau skema pesan di `topic_kafka` sudah
   diketahui (misal JSON), ganti `format = 'raw'` di
   `hello_flink_kafka.py` jadi `'json'` biar field-fieldnya otomatis jadi
   kolom terpisah, bukan satu string mentah.
13. **Python UDF** — begitu logic makin kompleks (misal split kata
    langsung di SQL, bukan ditarik ke Python dulu seperti sekarang), UDF
    jadi diperlukan.
14. **Job history persisten & Flink session cluster beneran** — sekarang
    daftar job di `api/` hilang begitu server FastAPI di-restart (disimpan
    di dictionary Python biasa), dan TIDAK ADA Flink Web UI/dashboard sama
    sekali di mode embedded ini (sudah dites langsung: `rest.port` gak
    kebuka apapun, karena tiap job punya MiniCluster sendiri -- lihat
    catatan di [Submit Job](#submit-job----job-flink-yang-jalan-selamanya-di-background)).
    Sengaja BELUM dikerjakan untuk prototipe ini (nambah kompleksitas
    operasional -- Docker/proses terpisah -- yang berisiko pas demo live).
    Langkah lanjutan kalau dibutuhkan: simpan job history ke SQLite biar
    persisten, atau connect ke **Flink session cluster** beneran (lewat
    setup **Dinky + Flink** di `template.env`) biar dapat dashboard visual
    dan job tetap jalan walau server API-nya mati.

<!-- Generated by readme-generator skill -->
