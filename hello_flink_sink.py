"""
Belajar SINK. Sebelumnya hasil cuma di-print ke terminal (hilang begitu
program selesai). Di sini hasil hitung kata ditulis ke FILE lewat connector
'filesystem' yang sama seperti di hello_flink_file.py -- cuma sekarang
dipakai untuk WRITE, bukan READ.

Jalankan:
    source .venv/bin/activate
    python hello_flink_sink.py
"""

import os
import shutil

from pyflink.table import EnvironmentSettings, TableEnvironment

env_settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(env_settings)

base_dir = os.path.dirname(os.path.abspath(__file__))
data_path = os.path.join(base_dir, "data", "kalimat.csv")
output_path = os.path.join(base_dir, "data", "hasil_jumlah_kata")

# Flink menolak menulis ke folder yang isinya sudah ada, jadi bersihkan dulu
# supaya script ini bisa dijalankan berkali-kali tanpa error
if os.path.exists(output_path):
    shutil.rmtree(output_path)

# 1. Source: baca kalimat dari file, sama seperti hello_flink_file.py
t_env.execute_sql(f"""
    CREATE TABLE kalimat_table (
        kalimat STRING
    ) WITH (
        'connector' = 'filesystem',
        'path' = '{data_path}',
        'format' = 'csv'
    )
""")

baris_baris = [row[0] for row in t_env.sql_query("SELECT kalimat FROM kalimat_table").execute().collect()]
kata_kata = [(kata,) for kalimat in baris_baris for kata in kalimat.split(" ")]

t_env.create_temporary_view(
    "kata_table",
    t_env.from_elements(kata_kata, ["kata"]),
)

# 2. Sink: daftarkan folder tujuan sebagai table juga, bedanya ini table untuk
#    DITULISI, bukan dibaca. Kolomnya harus cocok dengan hasil SELECT di bawah.
t_env.execute_sql(f"""
    CREATE TABLE hasil_table (
        kata STRING,
        jumlah BIGINT
    ) WITH (
        'connector' = 'filesystem',
        'path' = '{output_path}',
        'format' = 'csv'
    )
""")

# 3. INSERT INTO ... SELECT inilah yang benar-benar menjalankan job dan
#    menulis ke file. execute_sql() untuk INSERT berjalan async, makanya
#    kita panggil .wait() biar script tidak keluar sebelum job selesai nulis.
t_env.execute_sql("""
    INSERT INTO hasil_table
    SELECT kata, COUNT(*) AS jumlah
    FROM kata_table
    GROUP BY kata
""").wait()

# 4. Buktikan hasilnya benar-benar tersimpan di disk: baca ulang file yang
#    baru saja ditulis Flink, bukan print dari hasil query di memory.
print(f"Hasil ditulis ke: {output_path}\n")
for filename in sorted(os.listdir(output_path)):
    filepath = os.path.join(output_path, filename)
    if os.path.isfile(filepath):
        with open(filepath) as f:
            print(f.read(), end="")
