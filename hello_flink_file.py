"""
Sama seperti hello_flink.py (word count, mode BATCH), tapi sekarang datanya
TIDAK di-hardcode di Python. Kita baca dari file data/kalimat.csv pakai
konektor "filesystem" milik Flink -- inilah konsep SOURCE yang sebenarnya
dipakai di dunia nyata (nanti tinggal ganti 'connector' jadi 'kafka', dst).

Jalankan: JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 python hello_flink_file.py
"""

import os

from pyflink.table import EnvironmentSettings, TableEnvironment

env_settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(env_settings)

# Path absolut, biar script bisa dijalankan dari folder mana pun
data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "kalimat.csv")

# 1. Daftarkan file CSV sebagai table lewat DDL (CREATE TABLE ... WITH (...)).
#    Tiap baris di file dianggap satu kolom string "kalimat" karena tidak ada
#    koma di isinya -- format csv paling simpel yang bisa dipakai untuk teks bebas.
t_env.execute_sql(f"""
    CREATE TABLE kalimat_table (
        kalimat STRING
    ) WITH (
        'connector' = 'filesystem',
        'path' = '{data_path}',
        'format' = 'csv'
    )
""")

# 2. Ambil semua baris dari table hasil baca file, lalu pecah jadi kata-kata
#    di Python (sama seperti versi sebelumnya) supaya SQL agregasinya simpel.
baris_baris = [row[0] for row in t_env.sql_query("SELECT kalimat FROM kalimat_table").execute().collect()]
kata_kata = [(kata,) for kalimat in baris_baris for kata in kalimat.split(" ")]

t_env.create_temporary_view(
    "kata_table",
    t_env.from_elements(kata_kata, ["kata"]),
)

# 3. Hitung jumlah kemunculan tiap kata, sama seperti hello_flink.py
hasil = t_env.sql_query("""
    SELECT kata, COUNT(*) AS jumlah
    FROM kata_table
    GROUP BY kata
    ORDER BY jumlah DESC
""")
    
hasil.execute().print()
