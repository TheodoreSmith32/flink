"""
Sama seperti hello_flink.py (word count), tapi hasilnya ditulis ke CSV TANPA
bikin sink table (tanpa CREATE TABLE ... WITH ('connector'='filesystem', ...)
seperti di hello_flink_sink.py).

Caranya: tarik hasil query Table API ke pandas DataFrame lewat .to_pandas(),
lalu tulis CSV pakai pandas biasa (df.to_csv()). Flink cuma dipakai untuk
BAGIAN HITUNGnya (SELECT ... GROUP BY), sisanya (nulis file) pakai pandas.

Bedanya dengan sink 'filesystem':
- Hasilnya SATU file CSV, bukan folder isi beberapa file part-*.
- Tidak perlu bersihkan folder tujuan dulu (shutil.rmtree) tiap run.
- Tidak perlu .wait() -- .to_pandas() sudah pasti nunggu hasilnya selesai
  dihitung sebelum dikembalikan ke Python.

Trade-off: cara ini cuma masuk akal kalau hasilnya CUKUP KECIL buat ditarik
semua ke memory Python (DataFrame). Untuk data besar/beneran unbounded,
sink 'filesystem' (atau Kafka, dst) di hello_flink_sink.py tetap caranya
yang benar.

Jalankan:
    source .venv/bin/activate
    python hello_flink_to_csv.py
"""

import os

from pyflink.table import EnvironmentSettings, TableEnvironment

env_settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(env_settings)

kalimat = [
    "flink itu keren",
    "belajar flink pakai python",
    "python dan flink keren banget",
]
kata_kata = [(kata,) for baris in kalimat for kata in baris.split(" ")]

t_env.create_temporary_view(
    "kata_table",
    t_env.from_elements(kata_kata, ["kata"]),
)

hasil = t_env.sql_query("""
    SELECT kata, COUNT(*) AS jumlah
    FROM kata_table
    GROUP BY kata
    ORDER BY jumlah DESC
""")

# Ini yang menggantikan CREATE TABLE sink + INSERT INTO: tarik hasilnya
# langsung jadi pandas DataFrame, lalu tulis CSV pakai pandas.
df = hasil.to_pandas()

output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "hasil_jumlah_kata.csv")
df.to_csv(output_path, index=False)

print(f"Hasil ditulis ke: {output_path}\n")
print(df)
