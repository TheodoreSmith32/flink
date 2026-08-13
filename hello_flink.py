"""
Contoh paling sederhana PyFlink: baca data dari list Python (bukan file/Kafka),
lalu hitung jumlah kemunculan tiap kata (word count klasik), print hasilnya ke terminal.

Jalankan: python hello_flink.py
"""

from pyflink.table import EnvironmentSettings, TableEnvironment

# 1. Buat environment untuk batch processing (paling mudah untuk belajar dulu)
env_settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(env_settings)

# 2. Data mentah: anggap ini "baris log" yang mau diolah
kalimat = [
    "flink itu keren",
    "belajar flink pakai python",
    "python dan flink keren banget",
]

# 3. Pecah tiap kalimat jadi kata (dilakukan di Python dulu, biar SQL-nya simpel)
kata_kata = [(kata,) for baris in kalimat for kata in baris.split(" ")]

# 4. Bikin table dari list kata tersebut
t_env.create_temporary_view(
    "kata_table",
    t_env.from_elements(kata_kata, ["kata"]),
)

# 5. Hitung jumlah kemunculan tiap kata
hasil = t_env.sql_query("""
    SELECT kata, COUNT(*) AS jumlah
    FROM kata_table
    GROUP BY kata
    ORDER BY jumlah DESC
""")

# 6. Print hasilnya
hasil.execute().print()
