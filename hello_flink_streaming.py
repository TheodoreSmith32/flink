"""
Sama seperti hello_flink.py, tapi pakai mode STREAMING bukan BATCH.
Datanya masih tetap dari list Python (jadi "bounded stream" / stream yang ada
ujungnya), tapi cara Flink mengeksekusinya beda: baris demi baris muncul begitu
hasil sudah dihitung, bukan menunggu semuanya kelar dulu baru ditampilkan
seperti di batch.

Jalankan: JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 python hello_flink_streaming.py
"""

from pyflink.table import EnvironmentSettings, TableEnvironment

# 1. Bedanya cuma di sini: in_streaming_mode() bukan in_batch_mode()
env_settings = EnvironmentSettings.in_streaming_mode()
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

# Catatan: di streaming mode, hasil agregasi (COUNT, GROUP BY) bisa berubah
# nilainya seiring data baru masuk (namanya "changelog"/retraction), makanya
# tiap kata bisa muncul lebih dari sekali di output print di bawah -- baris
# lama "ditarik" (retract) lalu diganti baris baru dengan angka yang sudah
# diupdate. Ini normal dan salah satu ciri khas stream processing.
hasil = t_env.sql_query("""
    SELECT kata, COUNT(*) AS jumlah
    FROM kata_table
    GROUP BY kata
""")

hasil.execute().print()
