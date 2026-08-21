"""
Sama seperti hello_flink.py (word count), tapi pakai DataStream API bukan
Table API/SQL. Konsepnya beda:
- hello_flink.py: bikin Table dari list, terus tulis SQL (GROUP BY, COUNT).
- Script ini: bikin DataStream dari list yang sama, terus rangkai operator
  satu-satu (flat_map -> map -> key_by -> reduce) -- gak ada SQL/tabel sama
  sekali.

DataStream API cocok kalau kamu perlu kontrol lebih detail atas tiap elemen
data (custom function, custom key, custom window, dst) yang susah/gak bisa
diekspresikan lewat SQL biasa. flink_kafka.py contoh DataStream API juga,
tapi cuma source doang (baca topic) -- script ini nambahin transformasinya.

Jalankan: python hello_flink_datastream.py
"""

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()

kalimat = [
    "flink itu keren",
    "belajar flink pakai python",
    "python dan flink keren banget",
]

# 1. Bikin DataStream langsung dari list Python (bounded stream, sama seperti
#    from_elements() di Table API) -- perlu type_info eksplisit karena
#    DataStream API gak infer skema seperti Table API.
stream = env.from_collection(kalimat, type_info=Types.STRING())

# 2. flat_map: satu elemen masuk -> nol/banyak elemen keluar (kebalikan dari
#    map yang satu-ke-satu). Di hello_flink.py, pecah-kalimat-jadi-kata
#    dilakukan manual di Python SEBELUM masuk ke Flink; di sini dilakukan
#    DI DALAM pipeline Flink-nya sendiri.
kata_stream = stream.flat_map(
    lambda baris: baris.split(" "), output_type=Types.STRING()
)

# 3. Ubah tiap kata jadi pasangan (kata, 1) -- pola umum word count: hitung
#    kemunculan dengan cara nge-sum angka 1 per kemunculan.
pasangan_stream = kata_stream.map(
    lambda kata: (kata, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])
)

# 4. key_by mengelompokkan berdasarkan kata (elemen index 0 di tuple) --
#    mirip GROUP BY kata di versi SQL. reduce menjumlahkan angka 1-nya
#    (elemen index 1) tiap ada kata yang sama masuk.
hasil_stream = pasangan_stream.key_by(lambda pasangan: pasangan[0]).reduce(
    lambda a, b: (a[0], a[1] + b[1])
)

# Catatan: sama seperti versi Table API streaming (hello_flink_streaming.py),
# tiap kata bisa muncul lebih dari sekali di output -- reduce() ngeprint hasil
# SETIAP KALI ada update buat key itu, bukan cuma sekali di akhir.
hasil_stream.print()

env.execute("word_count_datastream")
