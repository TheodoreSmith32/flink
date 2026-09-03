-- Tabel sink hasil deteksi anomali trip per window 15 menit
-- (jobs/hackatown/flink_sql_traffic_anomaly.py) -- BEDA dari
-- traffic_weather_result (hasil join mentah per-trip, dipakai
-- flink_sql_interval_join.py yang TIDAK diubah oleh script anomali ini).
--
-- CREATE DATABASE/GRANT diulang persis dari traffic_weather_result.sql
-- (bukan cuma USE) supaya file ini AMAN dijalankan sendirian atau lebih
-- dulu -- MySQL menjalankan *.sql di docker-entrypoint-initdb.d urut
-- ALFABET pas volume kosong, dan "traffic_anomaly..." < "traffic_weather..."
-- jadi file ini bisa kejalan duluan. CREATE DATABASE IF NOT EXISTS/GRANT
-- keduanya idempotent, aman diulang.
CREATE DATABASE IF NOT EXISTS traffic_weather;
GRANT ALL PRIVILEGES ON traffic_weather.* TO 'mysql'@'%';
FLUSH PRIVILEGES;

USE traffic_weather;

-- PRIMARY KEY komposit (weather_location_id, window_end) -- alasan sama
-- kayak traffic_weather_result.sql: bikin connector jdbc Flink otomatis
-- pindah ke mode UPSERT (ON DUPLICATE KEY UPDATE), idempotent kalau job
-- di-restart & window yang sama diproses ulang (source Kafka-nya
-- earliest-offset).
CREATE TABLE IF NOT EXISTS traffic_anomaly_result (
    weather_location_id INT NOT NULL,
    window_end DATETIME(3) NOT NULL,
    trip_count BIGINT NOT NULL,
    avg_fare DOUBLE,
    weather_condition VARCHAR(50),
    -- Angka pembanding yang dipakai job buat mutusin is_anomaly -- disimpan
    -- juga di sini (bukan cuma di kode Flink) biar Metabase bisa nunjukin
    -- "actual vs expected" langsung tanpa hardcode ulang tabel referensi.
    expected_trip_count BIGINT,
    is_anomaly BOOLEAN NOT NULL,
    -- Kolom ini TIDAK ada di DDL sink table Flink -- sama seperti
    -- traffic_weather_result.sql, sengaja diisi otomatis MySQL pas INSERT,
    -- berguna buat filter "data masuk kapan" di Metabase.
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (weather_location_id, window_end)
);
