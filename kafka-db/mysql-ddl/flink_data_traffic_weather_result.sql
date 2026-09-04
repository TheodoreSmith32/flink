-- Tabel sink hasil interval join trip_events x weather_events
-- (jobs/hackatown/flink_sql_interval_join.py), versi disimpan di database
-- `flink_data` (dibuat manual lewat DBeaver) -- BUKAN `traffic_weather` yang
-- dipakai kafka-db/mysql-ddl/traffic_weather_result.sql, itu sink terpisah,
-- biarin apa adanya, bukan diganti.

GRANT ALL PRIVILEGES ON flink_data.* TO 'mysql'@'%';
FLUSH PRIVILEGES;

USE flink_data;

-- PRIMARY KEY (id) WAJIB ada -- ini yang bikin connector 'jdbc' Flink
-- otomatis pindah ke mode UPSERT (ON DUPLICATE KEY UPDATE), bukan INSERT
-- polos. Perlu karena job sumbernya baca dari scan.startup.mode =
-- earliest-offset: kalau job di-restart, trip yang sama bisa kebaca ulang
-- dari Kafka -- tanpa PK, baris yang sama bakal dobel di MySQL.
CREATE TABLE IF NOT EXISTS traffic_weather_result (
    id BIGINT PRIMARY KEY,
    pickup_datetime DATETIME(3) NOT NULL,
    pu_location_id INT NOT NULL,
    pu_location_name VARCHAR(100) NOT NULL,
    weather_location_id INT,
    trip_distance DOUBLE,
    fare_amount DOUBLE,
    weather_condition VARCHAR(50),
    precipitation DOUBLE,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
