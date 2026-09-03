-- Tabel sink hasil interval join trip_events x weather_events
-- (jobs/hackatown/flink_sql_interval_join.py) -- BUKAN tabel source CDC,
-- itu beda file (kafka-db/postgres-ddl/trip_events.sql).
--
-- Sengaja bikin database sendiri (traffic_weather), bukan numpang di
-- database "mysql" yang di-set lewat MYSQL_DATABASE di docker-compose --
-- nama itu collide sama system schema bawaan MySQL sendiri (mysql.user,
-- mysql.db, dst), jadi bahaya kalau tabel aplikasi ditaro di situ juga.
CREATE DATABASE IF NOT EXISTS traffic_weather;

-- User `mysql` (dari MYSQL_USER di docker-compose) defaultnya cuma dikasih
-- akses ke database `mysql` oleh entrypoint image -- perlu grant manual ke
-- database baru ini biar job Flink bisa connect pakai kredensial yang sama.
GRANT ALL PRIVILEGES ON traffic_weather.* TO 'mysql'@'%';
FLUSH PRIVILEGES;

USE traffic_weather;

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
    -- Nama kolom `weather_condition`, bukan `condition` -- CONDITION reserved
    -- keyword juga di MySQL (dipakai di handler DECLARE ... CONDITION),
    -- jadi dihindari dari awal lewat alias di SELECT Flink-nya, bukan
    -- di-backtick di sana-sini.
    weather_condition VARCHAR(50),
    precipitation DOUBLE,
    -- Kolom ini TIDAK ada di DDL sink table Flink -- sengaja, biar Flink
    -- gak perlu tau soal ini sama sekali. MySQL yang isi otomatis pas INSERT,
    -- berguna buat filter "data masuk kapan" di Metabase.
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
