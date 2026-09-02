-- Tabel sumber buat Debezium CDC (connector trip-events-postgres-cdc)
-- Kolom sengaja dibuat sama persis dengan AVRO_SCHEMA_DICT di
-- producer/trip_events_producer.py, biar bisa dibandingkan langsung
-- dengan versi Avro manual-nya.
CREATE TABLE IF NOT EXISTS public.trip_events (
    id BIGINT PRIMARY KEY,
    pickup_datetime TIMESTAMP NOT NULL,
    pu_location_id INT NOT NULL,
    pu_location_name TEXT NOT NULL,
    trip_distance DOUBLE PRECISION NOT NULL,
    fare_amount DOUBLE PRECISION NOT NULL
);
