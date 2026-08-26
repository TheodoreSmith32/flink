-- Dijalankan otomatis oleh container Postgres (docker-compose.yml) sekali,
-- pas volume-nya masih kosong (lihat docker-entrypoint-initdb.d di image
-- resmi postgres). Kalau kamu ganti skema di sini SETELAH volume terbuat,
-- jalankan manual lewat psql atau `docker compose down -v` dulu buat reset.

CREATE TABLE IF NOT EXISTS fraud_alerts (
    transaction_id  BIGINT PRIMARY KEY,
    account_id      TEXT NOT NULL,
    amount          DOUBLE PRECISION NOT NULL,
    merchant        TEXT,
    txn_count_window BIGINT NOT NULL,
    raw_score       DOUBLE PRECISION NOT NULL,
    flagged_at      TIMESTAMP NOT NULL,
    explanation     TEXT,
    explained_at    TIMESTAMP
);

-- Audit trail application-level -- Flink sendiri tidak punya fitur
-- governance/lineage built-in, jadi ini yang jadi jawaban ke bagian
-- "Flink Data Governance" di demo.
CREATE TABLE IF NOT EXISTS governance_log (
    id              SERIAL PRIMARY KEY,
    transaction_id  BIGINT NOT NULL,
    stage           TEXT NOT NULL,  -- 'flink_flagged' atau 'llm_explained'
    detail          TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT now()
);
