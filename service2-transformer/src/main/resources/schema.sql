CREATE TABLE IF NOT EXISTS status_mapping (
    raw_status VARCHAR(10) PRIMARY KEY,
    mapped_status VARCHAR(20) NOT NULL
);