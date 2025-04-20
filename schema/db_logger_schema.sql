-- etl_logs
CREATE TABLE IF NOT EXISTS etl_logs (
    log_id SERIAL PRIMARY KEY,
    step VARCHAR(100),
    status TEXT,
    source VARCHAR(100),
    table_name VARCHAR(100),
    etl_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- invalid_ids (new)
DROP TABLE IF EXISTS invalid_ids;

CREATE TABLE invalid_ids (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(50),
    entity_type VARCHAR(50),
    object_id VARCHAR(50),
    extra_entity_type_1 VARCHAR(50),
    extra_object_id_1 VARCHAR(50),
    logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

