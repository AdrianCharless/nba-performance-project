CREATE TABLE IF NOT EXISTS gold.pipeline_run_log (
    run_id UUID PRIMARY KEY,
    layer TEXT NOT NULL,
    rows_processed INTEGER,
    status TEXT NOT NULL,
    runtime_seconds NUMERIC,
    error_message TEXT,
    executed_at TIMESTAMPTZ DEFAULT now()
);
