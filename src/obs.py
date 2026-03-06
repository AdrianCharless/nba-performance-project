import time
import uuid
from sqlalchemy import text

def start_run():
    return str(uuid.uuid4()), time.time()

def log_run(engine, run_id, layer, status, runtime, rows=None, error=None):
    err = (str(error)[:2000] if error else None)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO gold.pipeline_run_log
              (run_id, layer, rows_processed, status, runtime_seconds, error_message)
            VALUES
              (:run_id, :layer, :rows, :status, :runtime, :err);
        """), {
            "run_id": run_id,
            "layer": layer,
            "rows": rows,
            "status": status,
            "runtime": runtime,
            "err": err
        })
