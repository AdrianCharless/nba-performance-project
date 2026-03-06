import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from obs import start_run, log_run
import time

def main():
    load_dotenv()
    engine = create_engine(os.environ["DATABASE_URL"])

    run_id, t0 = start_run()

    root = Path(__file__).resolve().parents[1]
    sql_path = root / "sql" / "silver_player_game_logs.sql"

    try:
        sql = sql_path.read_text(encoding="utf-8").strip()
        if not sql:
            raise ValueError(f"SQL file is empty: {sql_path}")

        with engine.begin() as conn:
            conn.execute(text(sql))

            # Row count for observability (within the same transaction)
            rows = conn.execute(text("SELECT COUNT(*) FROM silver.player_game_logs;")).scalar()

        runtime = time.time() - t0
        log_run(engine, run_id, layer="silver", status="success", runtime=runtime, rows=rows)

        print(f"Silver layer created/updated successfully: silver.player_game_logs (rows={rows})")

    except Exception as e:
        runtime = time.time() - t0
        log_run(engine, run_id, layer="silver", status="failure", runtime=runtime, error=e)
        raise

if __name__ == "__main__":
    main()
