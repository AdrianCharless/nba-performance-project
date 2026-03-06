import os
import time
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from obs import start_run, log_run


def table_exists(conn, schema: str, table: str) -> bool:
    return bool(
        conn.execute(
            text("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = :schema
                      AND table_name = :table
                );
            """),
            {"schema": schema, "table": table},
        ).scalar()
    )


def main():
    load_dotenv()
    engine = create_engine(os.environ["DATABASE_URL"])

    run_id, t0 = start_run()

    root = Path(__file__).resolve().parents[1]
    sql_path = root / "sql" / "gold_models.sql"

    try:
        sql = sql_path.read_text(encoding="utf-8").strip()
        if not sql:
            raise ValueError(f"SQL file is empty: {sql_path}")

        with engine.begin() as conn:
            conn.execute(text(sql))

            opponents_rows = 0
            features_rows = 0

            if table_exists(conn, "gold", "opponents_ranks"):
                opponents_rows = conn.execute(
                    text("SELECT COUNT(*) FROM gold.opponents_ranks;")
                ).scalar()

            if table_exists(conn, "gold", "player_features"):
                features_rows = conn.execute(
                    text("SELECT COUNT(*) FROM gold.player_features;")
                ).scalar()

        total_rows = (opponents_rows or 0) + (features_rows or 0)
        runtime = time.time() - t0

        log_run(engine, run_id, layer="gold", status="success", runtime=runtime, rows=total_rows)

        print(
            f"Gold layer created/updated successfully "
            f"(opponents_ranks={opponents_rows}, player_features={features_rows})"
        )

    except Exception as e:
        runtime = time.time() - t0
        log_run(engine, run_id, layer="gold", status="failure", runtime=runtime, error=e)
        raise


if __name__ == "__main__":
    main()
