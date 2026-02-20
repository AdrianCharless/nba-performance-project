import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def main():
    load_dotenv()
    db_url = os.environ["DATABASE_URL"]
    engine = create_engine(db_url)

    root = Path(__file__).resolve().parents[1]

    sql_files = [
        root / "sql" / "00_schema.sql",
        root / "sql" / "01_pipeline_run_log.sql",
    ]

    with engine.begin() as conn:
        for p in sql_files: 
            sql = p.read_text(encoding="utf-8").strip()
            if not sql:
                continue
            print(f"Running {p.name}")
            conn.execute(text(sql))

    print("Create Medallion Schemas: bronze, silver, gold")

if __name__ == "__main__":
    main()