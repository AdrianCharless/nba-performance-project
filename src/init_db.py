import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def main():
    load_dotenv()
    db_url = os.environ["DATABASE_URL"]
    engine = create_engine(db_url)

    root = Path(__file__).resolve().parents[1]
    sql_path = root / "sql" / "00_schema.sql"
    sql = sql_path.read_text(encoding="utf-8")

    with engine.begin() as conn:
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.execute(text(stmt))

    print("Create Medallion Schemas: bronze, silver, gold")

if __name__ == "__main__":
    main()