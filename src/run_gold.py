import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def main():
    load_dotenv()
    engine = create_engine(os.environ["DATABASE_URL"])

    root = Path(__file__).resolve().parents[1]
    sql_path = root / "sql" / "gold_models.sql"

    sql = sql_path.read_text(encoding="utf-8").strip()
    if not sql:
        raise RuntimeError(f"Error in SQL file: {sql_path}")
    
    with engine.begin() as conn:
        conn.execute(text(sql))

    print("Gold layer create/updated successfully: gold.opponents_ranks, gold.player_features")

if __name__ == "__main__":
    main()