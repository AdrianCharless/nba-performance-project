import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def main():
    load_dotenv()
    engine = create_engine(os.environ["DATABASE_URL"])

    root = Path(__file__).resolve().parents[1]
    sql_path = root / "sql" / "silver_player_game_logs.sql"

    sql = sql_path.read_text(encoding="utf-8")

    with engine.begin() as conn:
        conn.execute(text(sql))

    print("Silver layer created/updated successfully: silver.player_game_logs")

if __name__ == "__main__":
    main()
