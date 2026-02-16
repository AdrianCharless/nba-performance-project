import os
import time
from datetime import datetime, timezone, timedelta, date
import random

import pandas as pd
from nba_api.stats.endpoints import leaguegamelog
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

SEASONS = ["2025-26","2024-25"]
FULL_REFRESH = os.getenv("FULL_REFRESH", "0") == "1"
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))  # CI uses 3–7 days

def date_window() -> tuple[str, str]:
    end = date.today()
    start = end - timedelta(days=LOOKBACK_DAYS)
    return start.isoformat(), end.isoformat()


def fetch_season(season: str) -> pd.DataFrame:
    start_date = (date.today() - timedelta(days=LOOKBACK_DAYS)).isoformat()
    end_date = date.today().isoformat()

    if FULL_REFRESH:
        print("Running FULL refresh (entire season)...")
        resp = leaguegamelog.LeagueGameLog(
            season=season,
            season_type_all_star="Regular Season",
            player_or_team_abbreviation="P",
            timeout=120,
        )
    else:
        print(f"Running incremental refresh (last {LOOKBACK_DAYS} days)...")
        resp = leaguegamelog.LeagueGameLog(
            season=season,
            season_type_all_star="Regular Season",
            player_or_team_abbreviation="P",
            date_from_nullable=start_date,
            date_to_nullable=end_date,
            timeout=60,
        )

    df = resp.get_data_frames()[0]
    df["SEASON"] = season
    df["INGESTED_AT"] = datetime.now(timezone.utc)

    return df

def main():
    load_dotenv()
    db_url = os.environ["DATABASE_URL"]
    engine = create_engine(db_url)

    print("Fetching Seasons:")

    all_dfs = []
    for s in SEASONS:
        df = fetch_season(s)
        all_dfs.append(df)
        time.sleep(1)

    df_all = pd.concat(all_dfs, ignore_index=True)

    print(f"Rows fetched: {len(df_all)}")

    # Write to temp table first
    df_all.to_sql(
        "player_game_logs_tmp",
        engine,
        schema="bronze",
        if_exists="replace",
        index=False
    )

    with engine.begin() as conn:
        # Create permanent table if it doesn't exist
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bronze.player_game_logs AS
            SELECT * FROM bronze.player_game_logs_tmp WHERE 1=0;
        """))

        # Add unique constraint for idempotency
        conn.execute(text("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'bronze_game_logs_unique'
                ) THEN
                    ALTER TABLE bronze.player_game_logs
                    ADD CONSTRAINT bronze_game_logs_unique
                    UNIQUE ("SEASON","GAME_ID","PLAYER_ID");
                END IF;
            END$$;
        """))

        # Upsert
        conn.execute(text("""
            INSERT INTO bronze.player_game_logs
            SELECT * FROM bronze.player_game_logs_tmp
            ON CONFLICT ("SEASON","GAME_ID","PLAYER_ID")
            DO UPDATE SET
                "INGESTED_AT" = EXCLUDED."INGESTED_AT";
        """))

        conn.execute(text("DROP TABLE bronze.player_game_logs_tmp;"))

    print("Bronze ingestion successfully completed")

if __name__ == "__main__":
    main()