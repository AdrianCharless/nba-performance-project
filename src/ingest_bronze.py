import os
import time
from datetime import datetime, timezone
import random
import pandas as pd
from nba_api.stats.endpoints import leaguegamelog
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

SEASONS = ["2024-25"]

def fetch_season(season: str, max_retries: int = 5, base_sleep: float = 3.0) -> pd.DataFrame:
    """
    Fetch season data with retries + backoff because stats.nba.com can be slow/rate-limited,
    especially from CI runner IPs.
    """
    last_err = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = leaguegamelog.LeagueGameLog(
                season=season,
                season_type_all_star="Regular Season",
                player_or_team_abbreviation="P",
                timeout=180,  # was 60; CI often needs more
            )
            df = resp.get_data_frames()[0]
            df["SEASON"] = season
            df["INGESTED_AT"] = datetime.now(timezone.utc)
            return df

        except Exception as e:
            last_err = e
            # exponential backoff + jitter
            sleep_s = base_sleep * (2 ** (attempt - 1)) + random.uniform(0, 1.5)
            print(f"⚠️ fetch failed for {season} (attempt {attempt}/{max_retries}): {e}")
            print(f"   sleeping {sleep_s:.1f}s then retrying...")
            time.sleep(sleep_s)

    raise RuntimeError(f"Failed to fetch season {season} after {max_retries} retries") from last_err

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