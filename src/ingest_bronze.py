import os
import time
import random
from datetime import datetime, timezone, timedelta, date

import pandas as pd
from nba_api.stats.endpoints import leaguegamelog
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


# ===============================
# Configuration (env-driven)
# ===============================

SEASONS = os.getenv("SEASONS", "2025-26").split(",")
FULL_REFRESH = os.getenv("FULL_REFRESH", "0") == "1"
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))


# ===============================
# Fetch Logic
# ===============================

def fetch_season_once(season: str) -> pd.DataFrame:
    """
    Makes a single API request for a season.
    Used by retry wrapper.
    """
    start_date = (date.today() - timedelta(days=LOOKBACK_DAYS)).isoformat()
    end_date = date.today().isoformat()

    if FULL_REFRESH:
        print(f"[{season}] FULL refresh (entire season)")
        resp = leaguegamelog.LeagueGameLog(
            season=season,
            season_type_all_star="Regular Season",
            player_or_team_abbreviation="P",
            timeout=120,
        )
    else:
        print(
            f"[{season}] Incremental refresh "
            f"(last {LOOKBACK_DAYS} days: {start_date} → {end_date})"
        )
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


def fetch_season(season: str, max_retries: int = 4, base_sleep: float = 2.0) -> pd.DataFrame:
    """
    Retry wrapper with exponential backoff.
    Protects CI from flaky API timeouts.
    """
    last_err = None

    for attempt in range(1, max_retries + 1):
        try:
            return fetch_season_once(season)

        except Exception as e:
            last_err = e
            sleep_s = base_sleep * (2 ** (attempt - 1)) + random.uniform(0, 1.0)
            print(f"⚠️ [{season}] fetch failed (attempt {attempt}/{max_retries}): {e}")
            print(f"   sleeping {sleep_s:.1f}s then retrying...")
            time.sleep(sleep_s)

    raise RuntimeError(
        f"Failed to fetch season {season} after {max_retries} retries"
    ) from last_err


# ===============================
# Main Bronze Load
# ===============================

def main():
    load_dotenv()

    db_url = os.environ["DATABASE_URL"]
    engine = create_engine(db_url)

    print("=== Bronze Ingestion Started ===")
    print(f"Seasons: {SEASONS}")
    print(f"FULL_REFRESH: {FULL_REFRESH}")
    print(f"LOOKBACK_DAYS: {LOOKBACK_DAYS}")

    all_dfs = []

    for season in SEASONS:
        df = fetch_season(season)
        all_dfs.append(df)
        time.sleep(1)  # light throttle between seasons

    if not all_dfs:
        print("No data fetched.")
        return

    df_all = pd.concat(all_dfs, ignore_index=True)
    print(f"Rows fetched: {len(df_all)}")

    # Write to temp table
    df_all.to_sql(
        "player_game_logs_tmp",
        engine,
        schema="bronze",
        if_exists="replace",
        index=False
    )

    # Upsert into permanent table
    with engine.begin() as conn:

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bronze.player_game_logs AS
            SELECT * FROM bronze.player_game_logs_tmp WHERE 1=0;
        """))

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

        conn.execute(text("""
            INSERT INTO bronze.player_game_logs
            SELECT * FROM bronze.player_game_logs_tmp
            ON CONFLICT ("SEASON","GAME_ID","PLAYER_ID")
            DO UPDATE SET
                "INGESTED_AT" = EXCLUDED."INGESTED_AT";
        """))

        conn.execute(text("DROP TABLE bronze.player_game_logs_tmp;"))

    print("=== Bronze ingestion completed successfully ===")


if __name__ == "__main__":
    main()
