import time
import sqlite3
from pathlib import Path

import pandas as pd
from nba_api.stats.endpoints import leaguegamelog

def fetch_player_game_logs(season: str) -> pd.DataFrame:
    player_logs = leaguegamelog.LeagueGameLog(
        season=season,
        season_type_all_star="Regular Season",
        player_or_team_abbreviation="P",
    )
    df = player_logs.get_data_frames()[0]
    df["SEASON"] = season
    return df

def fetch_multiple_season_logs(seasons) -> pd.DataFrame:
    all_dfs = []
    for i in seasons:
        df_i = fetch_player_game_logs(i)
        all_dfs.append(df_i)
        # sleep for api to not be overworked
        time.sleep(1)
    return pd.concat(all_dfs, ignore_index=True)

def write_to_sqlite(df: pd.DataFrame, db_path: str= "nba.db", table_name: str = "game_logs"):
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    print("Done writing to SQLite.")


if __name__ == "__main__":
    # Project Root
    root = Path(__file__).resolve().parents[1]

    # Seasons ran by project
    seasons = [
        "2024-25",
        "2023-24",
    ]

    df_logs = fetch_multiple_season_logs(seasons)

    # save raw data as a csv
    raw_dir = root / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    csv_path = raw_dir / "player_game_logs.csv"
    df_logs.to_csv(csv_path, index=False)    
    print(f"Saved raw logs to {csv_path}")

    # write to SQLite DB
    db_path = root / "nba.db"
    write_to_sqlite(df_logs, db_path=str(db_path))