import time
import sqlite3
from pathlib import Path

import pandas as pd
from nba_api.stats.endpoints import leaguegamelog

# function to fetch player game logs
def fetch_player_game_logs(season: str) -> pd.DataFrame:
    # using nba_api fetch all player logs from chosen season
    player_logs = leaguegamelog.LeagueGameLog(
        season=season,
        season_type_all_star="Regular Season",
        player_or_team_abbreviation="P",
    )
    # specify index 0
    df = player_logs.get_data_frames()[0]
    df["SEASON"] = season
    return df

# function for multiple seasons
def fetch_multiple_season_logs(seasons) -> pd.DataFrame:
    all_dfs = []
    # for loop to cycle through seasons
    for i in seasons:
        df_i = fetch_player_game_logs(i)
        all_dfs.append(df_i)
        # sleep for api to not be overworked
        time.sleep(1)
        # concatonate all seasons to one dataframe
    return pd.concat(all_dfs, ignore_index=True)

# function to write db to sql 
def write_to_sqlite(df: pd.DataFrame, db_path: str= "nba.db", table_name: str = "game_logs"):
    # connection to db
    conn = sqlite3.connect(db_path)
    # write table game logs to sql and if it already exists then replace
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    # confirm message
    print("Done writing to SQLite.")


if __name__ == "__main__":
    # Project Root
    root = Path(__file__).resolve().parents[1]

    # Seasons ran by project, for this project I just used 2024-2025 and 2023-2024
    seasons = [
        "2024-25",
        "2023-24",
    ]
    # fetch the game logs and store them in df_logs
    df_logs = fetch_multiple_season_logs(seasons)

    # save raw data as a csv 
    # create the directory 
    raw_dir = root / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    # csv path creation
    csv_path = raw_dir / "player_game_logs.csv"
    # save to path
    df_logs.to_csv(csv_path, index=False) 
    # confirm message   
    print(f"Saved raw logs to {csv_path}")

    # write to SQLite DB
    db_path = root / "nba.db"
    write_to_sqlite(df_logs, db_path=str(db_path))