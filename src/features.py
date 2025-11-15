import sqlite3
from pathlib import Path
import pandas as pd

def load_raw_from_sqlite(db_path="nba.db"):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql("SELECT * FROM game_logs", conn)
    conn.close
    return df

def fetch_opponent(matchup: str) -> str:
    if not isinstance(matchup, str):
        return None
    parts = matchup.split()
    if len(parts) >= 3:
        return parts[-1]
    return None

def add_opponent_def_features(df: pd.DataFrame) -> pd.DataFrame:
    df["OPPONENT_TEAM"] = df["MATCHUP"].apply(fetch_opponent)
    opponent_defense = (
        df.groupby(["SEASON", "OPPONENT_TEAM"])
        .agg(
            OPP_PTS_ALLOWED=("PTS", "mean"),
            OPP_REB_ALLOWED=("REB", "mean"),
            OPP_AST_ALLOWED=("AST", "mean"),
        )
        .reset_index()
    )

    opponent_defense["OPP_PTS_ALLOWED_RANK"] = (
        opponent_defense.groupby("SEASON")["OPP_PTS_ALLOWED"]
        .rank(method="dense", ascending=True)
    )
    opponent_defense["OPP_REB_ALLOWED_RANK"] = (
        opponent_defense.groupby("SEASON")["OPP_REB_ALLOWED"]
        .rank(method="dense", ascending=True)
    )
    opponent_defense["OPP_AST_ALLOWED_RANK"] = (
        opponent_defense.groupby("SEASON")["OPP_AST_ALLOWED"]
        .rank(method="dense", ascending=True)
    )

    df = df.merge(
        opponent_defense,
        on=["SEASON", "OPPONENT_TEAM"],
        how="left",
    )

    return df


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])
    df = add_opponent_def_features(df)
    df = df.sort_values(["PLAYER_NAME", "GAME_DATE"])
    group = df.groupby("PLAYER_NAME")

    # Calculation of rolling averages for last 5 games 
    df["PTS_LAST5"] = group["PTS"].rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    df["REB_LAST5"] = group["REB"].rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    df["AST_LAST5"] = group["AST"].rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    df["MIN_LAST5"] = group["MIN"].rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)

    df["PRA_LAST5"] = df["PTS_LAST5"] + df["REB_LAST5"] + df["AST_LAST5"]

    # Target for next game points
    df["PTS_NEXT_GAME"] = group["PTS"].shift(-1)

    df = df.dropna(subset=["PTS_NEXT_GAME"])

    return df

def save_features(df: pd.DataFrame, out_path="data/processed/features.csv"):
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"Saved feature dataset to {out_path}")

if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    db_path = root / "nba.db"

    df_raw = load_raw_from_sqlite(str(db_path))

    df_features = engineer_features(df_raw)

    save_path = root / "data" / "processed" / "features.csv"
    save_features(df_features, str(save_path))

