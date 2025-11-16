from pathlib import Path
import pandas as pd
import numpy as np

def load_predictions(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    print(f"Loaded {len(df)} rows from {path}")
    return df

def compute_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    df["RESIDUAL"] = df["PTS"] - df["PRED_PTS_NEXT_GAME"]

    df["ABS_RESIDUAL"] = df["RESIDUAL"].abs()

    # z score
    df["RESIDUAL_ZSCORE"] = (
        (df["RESIDUAL"] - df["RESIDUAL"].mean()) /
        df["RESIDUAL"].std(ddof=0)
    )
    
    df["ANOMALY_TYPE"] = np.where(
        df["RESIDUAL"] > 0,
        "OVERPERFORM",
        "UNDERPERFORM"
    )

    df_sorted = df.sort_values("ABS_RESIDUAL", ascending=False)

    return df_sorted

def save_anomalies(df: pd.DataFrame, out_path: str):
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"Saved anomalies to {out_path}")

if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    preds_path = root / "data" / "processed" / "test_with_preds.csv"

    df = load_predictions(str(preds_path))
    df_anom = compute_anomalies(df)

    out_path = root / "data" / "processed" / "anomalies.csv"
    save_anomalies(df_anom, str(out_path))