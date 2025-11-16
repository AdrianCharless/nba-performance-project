# src/viz.py

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def load_anomalies(path: str):
    return pd.read_csv(path)


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    anomalies_path = root / "data" / "processed" / "anomalies.csv"

    df = load_anomalies(str(anomalies_path))

    # Plot 1: predicted vs actual scatter
    plt.figure(figsize=(7, 6))
    sns.scatterplot(x=df["PRED_PTS_NEXT_GAME"], y=df["PTS"], alpha=0.3)
    plt.xlabel("Predicted Points")
    plt.ylabel("Actual Points")
    plt.title("Predicted vs Actual Points")
    plt.grid(True)
    plt.show()

    # Plot 2: residual distribution
    plt.figure(figsize=(7, 6))
    sns.histplot(df["RESIDUAL"], bins=40, kde=True)
    plt.xlabel("Residual (Actual - Predicted)")
    plt.title("Residual Distribution")
    plt.grid(True)
    plt.show()

    # Plot 3: top 20 anomalies
    top20 = df.head(20)
    plt.figure(figsize=(10, 6))
    sns.barplot(x="ABS_RESIDUAL", y="PLAYER_NAME", data=top20, hue="ANOMALY_TYPE")
    plt.xlabel("Absolute Error")
    plt.title("Top 20 NBA Player Performance Anomalies")
    plt.show()