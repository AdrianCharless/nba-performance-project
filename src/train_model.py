from pathlib import Path

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split

def load_features(path: str = "data/processed/features.csv") -> pd.DataFrame:
    return pd.read_csv(path)

def train_model(df: pd.DataFrame):
    feature_cols = [
        "PTS_LAST5",
        "REB_LAST5",
        "AST_LAST5",
        "MIN_LAST5",
        "PRA_LAST5",
        "OPP_PTS_ALLOWED_RANK",
        "OPP_REB_ALLOWED_RANK",
        "OPP_PTS_ALLOWED_RANK",
    ]

    x = df[feature_cols]
    y = df["PTS_NEXT_GAME"]

    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=0.2, shuffle=False
    )

    model = RandomForestRegressor(
        n_estimators=300,
        max_depth=None,
        random_state=42,
        n_jobs=-1,
    )

    print("Model is being trained")
    model.fit(x_train, y_train)

    print("Calculating prediction")
    y_pred = model.predict(x_test)
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    print(f"MAE: {mae:.2f}")
    print(f"R^2: {r2:.3f}")

    df_test = df.iloc[len(x_train):].copy()
    df_test["PRED_PTS_NEXT_GAME"] = y_pred

    return model, df_test, feature_cols

def save_artifacts(model, df_test: pd.DataFrame, feature_cols, root: Path):
    models_dir = root / "models"
    models_dir.mkdir(exist_ok=True)

    model_path = models_dir / "rf_pts_predictor.pkl"
    joblib.dump(model, model_path)
    print(f"Saved model to {model_path}")

    processed_dir = root / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    test_path = processed_dir / "test_with_preds.csv"
    df_test.to_csv(test_path, index=False)
    print("Saved test set with predictions to {test_path}")

    feature_list_path = models_dir / "feature_column.txt"
    with open(feature_list_path, "w") as f:
        for col in feature_cols:
            f.write(col + "\n")
    print(f"Saved feature column list to {feature_list_path}")


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]

    features_path = root / "data" / "processed" / "features.csv"
    print(f"Loading features from {features_path}")
    df_features = load_features(str(features_path))

    model, df_test, feature_cols = train_model(df_features)
    save_artifacts(model, df_test, feature_cols, root)