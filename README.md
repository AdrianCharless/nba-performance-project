# nba-performance-project
NBA Player Performance Forecasting & Anomaly Detection System

flowchart TD
  A[nba_api] --> B[ingest.py]
  B --> C[(SQLite nba.db)]
  C --> D[features.py]
  D --> E[train_model.py]
  E --> F[test_with_preds.csv]
  F --> G[detect_anomalies.py]
  G --> H[anomalies.csv]
  H --> I[viz.py]
