## NBA Medallion Data Pipeline
# Overview
This project implementes an end-to-end data pipeline that ingests NBA game logs, models them using a medallion architecture (bronze -> silver -> Gold), and serves analytics-ready features through a Postgres warehouse and Streamlit dashboard.

# Architecture
flowchart TD
  A[nba_api / stats.nba.com] --> B[(Bronze: raw game logs)]
  B --> C[Silver: cleaned & standardized view]
  C --> D[Gold: rolling features + opponent ranks]
  D --> E[Streamlit Dashboard]
  C --> F[GitHub Actions Scheduler]
  F --> D

# Medallion Layers
Bronze
Silver
Gold

# Tech Stack
* Python
* Postgres (Neon)
* SQLAlchemy
* nba_api
* GitHub Actions
* Streamlit
* SQL window functions

# Dashboard
Deployed Streamlit app reading directly from Gold layer
Check it out here: https://nba-performance-project.streamlit.app/

Features:
* Player-level rolling performance trends
* Oponent defensive rank context
* Live warehouse-backed queries