import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

st.set_page_config(page_title="NBA Warehouse Dashboard", layout="wide")

load_dotenv()
db_url = os.getenv("DATABASE_URL")
if not db_url:
    st.error("DATABASE_URL is not set. Add it to .env locally or Streamlit secrets in deployment.")
    st.stop()

engine = create_engine(db_url)

st.title("NBA Medallion Warehouse (Bronze → Silver → Gold)")

# Player selector
players = pd.read_sql(
    text("SELECT DISTINCT player_name FROM gold.player_features ORDER BY player_name;"),
    engine
)["player_name"].tolist()

player = st.selectbox("Select a player", players)

df = pd.read_sql(
    text("""
        SELECT
          game_date, team, opponent_team, minutes, pts, reb, ast,
          pts_last3, reb_last3, ast_last3, min_last3,
          opp_pts_allowed_rank, opp_reb_allowed_rank, opp_ast_allowed_rank
        FROM gold.player_features
        WHERE player_name = :p
        ORDER BY game_date DESC
        LIMIT 50;
    """),
    engine,
    params={"p": player}
)

st.subheader("Recent games (last 50)")
st.dataframe(df, width="stretch")

st.subheader("Trends")
df_sorted = df.sort_values("game_date")
st.line_chart(df_sorted.set_index("game_date")[["pts", "pts_last3", "minutes", "min_last3"]])

st.subheader("Pipeline Run Log (last 20)")
df_log = pd.read_sql(
    text("""
        SELECT
          run_id::text AS run_id,
          layer,
          rows_processed,
          status,
          runtime_seconds,
          error_message,
          executed_at
        FROM gold.pipeline_run_log
        ORDER BY executed_at DESC
        LIMIT 20
    """),
    engine,
)
st.dataframe(df_log, width="stretch")