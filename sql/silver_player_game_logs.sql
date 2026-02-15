-- sql/silver_player_game_logs.sql
CREATE SCHEMA IF NOT EXISTS silver;

-- Silver is a cleaned, standardized view over bronze
CREATE OR REPLACE VIEW silver.player_game_logs AS
SELECT
  "SEASON"                      AS season,
  "GAME_ID"                     AS game_id,
  "GAME_DATE"::date             AS game_date,
  "PLAYER_ID"                   AS player_id,
  "PLAYER_NAME"                 AS player_name,
  "TEAM_ABBREVIATION"           AS team,
  "MATCHUP"                     AS matchup,
  -- matchup looks like "LAL @ DEN" or "LAL vs DEN"
  split_part("MATCHUP", ' ', 3) AS opponent_team,
  "MIN"::float                  AS minutes,
  "PTS"::float                  AS pts,
  "REB"::float                  AS reb,
  "AST"::float                  AS ast,
  "FGM"::float                  AS fgm,
  "FGA"::float                  AS fga,
  "FG3M"::float                 AS fg3m,
  "FG3A"::float                 AS fg3a,
  "FTM"::float                  AS ftm,
  "FTA"::float                  AS fta,
  "TOV"::float                  AS tov,
  "INGESTED_AT"::timestamptz    AS ingested_at
FROM bronze.player_game_logs;
