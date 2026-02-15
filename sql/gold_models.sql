CREATE SCHEMA IF NOT EXISTS gold;

-- 1) Opponent allowed stats per season (what teams allow to opponents)
CREATE OR REPLACE VIEW gold.opponent_allowed AS
SELECT
  season,
  opponent_team AS team,
  avg(pts) AS opp_pts_allowed,
  avg(reb) AS opp_reb_allowed,
  avg(ast) AS opp_ast_allowed
FROM silver.player_game_logs
GROUP BY 1,2;

-- 2) Ranks (1 = toughest defense, lowest allowed)
CREATE OR REPLACE VIEW gold.opponent_ranks AS
SELECT
  season,
  team,
  opp_pts_allowed,
  opp_reb_allowed,
  opp_ast_allowed,
  dense_rank() OVER (PARTITION BY season ORDER BY opp_pts_allowed ASC) AS opp_pts_allowed_rank,
  dense_rank() OVER (PARTITION BY season ORDER BY opp_reb_allowed ASC) AS opp_reb_allowed_rank,
  dense_rank() OVER (PARTITION BY season ORDER BY opp_ast_allowed ASC) AS opp_ast_allowed_rank
FROM gold.opponent_allowed;

-- 3) Player features + next-game target
CREATE OR REPLACE VIEW gold.player_features AS
SELECT
  p.season,
  p.game_id,
  p.game_date,
  p.player_id,
  p.player_name,
  p.team,
  p.opponent_team,
  p.minutes,
  p.pts,
  p.reb,
  p.ast,

  avg(p.pts) OVER (
    PARTITION BY p.player_id
    ORDER BY p.game_date
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS pts_last3,

  avg(p.reb) OVER (
    PARTITION BY p.player_id
    ORDER BY p.game_date
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS reb_last3,

  avg(p.ast) OVER (
    PARTITION BY p.player_id
    ORDER BY p.game_date
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS ast_last3,

  avg(p.minutes) OVER (
    PARTITION BY p.player_id
    ORDER BY p.game_date
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS min_last3,

  lead(p.pts) OVER (
    PARTITION BY p.player_id
    ORDER BY p.game_date
  ) AS pts_next_game,

  r.opp_pts_allowed_rank,
  r.opp_reb_allowed_rank,
  r.opp_ast_allowed_rank
FROM silver.player_game_logs p
LEFT JOIN gold.opponent_ranks r
  ON r.season = p.season AND r.team = p.opponent_team;