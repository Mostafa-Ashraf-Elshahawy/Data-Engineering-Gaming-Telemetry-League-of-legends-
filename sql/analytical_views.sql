-- ====================================================================
-- Project: League of Legends Telemetry Data Engineering Pipeline
-- File: sql/analytical_views.sql
-- Phase 3: Analytical SQL Views & JOIN Queries for the 3 Questions
-- ====================================================================

-- --------------------------------------------------------------------
-- Analytical View 1 (Question 1):
-- "Does getting First Blood increase the probability of winning?"
-- Joins matches + team_stats at the 100% final match state.
-- --------------------------------------------------------------------
CREATE OR REPLACE VIEW public.vw_q1_first_blood_win_rate AS
SELECT
    ts.first_blood,
    COUNT(*) AS total_team_matches,
    SUM(CASE WHEN ts.win THEN 1 ELSE 0 END) AS matches_won,
    ROUND(
        100.0 * SUM(CASE WHEN ts.win THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS win_rate_pct,
    ROUND(AVG(m.game_duration_min), 2) AS avg_game_duration_min,
    ROUND(AVG(ts.champion_kills), 2) AS avg_final_kills,
    ROUND(AVG(ts.total_gold), 0) AS avg_final_gold
FROM public.team_stats ts
INNER JOIN public.matches m
    ON ts.match_id = m.match_id
WHERE ts.snapshot_time_pct = 100
GROUP BY ts.first_blood
ORDER BY ts.first_blood DESC;

-- --------------------------------------------------------------------
-- Analytical View 2 (Question 2):
-- "How many towers does the winning team destroy compared to the losing team?"
-- Joins matches + team_stats + objectives_control at 100% match completion.
-- --------------------------------------------------------------------
CREATE OR REPLACE VIEW public.vw_q2_towers_by_match_outcome AS
SELECT
    CASE WHEN ts.win THEN 'Winning Team' ELSE 'Losing Team' END AS match_outcome,
    COUNT(*) AS team_count,
    ROUND(AVG(ts.towers_destroyed), 2) AS avg_towers_destroyed,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ts.towers_destroyed) AS median_towers_destroyed,
    MIN(ts.towers_destroyed) AS min_towers_destroyed,
    MAX(ts.towers_destroyed) AS max_towers_destroyed,
    ROUND(AVG(oc.dragons_killed), 2) AS avg_dragons_killed,
    ROUND(AVG(oc.barons_killed), 2) AS avg_barons_killed,
    ROUND(AVG(oc.heralds_killed), 2) AS avg_heralds_killed
FROM public.team_stats ts
INNER JOIN public.matches m
    ON ts.match_id = m.match_id
INNER JOIN public.objectives_control oc
    ON ts.match_id = oc.match_id
   AND ts.snapshot_time_pct = oc.snapshot_time_pct
   AND ts.team_color = oc.team_color
WHERE ts.snapshot_time_pct = 100
GROUP BY ts.win
ORDER BY ts.win DESC;

-- --------------------------------------------------------------------
-- Analytical View 3 (Question 3):
-- "How does total gold grow from the 20% mark to the 100% mark of a match?"
-- Joins matches + team_stats + objectives_control across all 5 intervals.
-- --------------------------------------------------------------------
CREATE OR REPLACE VIEW public.vw_q3_gold_growth_across_intervals AS
SELECT
    ts.snapshot_time_pct,
    CASE WHEN ts.win THEN 'Winning Team' ELSE 'Losing Team' END AS match_outcome,
    COUNT(*) AS snapshot_records,
    ROUND(AVG(ts.total_gold), 0) AS avg_total_gold,
    ROUND(AVG(ts.champion_kills), 2) AS avg_kills,
    ROUND(AVG(ts.towers_destroyed), 2) AS avg_towers_destroyed,
    ROUND(AVG(oc.dragons_killed), 2) AS avg_dragons_killed
FROM public.team_stats ts
INNER JOIN public.matches m
    ON ts.match_id = m.match_id
INNER JOIN public.objectives_control oc
    ON ts.match_id = oc.match_id
   AND ts.snapshot_time_pct = oc.snapshot_time_pct
   AND ts.team_color = oc.team_color
GROUP BY ts.snapshot_time_pct, ts.win
ORDER BY ts.snapshot_time_pct ASC, ts.win DESC;

-- --------------------------------------------------------------------
-- Query the 3 Analytical Views (Run these in pgAdmin Query Tool)
-- --------------------------------------------------------------------
SELECT * FROM public.vw_q1_first_blood_win_rate;
SELECT * FROM public.vw_q2_towers_by_match_outcome;
SELECT * FROM public.vw_q3_gold_growth_across_intervals;
