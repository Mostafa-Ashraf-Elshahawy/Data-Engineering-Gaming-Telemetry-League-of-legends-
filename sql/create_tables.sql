-- ====================================================================
-- Project: League of Legends Telemetry Data Engineering Pipeline
-- File: sql/create_tables.sql
-- Phase 3: Relational Schema DDL (3NF Fact & Dimension Tables)
-- ====================================================================

-- 1. Dimension Table: matches (1 row per valid match >= 3 minutes)
CREATE TABLE IF NOT EXISTS public.matches (
    match_id VARCHAR(50) PRIMARY KEY,
    full_time_ms BIGINT NOT NULL CHECK (full_time_ms >= 180000),
    game_duration_min NUMERIC(5, 2) NOT NULL CHECK (game_duration_min >= 3.00)
);

-- 2. Fact Table: team_stats (Combat & economy metrics per match, snapshot, and team)
CREATE TABLE IF NOT EXISTS public.team_stats (
    stat_id BIGSERIAL PRIMARY KEY,
    match_id VARCHAR(50) NOT NULL REFERENCES public.matches(match_id) ON DELETE CASCADE,
    snapshot_time_pct INT NOT NULL CHECK (snapshot_time_pct IN (20, 40, 60, 80, 100)),
    team_color VARCHAR(10) NOT NULL CHECK (team_color IN ('Blue', 'Red')),
    total_gold INT NOT NULL CHECK (total_gold >= 0),
    champion_kills INT NOT NULL DEFAULT 0 CHECK (champion_kills >= 0),
    towers_destroyed INT NOT NULL DEFAULT 0 CHECK (towers_destroyed BETWEEN 0 AND 11),
    first_blood BOOLEAN NOT NULL,
    win BOOLEAN NOT NULL,
    CONSTRAINT uq_team_snapshot UNIQUE (match_id, snapshot_time_pct, team_color)
);

-- 3. Fact Table: objectives_control (Neutral monster telemetry per match, snapshot, and team)
CREATE TABLE IF NOT EXISTS public.objectives_control (
    objective_id BIGSERIAL PRIMARY KEY,
    match_id VARCHAR(50) NOT NULL REFERENCES public.matches(match_id) ON DELETE CASCADE,
    snapshot_time_pct INT NOT NULL CHECK (snapshot_time_pct IN (20, 40, 60, 80, 100)),
    team_color VARCHAR(10) NOT NULL CHECK (team_color IN ('Blue', 'Red')),
    dragons_killed INT NOT NULL DEFAULT 0 CHECK (dragons_killed >= 0),
    barons_killed INT NOT NULL DEFAULT 0 CHECK (barons_killed >= 0),
    heralds_killed INT NOT NULL DEFAULT 0 CHECK (heralds_killed >= 0),
    CONSTRAINT uq_objective_snapshot UNIQUE (match_id, snapshot_time_pct, team_color)
);

-- Indexes for fast analytical JOINs and filtering
CREATE INDEX IF NOT EXISTS idx_team_stats_match_id ON public.team_stats(match_id);
CREATE INDEX IF NOT EXISTS idx_team_stats_snapshot ON public.team_stats(snapshot_time_pct);
CREATE INDEX IF NOT EXISTS idx_team_stats_win ON public.team_stats(win);
CREATE INDEX IF NOT EXISTS idx_objectives_match_id ON public.objectives_control(match_id);
CREATE INDEX IF NOT EXISTS idx_objectives_snapshot ON public.objectives_control(snapshot_time_pct);
