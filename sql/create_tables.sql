-- ====================================================
-- Project: Data-Engineering-Gaming-Telemetry-League-of-legends-
-- File: sql/create_tables.sql
-- Description: DDL Schema for League of Legends telemetry data
-- Database: PostgreSQL 15+
-- ====================================================

-- 1. Create Matches Table (Dimension: Match-level static metadata)
CREATE TABLE IF NOT EXISTS matches (
    match_id VARCHAR(50) PRIMARY KEY,
    full_time_ms BIGINT NOT NULL,
    game_duration_min NUMERIC(5, 2)
);

-- 2. Create Team Stats Table (Fact: Combat & Economy Snapshots)
CREATE TABLE IF NOT EXISTS team_stats (
    stat_id BIGSERIAL PRIMARY KEY,
    match_id VARCHAR(50) NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    snapshot_time_pct INT NOT NULL,
    team_color VARCHAR(10) NOT NULL,
    total_gold INT NOT NULL,
    champion_kills INT DEFAULT 0,
    towers_destroyed INT DEFAULT 0,
    first_blood BOOLEAN NOT NULL,
    win BOOLEAN NOT NULL,
    CONSTRAINT uq_team_snapshot UNIQUE(match_id, snapshot_time_pct, team_color)
);

-- 3. Create Objectives Control Table (Fact: Monster & Objective Telemetry)
CREATE TABLE IF NOT EXISTS objectives_control (
    objective_id BIGSERIAL PRIMARY KEY,
    match_id VARCHAR(50) NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    snapshot_time_pct INT NOT NULL,
    team_color VARCHAR(10) NOT NULL,
    dragons_killed INT DEFAULT 0,
    barons_killed INT DEFAULT 0,
    heralds_killed INT DEFAULT 0,
    CONSTRAINT uq_objective_snapshot UNIQUE(match_id, snapshot_time_pct, team_color)
);

-- Indexes to accelerate analytical queries
CREATE INDEX IF NOT EXISTS idx_team_stats_snapshot ON team_stats(snapshot_time_pct);
CREATE INDEX IF NOT EXISTS idx_team_stats_win ON team_stats(win);
CREATE INDEX IF NOT EXISTS idx_objectives_snapshot ON objectives_control(snapshot_time_pct);
