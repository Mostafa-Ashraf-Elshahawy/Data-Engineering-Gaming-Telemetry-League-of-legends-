# League of Legends Match Data Analysis

## Project Overview

This project focuses on analyzing League of Legends ranked matches using data engineering technologies.

The goal is to build a data pipeline that extracts, processes, stores, and analyzes match data to discover insights about team performance and match outcomes.

## Dataset

**Dataset:** League of Legends Ranked Matches (January 12 – May 18, 2023)

**Source:** Zenodo

The dataset contains ranked matches from Iron to Diamond tiers across five game-time intervals.

- Files: `full_data_20.csv`, `full_data_40.csv`, `full_data_60.csv`, `full_data_80.csv`, `full_data_100.csv`
- Rows: 16,562 per file
- Columns: 42 per file

## Analytical Questions

1. Does getting First Blood increase the probability of winning?
2. How many towers does the winning team destroy compared to the losing team?
3. How does total gold grow from the 20% mark to the 100% mark of a match?

## Technologies

- Python – Data extraction and cleaning
- Pandas – Data processing
- PostgreSQL – Database storage
- Docker – Containerization
- pgAdmin – Database management
- Kestra – Workflow orchestration
- Tableau / Matplotlib – Data visualization

## Project Architecture

```text
Raw CSV Files
      |
      v
Python Ingestion & Cleaning
      |
      v
PostgreSQL Database
      |
      v
Kestra Orchestration
      |
      v
Dashboard & Visualization
```

## Repository Structure

```text
lol-match-data-analysis/
│
├── docs/
├── scripts/
├── sql/
├── flows/
│
├── docker-compose.yml
├── .env.example
├── .gitignore
└── README.md
```

## Environment Setup

### Prerequisites

- Docker Desktop
- Git
- GitHub account

### 1. Clone the repository

```bash
git clone YOUR_REPOSITORY_URL
cd lol-match-data-analysis
```

### 2. Configure environment variables

Copy `.env.example` to `.env` and update the example passwords.

### 3. Start Docker services

```bash
docker compose up -d
```

### 4. Check running containers

```bash
docker compose ps
```

### 5. Access pgAdmin

Open your browser:

`http://localhost:5050`

Log in using the credentials configured in your `.env` file.

## Team

This project is developed as a collaborative Data Engineering course project.
