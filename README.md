# Football Statistics Platform

> A full-stack football data pipeline: API-Football → MongoDB → Dagster orchestration → Metabase dashboards.

## Status

Built incrementally over 10 days. **Progress: Days 1–3 complete.**

| Day | Phase | Status |
|---|---|---|
| 1 | Project skeleton, dependency manifests, workspace config | ✅ |
| 2 | Config loader, MongoDB layer, HTTP client, Docker Compose | ✅ |
| 3 | Ingestion: standings, teams, match fixtures | ✅ |
| 4 | Ingestion: players, top scorers/assists, `fetch_all.py` runner | ⏳ next |
| 5–10 | Processing, Dagster orchestration, tests, polish, deployment | ⏳ |

---

## Quick Start

### 1. Set up credentials

Copy `.env.example` to `.env` and fill in your keys:

```bash
cp .env.example .env
```

Required values:
- `RAPIDAPI_KEY` — from [RapidAPI API-Football](https://rapidapi.com/api-sports/api/api-football)
- `MONGODB_URI` — from [MongoDB Atlas](https://www.mongodb.com/atlas) or the local Docker container

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Start MongoDB + Metabase (Docker)

```bash
docker compose up -d
```

- MongoDB listens on `mongodb://admin:password@localhost:27017/`
- Metabase UI is available at http://localhost:3000

### 4. Initialize database indexes

```bash
python -c "from db.collections import setup_indexes; setup_indexes()"
```

### 5. Run an ingestion step (any one of)

```bash
python ingestion/fetch_standings.py
python ingestion/fetch_teams.py
python ingestion/fetch_matches.py
```

Optional league/season override via environment:

```bash
$env:DEFAULT_LEAGUE_ID=140; $env:DEFAULT_SEASON=2024; python ingestion/fetch_teams.py
```

> **Day 4 will add `python ingestion/fetch_all.py`** — a single command that runs every fetcher in sequence.

---

## Project Structure

```
football-stats/
├── config/
│   └── settings.py          # Environment-backed config (API keys, Mongo, defaults)
│
├── db/
│   ├── connection.py        # MongoDB client singleton + get_db()
│   └── collections.py       # Collection registry + setup_indexes() for all collections
│
├── ingestion/
│   ├── base.py              # HTTP client: retries, rate-limit handling, pagination
│   ├── fetch_standings.py   # League standings
│   ├── fetch_teams.py       # Team list + per-team statistics
│   └── fetch_matches.py     # Match fixtures (full season or date range)
│
├── processing/              # (Day 5+) Pandas transformations
├── pipelines/               # (Day 7+) Dagster assets, jobs, schedules
│
├── docker-compose.yml       # MongoDB 7.0 + Metabase
├── reset_db.py              # Drop raw / processed / all collections
├── requirements.txt
├── pyproject.toml
└── .env.example
```

---

## Data Model — Raw Collections

These collections are populated directly from API-Football by the ingestion layer.

| Collection | Populated by | Key fields |
|---|---|---|
| `standings` | `fetch_standings.py` | `league.id`, `league.season` |
| `teams` | `fetch_teams.py` | `team.id` (with `league_id`, `season` denormalized) |
| `team_statistics` | `fetch_teams.py` → `fetch_team_statistics()` | `team.id`, `league.id`, `league.season` |
| `matches` | `fetch_matches.py` | `fixture.id` |
| `players` | *(Day 4)* | — |
| `top_scorers` | *(Day 4)* | — |
| `top_assists` | *(Day 4)* | — |

---

## Configuration

All settings are read from environment variables (see `.env.example` for the full template):

| Variable | Default | Description |
|---|---|---|
| `RAPIDAPI_KEY` | *(empty)* | API-Football / RapidAPI key |
| `MONGODB_URI` | `mongodb://admin:password@localhost:27017/` | MongoDB connection string |
| `MONGODB_DB` | `football_stats` | Database name |
| `DEFAULT_LEAGUE_ID` | `39` | League id (39 = EPL, 140 = La Liga, 135 = Serie A, 78 = Bundesliga, 61 = Ligue 1) |
| `DEFAULT_SEASON` | `2024` | Season year |
| `REQUEST_TIMEOUT` | `30` | HTTP request timeout (seconds) |
| `MAX_RETRIES` | `3` | HTTP retry count for 429/5xx |
| `RETRY_BACKOFF` | `2` | Retry backoff factor |
| `PAGINATION_SLEEP` | `7` | Sleep between paginated API pages (respects free-tier rate limit) |

---

## What's Next

- **Day 4:** Player roster, top scorers & assists fetchers, and the `fetch_all.py` full-pull orchestrator.
- **Day 5–6:** Pandas processing layer — normalized matches, form tables, player rankings, team aggregates, league summary.
- **Day 7:** Dagster software-defined assets, jobs, and daily cron schedules.
- **Day 8–10:** Tests, logging/hardening, Metabase dashboards, CI, and deployment guides.
