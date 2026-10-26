# Football Statistics Platform

> A full-stack football data pipeline: API-Football → MongoDB → Dagster orchestration → Metabase dashboards.

## Status

Built incrementally over 10 days. **Progress: Days 1–7 complete.**

| Day | Phase | Status |
|---|---|---|
| 1 | Project skeleton, dependency manifests, workspace config | ✅ |
| 2 | Config loader, MongoDB layer, HTTP client, Docker Compose | ✅ |
| 3 | Ingestion: standings, teams, match fixtures | ✅ |
| 4 | Ingestion: players, top scorers/assists, `fetch_all.py` runner | ✅ |
| 5 | Processing: match normalization, form tables, head-to-head | ✅ |
| 6 | Processing: player rankings, team stats, xG, league summary, `process_all.py` | ✅ |
| 7 | Dagster orchestration: assets, jobs, daily schedules | ✅ |
| 8–10 | Tests, logging/hardening, Metabase dashboards, deployment | ⏳ |

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
python ingestion/fetch_players.py
python ingestion/fetch_top_scorers.py
python ingestion/fetch_top_assists.py
```

Optional league/season override via environment:

```bash
$env:DEFAULT_LEAGUE_ID=140; $env:DEFAULT_SEASON=2024; python ingestion/fetch_teams.py
```

### 6. Or run the full ingestion in one command

```bash
python ingestion/fetch_all.py
```

This runs every fetcher in dependency order (standings → teams → team statistics → matches → players → top scorers → top assists). Override the league/season:

```bash
python ingestion/fetch_all.py --league 140 --season 2024
```

### 7. Run the processing layer

Transform the raw collections into the query-ready `processed_*` collections:

```bash
python processing/process_all.py
```

This flattens the fixtures and player statistics, then derives team stats, recent form, player rankings, the league summary, and the top-scorer/assist leaderboards. Override the league/season the same way:

```bash
python processing/process_all.py --league 140 --season 2024
```

### 8. Orchestrate with Dagster

Launch the Dagster UI (reads `pipelines.definitions` via `pyproject.toml`):

```bash
dagster dev
```

Open http://localhost:3000, pick a job from the **Jobs** tab, and materialize it. Each asset
reports the number of records it wrote, so row counts are visible immediately after a run.

To materialize the whole graph headless:

```bash
dagster asset materialize --select '*' -m pipelines.definitions
```

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
│   ├── fetch_matches.py     # Match fixtures (full season or date range)
│   ├── fetch_players.py     # Player roster (paginated)
│   ├── fetch_top_scorers.py # Top scorers leaderboard
│   ├── fetch_top_assists.py # Top assists leaderboard
│   └── fetch_all.py         # Full ingestion orchestrator (all fetchers in order)
│
├── processing/              # Pandas transformations
│   ├── clean_matches.py     # Flatten fixtures -> processed_matches, form tables, H2H
│   ├── clean_players.py     # Explode player statistics, per-90 rankings
│   ├── stats.py             # Team stats, xG proxy, leaderboards, league summary
│   └── process_all.py       # Full processing orchestrator (all stages in order)
│
├── pipelines/               # Dagster orchestration
│   ├── assets.py            # Software-defined assets (ingestion + processing groups)
│   ├── schedules.py         # Daily ingest/process jobs + cron schedules
│   └── definitions.py       # Consolidated Definitions loaded by `dagster dev`
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
| `players` | `fetch_players.py` | `player.id`, `statistics.0.league.season` |
| `top_scorers` | `fetch_top_scorers.py` | `player.id`, `season`, `league_id` |
| `top_assists` | `fetch_top_assists.py` | `player.id`, `season`, `league_id` |

---

## Data Model — Processed Collections

These collections are derived from the raw data by `processing/process_all.py` and are what
Metabase queries directly. Every write is an idempotent upsert on the key shown.

| Collection | Produced by | Key fields | Contents |
|---|---|---|---|
| `processed_matches` | `clean_matches.py` → `save_processed_matches()` | `fixture_id` | Flattened fixture rows (teams, goals, half-time score, venue, round) |
| `processed_form` | `clean_matches.py` → `save_processed_form()` | `team_id`, `season` | Last-N form: W/D/L, points, goal difference, form string |
| `processed_players` | `clean_players.py` → `save_processed_players()` | `player_id`, `season` | Flat player rows + `goals_per_90`, `assists_per_90`, `goal_contributions`, `shot_accuracy` |
| `processed_teams` | `stats.py` → `save_processed_teams()` | `team_id`, `season` | Team table: played/wins/draws/losses, goals for/against, points, `win_rate` |
| `processed_top_scorers` | `stats.py` → `save_top_scorers()` | `player_id`, `season`, `league_id` | Ranked scorers with per-90 metrics |
| `processed_top_assists` | `stats.py` → `save_top_assists()` | `player_id`, `season`, `league_id` | Ranked assisters with per-90 metrics |
| `processed_league_summary` | `stats.py` → `save_league_summary()` | `league_id`, `season` | Season totals (goals, goals/match) + top scorer/assister + best team |

---

## Dagster Jobs & Schedules

The asset graph is split into two groups — `ingestion` (raw API pulls) and `processing`
(derived collections). Dependencies encode the ordering, so a single job materializes
everything in the correct sequence.

### Assets

| Group | Asset | Depends on | Writes to |
|---|---|---|---|
| `ingestion` | `standings` | — | `standings` |
| `ingestion` | `teams` | — | `teams` |
| `ingestion` | `matches` | — | `matches` |
| `ingestion` | `players` | — | `players` |
| `ingestion` | `top_stats` | `players` | `top_scorers`, `top_assists` |
| `processing` | `processed_matches` | `matches` | `processed_matches` |
| `processing` | `processed_players` | `players` | `processed_players` |
| `processing` | `processed_teams` | `processed_matches` | `processed_teams` |
| `processing` | `processed_form` | `processed_matches` | `processed_form` |
| `processing` | `processed_top_scorers` | `processed_players` | `processed_top_scorers` |
| `processing` | `processed_top_assists` | `top_stats` | `processed_top_assists` |
| `processing` | `league_summary` | `processed_matches`, `processed_players` | `processed_league_summary` |

### Jobs & schedules

| Job | Selection | Schedule | Cron (UTC) |
|---|---|---|---|
| `daily_ingest_job` | `AssetSelection.groups("ingestion")` | `daily_ingest_schedule` | `0 6 * * *` |
| `daily_process_job` | `AssetSelection.groups("processing")` | `daily_process_schedule` | `0 7 * * *` |
| `full_pipeline_job` | `AssetSelection.all()` | *(manual)* | — |

Every asset accepts an optional `LeagueSeasonConfig` (`league_id`, `season`) so a run can be
scoped to another league from the launchpad; both fields default to `DEFAULT_LEAGUE_ID` /
`DEFAULT_SEASON`.

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
| `MAX_PLAYER_PAGES` | `0` (all pages) | Hard cap on `/players` pagination pages (0 = unlimited) |

---

## What's Next

- **Day 8:** pytest infrastructure and unit/integration tests for config, DB, ingestion, processing, and assets.
- **Day 9:** Centralized logging, idempotency/retry hardening, and Metabase dashboard blueprints.
- **Day 10:** E2E smoke script, performance tuning, CI workflow, and deployment guides.
