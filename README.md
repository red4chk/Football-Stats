# Football Statistics Platform

> A full-stack football data pipeline: API-Football → MongoDB → Dagster orchestration → Metabase dashboards.

## Status

Built incrementally over 10 days. **Progress: Days 1–6 complete.**

| Day | Phase | Status |
|---|---|---|
| 1 | Project skeleton, dependency manifests, workspace config | ✅ |
| 2 | Config loader, MongoDB layer, HTTP client, Docker Compose | ✅ |
| 3 | Ingestion: standings, teams, match fixtures | ✅ |
| 4 | Ingestion: players, top scorers/assists, `fetch_all.py` runner | ✅ |
| 5 | Processing: match normalization, form tables, head-to-head | ✅ |
| 6 | Processing: player rankings, team stats, xG, league summary, `process_all.py` | ✅ |
| 7–10 | Dagster orchestration, tests, polish, deployment | ⏳ |

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

- **Day 7:** Dagster software-defined assets, jobs, and daily cron schedules.
- **Day 8–10:** Tests, logging/hardening, Metabase dashboards, CI, and deployment guides.
