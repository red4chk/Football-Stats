# Football Statistics Platform — 10-Phase Development Roadmap

## Project Overview

A full-stack Python data engineering platform that ingests football match/team/player data from the API-Football (RapidAPI) service, stores raw and derived results in MongoDB, orchestrates daily refresh jobs with Dagster, and surfaces analytics through Metabase BI dashboards.

**Target stack:** Python 3.11+, pymongo, pandas, requests, Dagster 1.9, MongoDB 7.0, Docker Compose, Metabase

**Realized daily workload:** Each phase below is scoped to a realistic focused engineering day (6–8 hours equivalent): design, implementation, basic sanity testing, inline documentation, and a descriptive commit.

---

## Phase 1 · Day 1 — Project Initialization & Foundational Setup

**Scope:** Repository scaffolding only — zero feature code. Lock down toolchain and workspace hygiene before writing any logic.

**Deliverables:**
- `pyproject.toml` — Python project metadata, pinned runtime dependency list (requests, pymongo, pandas, dagster, dagster-webserver, python-dotenv, urllib3), and Dagster `[tool.dagster]` entry-point stub
- `requirements.txt` — Frozen pip-compatible mirror of `pyproject.toml` deps for easy `pip install -r` workflows
- `.env.example` — Environment-variable template (RAPIDAPI_KEY, MONGODB_URI, MONGODB_DB, DEFAULT_LEAGUE_ID, DEFAULT_SEASON) with inline comments linking signup URLs
- `.gitignore` — Python, virtualenv, caches, logs, `.env`, pytest/mypy/ruff caches, Docker bind-mount noise
- `README.md` — Project title, one-paragraph purpose statement, table of contents anchor, and an *under-construction* "coming soon" section stubs for quick start, architecture, and project structure
- Top-level directory placeholders (empty `config/`, `db/`, `ingestion/`, `processing/`, `pipelines/` with `__init__.py` files so the package is importable from day one)

**Commit message:**
`chore: initialize repository skeleton, dependency manifests, and workspace configuration`

---

## Phase 2 · Day 2 — Core Infrastructure (Config, Database, HTTP Client)

**Scope:** Establish all the cross-cutting infrastructure that every future feature depends on. Nothing downstream works without this layer.

**Deliverables:**
- `config/settings.py` — Typed/validated configuration loaded from environment via `python-dotenv`, with sensible defaults (API hosts, league 39 = EPL, season 2024, request timeout, retry backoff, max retries)
- `db/connection.py` — MongoDB singleton client factory `get_client()`/`get_db()`, connection-failure logging, ping health check, and `close_client()` teardown
- `db/collections.py` — `get_collection(name)` helper and a `setup_indexes()` function that pre-creates unique + secondary indexes for all planned raw and processed collections (matches, players, teams, standings, top_scorers, top_assists, team_statistics, processed_*)
- `ingestion/base.py` — HTTP client helpers: `build_session()` with `urllib3.Retry` adapter for 429/5xx, `api_get(endpoint, params)` with rate-limit awareness (sleep on low remaining quota, recover from rateLimit errors), and `paginate(endpoint, params, max_pages)` for automatic multi-page pulls
- `docker-compose.yml` v3.9 with MongoDB 7.0 service (persistent volume, root auth, port 27017) and a Metabase service (port 3000, depends_on mongodb, H2 internal store for simplicity)
- `reset_db.py` — CLI utility with `--raw`, `--processed`, and default "drop everything" modes; enumerates collection name lists and drops each if present

**Basic verification:**
- `python -c "from config.settings import *; print(DEFAULT_LEAGUE_ID)"` succeeds
- `docker compose config` validates the compose file
- `python reset_db.py --help` prints usage (requires no running DB)

**Commit message:**
`feat(core): add config loader, MongoDB connection layer, index setup, HTTP client with retries, and Docker Compose services`

---

## Phase 3 · Day 3 — Ingestion: Standings, Teams, and Matches (Core Domain Data)

**Scope:** The first ingestion endpoints that populate the league/season context everything else joins on.

**Deliverables:**
- `ingestion/fetch_standings.py` — `fetch_standings(league_id, season)` that calls `/standings`, bulk-upserts into the `standings` collection by composite `(league.id, league.season)` key
- `ingestion/fetch_teams.py` — `fetch_teams(league_id, season)` hitting `/teams`, denormalizing `league_id`/`season` onto the document, upsert by `team.id`; plus `fetch_team_statistics(team_id, league_id, season)` for per-team deep stats into `team_statistics`
- `ingestion/fetch_matches.py` — `fetch_matches(league_id, season, from_date, to_date)` calling paginated `/fixtures`, bulk upsert by `fixture.id`; plus a convenience `fetch_recent_matches(days, ...)` for catch-up syncs
- Update `db/collections.py` index setup if new compound indexes emerge from the three modules
- Unit smoke tests inside `if __name__ == "__main__":` blocks (print counts, require API key to actually run)
- README updated with "Data Model — Raw Collections" table

**Commit message:**
`feat(ingestion): implement standings, teams, and match fixture fetchers with MongoDB upsert`

---

## Phase 4 · Day 4 — Ingestion: Player Roster, Top Scorers, Top Assists + Manual Pull Orchestrator

**Scope:** Complete the ingestion surface area for all API endpoints, then glue them into a single reproducible CLI entry point.

**Deliverables:**
- `ingestion/fetch_players.py` — `fetch_players(league_id, season)` using paginated `/players` with `FREE_PLAN_PAGE_LIMIT = 3` guard; `fetch_top_scorers(...)` hitting `/players/topscorers`; `fetch_top_assistants(...)` hitting `/players/topassists`; each with `player.id + season` composite upsert keys
- `ingestion/fetch_all.py` — `argparse`-driven manual full-pull runner: calls `setup_indexes()` then standings → teams → matches → players → top_scorers → top_assists sequentially, with `--league` and `--season` flags and structured logging
- Update `db/collections.py` with player / top_scorers / top_assists unique compound indexes
- README "Quick Start" section filled in for install + `fetch_all.py` usage
- `pyproject.toml` Dagster `module_name = "pipelines.definitions"` entry (directory exists, module stub will land Day 7)

**Commit message:**
`feat(ingestion): add player roster, top scorers & assists fetchers plus fetch_all manual orchestrator`

---

## Phase 5 · Day 5 — Processing Layer: Match Normalization, Form Tables, and Head-to-Head

**Scope:** Turn raw nested fixture JSON into flat, query-friendly records that BI dashboards and downstream stats can consume.

**Deliverables:**
- `processing/clean_matches.py`:
  - `load_matches_df(league_id, season)` — `pymongo` cursor → flattened pandas DataFrame with columns `fixture_id, date, status, venue, league_id/name/season/round, home_team_id/name, away_team_id/name, home/away goals, winners, half-time scores`; UTC datetime coercion
  - `compute_form_table(df, last_n=5)` — explode each match into two "team-match" rows, sort by date, take last N per team, aggregate W/D/L/GD/points, and a string form column (e.g. `"WWDLW"`)
  - `head_to_head(df, team_a_id, team_b_id)` — filter helper returning historical matches between two clubs
  - `save_processed_matches(df)` — iterate DataFrame rows, convert `Timestamp` → Python `datetime`, upsert into `processed_matches` by `fixture_id`
- Extend `setup_indexes()` for `processed_matches.fixture_id` uniqueness

**Commit message:**
`feat(processing): flatten raw fixtures into processed_matches, compute recent-form tables and H2H lookup`

---

## Phase 6 · Day 6 — Processing Layer: Player Normalization, Rankings, Team Aggregates, xG, League Summary

**Scope:** Complete the analytics processing engine — produce all derived collections that the BI layer will query directly.

**Deliverables:**
- `processing/clean_players.py`:
  - `load_players_df(league_id, season)` — explode each player's `statistics[]` array into a flat row per player/team/season tuple
  - `compute_player_rankings(df, min_appearances=1)` — add `goals_per_90`, `assists_per_90`, `goal_contributions`, `shot_accuracy`; sort by contributions
  - `save_processed_players(df)` — upsert by `(player_id, season)`
- `processing/stats.py`:
  - `estimate_xg(shots_total, shots_on_target, goals)` — simplified heuristic xG estimator with documentation noting it is a free-tier proxy
  - `compute_team_stats(matches_df)` — home/away concat → per-team `played/wins/draws/losses/goals_for/against/points/GD/win_rate`
  - `compute_league_summary(league_id, season)` — MongoDB aggregation for total goals + lookup helpers for top scorer/assister/best team → single summary doc
  - `compute_top_scorers(league_id, season, limit=20)` and `compute_top_assists(...)` — rank-enriched tables with per-90 metrics
  - `save_top_scorers()`, `save_top_assists()`, `save_league_summary()` write helpers
- `processing/process_all.py` — argparse-driven orchestrator tying together match processing → team stats → form → player processing → league summary → top scorers → top assists with per-stage logging
- README "MongoDB Collections" table completed for raw + processed
- `db/collections.py` indexes extended for processed players/teams/form/top scorers/league summary

**Commit message:**
`feat(processing): player rankings, aggregated team stats, xG estimates, league summary, top-N leaderboards, and process_all orchestrator`

---

## Phase 7 · Day 7 — Dagster Orchestration: Software-Defined Assets and Jobs

**Scope:** Lift the manual CLI scripts into a reproducible, UI-visible data pipeline with dependency-aware execution.

**Deliverables:**
- `pipelines/assets.py` — Dagster `@asset` graph grouped into `ingestion` and `processing` groups:
  - Ingestion group: `standings`, `teams`, `matches`, `players`, `top_stats` (depends on `players`)
  - Processing group: `processed_matches` (deps `matches`), `processed_players` (deps `players`), `processed_teams` (deps `processed_matches`), `processed_form` (deps `processed_matches`), `processed_top_scorers` (deps `processed_players`), `processed_top_assists` (deps `top_stats`), `league_summary` (deps `processed_matches` + `processed_players`)
  - Each asset returns `Output[T]` with `MetadataValue` for record counts so the UI shows row counts post-run
- `pipelines/schedules.py`:
  - `daily_ingest_job`, `daily_process_job`, `full_pipeline_job` via `define_asset_job` with `AssetSelection.groups(...)`
  - `daily_ingest_schedule` cron `0 6 * * *`, `daily_process_schedule` cron `0 7 * * *`
- `pipelines/definitions.py` — Consolidated `Definitions(assets=[...], jobs=[...], schedules=[...])` object
- Confirm `pyproject.toml` `[tool.dagster]` points at `pipelines.definitions` so `dagster dev` works
- README updated with Dagster instructions and a "Dagster Jobs & Schedules" reference table

**Commit message:**
`feat(orchestration): add Dagster software-defined assets, daily cron schedules, and full pipeline job`

---

## Phase 8 · Day 8 — Testing Infrastructure and Unit/Integration Test Suite

**Scope:** Add reproducible confidence before the polish/deployment phase — pure Python tests that don't require live API keys.

**Deliverables:**
- Add `pytest` and `pytest-mock` to `requirements.txt` and `pyproject.toml`
- `tests/__init__.py`, `tests/conftest.py` — insert project root onto `sys.path` for importable package
- `tests/test_config.py` — unit tests asserting environment-variable override behavior, default league/season values, retry backoff
- `tests/test_db.py` — mock `MongoClient` using `unittest.mock.patch`, verify singleton pattern, verify `setup_indexes()` emits the expected `create_index` calls for all declared collections
- `tests/test_ingestion.py` — mock `ingestion.base.api_get` + `paginate` to return JSON fixtures; assert `fetch_matches`, `fetch_standings`, `fetch_players` call `bulk_write` with correct `UpdateOne` count
- `tests/test_processing.py` — build in-memory pandas DataFrames with 10–20 synthetic match/player rows; assert `compute_form_table` produces the correct `WWDLW` strings, `compute_team_stats` sums points correctly, `compute_player_rankings` produces non-null `goals_per_90`
- `tests/test_assets.py` — stub out asset functions with `context=build_asset_context()` (Dagster test utilities) and verify return-type shapes without hitting a real DB
- Basic `pytest.ini` / `pyproject.toml` `[tool.pytest.ini_options]` section and `.pytest_cache` already ignored
- README updated with "Running Tests" section

**Commit message:**
`test: add pytest infrastructure and unit tests for config, DB, ingestion, processing, and Dagster assets`

---

## Phase 9 · Day 9 — Observability, Logging, Error Hardening, and Metabase Dashboard Blueprint

**Scope:** Harden the pipeline against production failure modes, improve telemetry, and provide the BI layer with an onboarding blueprint so dashboards are discoverable without reverse-engineering collections.

**Deliverables:**
- Centralized logging configuration in a new `config/logging.py` (or top-of-script imports in all CLIs) using a consistent `%(asctime)s [%(levelname)s] %(name)s: %(message)s` format, used consistently across `fetch_all.py`, `process_all.py`, `reset_db.py`
- Circuit-breaker / idempotency improvements: each fetcher function returns early with `0` instead of raising on empty API responses; each `upsert` path logs both `upserted_count` and `modified_count` distinctly; duplicate key errors inside `bulk_write(ordered=False)` are tolerated
- Retry budget improvements: in `ingestion/base.py`, add Jitter to `Retry(backoff_factor=...)` and a distinct log line for each retry event with the response status code that triggered it
- `docs/dashboards.md` (linked from README) with four dashboard blueprints exactly matching the README's "Suggested Dashboards": Match Performance, Player Leaderboard, Team Comparison, League Standings — each with a bullet list of which processed collection, which fields, and which chart types to use in Metabase; plus the full "Metabase Setup" walkthrough (Docker hostname `mongodb`, auth creds, database name)
- README "Supported Leagues" table completed (IDs 39, 140, 135, 78, 61, 2) with notes on swapping `DEFAULT_LEAGUE_ID`
- Minor bug fixes discovered during test expansion (e.g. any `KeyError` edge cases when `statistics` array is empty on fringe player records)

**Commit message:**
`chore: centralized logging, idempotency & retry hardening, and Metabase dashboard blueprints`

---

## Phase 10 · Day 10 — Final Polish, End-to-End Verification, Performance Optimizations, and Deployment Checklist

**Scope:** Wrap the project for production handoff: final bug pass, E2E smoke run, perf tweaks, full docs, deployment configuration + CI placeholder.

**Deliverables:**
- End-to-end smoke run script `scripts/smoke_test.sh` (Windows: `scripts/smoke_test.ps1`) that boots MongoDB with Docker Compose, runs `python reset_db.py`, runs `processing.process_all` + ingestion modules against mocked fixtures, then exits non-zero on any failure. Documented in README.
- Performance optimizations:
  - In `db/collections.py setup_indexes()` — explicitly declare every index in one pass and reorder indexes so high-cardinality lookups come first
  - In processing modules — call `.to_dict("records")` once and reuse; bulk upsert via `bulk_write` instead of Nx `update_one` loops where feasible
  - In pagination — introduce a config flag to tune per-page sleep lower for paid API tiers
- Complete all README sections: Quick Start with numbered steps, Project Structure tree, MongoDB Collections table, Dagster Jobs table, Supported Leagues, Metabase Setup, Suggested Dashboards, Running Tests, Troubleshooting FAQ (3–5 common issues: "MongoDB auth failed", "Rate limit hit", "Dagster won't start", "No matches returned", "pandas empty DF warning")
- `.github/workflows/ci.yml` GitHub Actions skeleton that on push-to-main runs `pip install -r requirements.txt`, runs `pytest`, runs `docker compose config` to validate the compose file, and runs `python -m py_compile` against all `*.py` files as a syntax smoke gate
- `DEPLOYMENT.md` with three sections: (1) local Docker Compose, (2) deploying to a single VPS with systemd units for Dagster daemon + webserver, (3) Mongo Atlas + Metabase Cloud managed deployment topology diagram (ASCII)
- Final verification: walk the README steps top-to-bottom in a fresh clone (simulated by running each command locally), fix any drift
- Final comprehensive commit + optional version tag `v1.0.0`

**Commit message:**
`release: add E2E smoke script, performance tuning, full README, CI workflow, and deployment guides — v1.0`

---

## Dependency Graph (Visual Chronology)

```
Day 1: Skeleton + Manifests
    │
    ▼
Day 2: Config / DB / HTTP Client / Compose
    │
    ├──────────────────────────────────┐
    ▼                                  ▼
Day 3: Standings+Teams+Matches      Day 4: Players+Leaderboards + fetch_all
    │                                  │
    └──────────────┬───────────────────┘
                   ▼
        Day 5: clean_matches + Form
                   │
                   ▼
    Day 6: clean_players + Team Stats + xG + Summary + process_all
                   │
                   ▼
        Day 7: Dagster Assets / Schedules
                   │
                   ▼
    Day 8: pytest unit/integration coverage
                   │
                   ▼
    Day 9: Logging + Hardening + Dashboard blueprints
                   │
                   ▼
    Day 10: E2E, Perf, Docs, CI, Deployment — v1.0
```

Each phase's commit produces a working, runnable increment that can be demonstrated on its own.
