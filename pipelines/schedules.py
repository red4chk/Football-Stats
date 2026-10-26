"""
Dagster jobs and cron schedules.

Three jobs slice the asset graph by stage:

* ``daily_ingest_job`` — every ingestion asset.
* ``daily_process_job`` — every processing asset.
* ``full_pipeline_job`` — the entire graph (ingestion then processing).

The two schedules run daily in UTC: ingestion at 06:00, processing at 07:00,
so the derived collections refresh roughly an hour after the raw pull.
"""
from dagster import AssetSelection, ScheduleDefinition, define_asset_job

# -- jobs -------------------------------------------------------------------
daily_ingest_job = define_asset_job(
    "daily_ingest_job",
    selection=AssetSelection.groups("ingestion"),
)

daily_process_job = define_asset_job(
    "daily_process_job",
    selection=AssetSelection.groups("processing"),
)

full_pipeline_job = define_asset_job(
    "full_pipeline_job",
    selection=AssetSelection.all(),
)

# -- schedules --------------------------------------------------------------
daily_ingest_schedule = ScheduleDefinition(
    name="daily_ingest_schedule",
    job=daily_ingest_job,
    cron_schedule="0 6 * * *",
)

daily_process_schedule = ScheduleDefinition(
    name="daily_process_schedule",
    job=daily_process_job,
    cron_schedule="0 7 * * *",
)
