"""
Consolidated Dagster definitions.

``pyproject.toml`` points ``[tool.dagster] module_name`` at this module, so
``dagster dev`` loads the full pipeline (asset graph, jobs, and schedules)
from the single ``defs`` object below.
"""
from dagster import Definitions

from pipelines.assets import INGESTION_ASSETS, PROCESSING_ASSETS
from pipelines.schedules import (
    daily_ingest_job,
    daily_ingest_schedule,
    daily_process_job,
    daily_process_schedule,
    full_pipeline_job,
)

defs = Definitions(
    assets=INGESTION_ASSETS + PROCESSING_ASSETS,
    jobs=[daily_ingest_job, daily_process_job, full_pipeline_job],
    schedules=[daily_ingest_schedule, daily_process_schedule],
)
