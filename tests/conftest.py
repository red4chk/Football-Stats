"""
Pytest configuration.

Puts the project root on ``sys.path`` so the ``config``, ``db``, ``ingestion``,
``processing``, and ``pipelines`` packages are importable without installing
the project or setting ``PYTHONPATH`` manually.
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
