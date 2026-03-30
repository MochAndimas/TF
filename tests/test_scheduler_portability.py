"""Regression tests for scheduler configuration portability."""

from __future__ import annotations

from pathlib import Path
from unittest import TestCase


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class TestSchedulerPortability(TestCase):
    """Protect against reintroducing machine-specific scheduler paths."""

    def test_cron_source_uses_project_root_placeholder(self):
        cron_file = PROJECT_ROOT / "scripts" / "cron" / "traders_family_etl.cron"
        cron_text = cron_file.read_text(encoding="utf-8")

        self.assertIn("{{PROJECT_ROOT}}/scripts/run_scheduled_etl.sh", cron_text)
        self.assertNotIn("/Users/macbook/Documents/TF", cron_text)

    def test_scheduler_entrypoint_renders_project_root_placeholder(self):
        entrypoint_file = PROJECT_ROOT / "docker" / "scheduler-entrypoint.sh"
        entrypoint_text = entrypoint_file.read_text(encoding="utf-8")

        self.assertIn('PROJECT_ROOT="${PROJECT_ROOT:-/app}"', entrypoint_text)
        self.assertIn("awk -v project_root", entrypoint_text)
        self.assertIn('gsub("\\\\{\\\\{PROJECT_ROOT\\\\}\\\\}", project_root, command)', entrypoint_text)
        self.assertNotIn("/Users/macbook/Documents/TF", entrypoint_text)
