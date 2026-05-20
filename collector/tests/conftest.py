"""Top-level pytest configuration for the collector test suite."""
from __future__ import annotations

import pytest


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "unit: pure unit tests, no external deps")
    config.addinivalue_line(
        "markers",
        "integration: requires a running PostgreSQL instance (skipped if unavailable)",
    )
