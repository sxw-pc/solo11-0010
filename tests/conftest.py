from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from citefuse.api import create_app
from citefuse.clock import FixedClock
from citefuse.config import AppConfig, load_config
from citefuse.storage import Storage
from scripts.seed_fixtures import write_fixtures

FIXED_NOW = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def clock() -> FixedClock:
    return FixedClock(FIXED_NOW)


@pytest.fixture
def config() -> AppConfig:
    return load_config()


@pytest.fixture
def storage(tmp_path: Path):
    instance = Storage(tmp_path / "citefuse-test.sqlite3")
    yield instance
    instance.close()


@pytest.fixture
def client(tmp_path: Path, clock: FixedClock):
    app = create_app(data_dir=tmp_path / "data", clock=clock)
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def tiny_payload() -> bytes:
    return (FIXTURES / "tiny.jsonl").read_bytes()


@pytest.fixture
def tiny_gold() -> dict:
    return json.loads((FIXTURES / "gold_tiny.json").read_text(encoding="utf-8"))


@pytest.fixture
def seeded_paths(tmp_path: Path) -> tuple[Path, Path]:
    return write_fixtures(20240517, tmp_path / "fixtures")
