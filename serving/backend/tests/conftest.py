import os
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


BACKEND_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = BACKEND_ROOT.parents[1]

for path in (str(REPO_ROOT), str(BACKEND_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)

os.environ.setdefault("STARROCKS_HOST", "ignored")

import main


@pytest.fixture(autouse=True)
def reset_backend_state():
    main.Metrics._counts.clear()
    main.Metrics._latencies_ms.clear()
    main.Metrics._coach_429 = 0
    main._coach_buckets.clear()
    yield


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(main.StarRocks, "init", classmethod(lambda cls: None))
    monkeypatch.setattr(main.StarRocks, "close", classmethod(lambda cls: None))
    with TestClient(main.app) as test_client:
        yield test_client
