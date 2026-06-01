import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture
def sample_coinbase_response():
    """A realistic Coinbase /trades response, trimmed to a few entries."""
    return [
        {
            "time": "2026-05-19T20:13:42.123456Z",
            "trade_id": 123456789,
            "price": "67421.42",
            "size": "0.0123",
            "side": "buy",
        },
        {
            "time": "2026-05-19T20:13:41.987654Z",
            "trade_id": 123456788,
            "price": "67420.10",
            "size": "0.05",
            "side": "sell",
        },
    ]


@pytest.fixture(autouse=True)
def isolate_env(monkeypatch):
    """Each test starts with a clean env so missing-var checks work."""
    for var in [
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
    ]:
        monkeypatch.delenv(var, raising=False)
