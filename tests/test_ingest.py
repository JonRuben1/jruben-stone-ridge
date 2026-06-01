from unittest.mock import patch

import pytest

import conn as conn_module
import ingest


def _set_snowflake_env(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_USER", "u")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "p")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "a")
    monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "w")
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "d")
    monkeypatch.setenv("SNOWFLAKE_SCHEMA", "s")


def test_missing_snowflake_user_raises(monkeypatch):
    _set_snowflake_env(monkeypatch)
    monkeypatch.delenv("SNOWFLAKE_USER")
    with pytest.raises(RuntimeError, match="SNOWFLAKE_USER"):
        conn_module.snowflake_connection()


def test_collect_rows_shape():
    def fake_fetch(product, pages=1):
        yield {"time": "2026-05-19T20:13:42.123456Z", "price": "67421.42"}
        yield {"time": "2026-05-19T20:13:41.987654Z", "price": "67420.10"}

    with patch("ingest.fetch_prices", side_effect=fake_fetch):
        rows = ingest.collect_rows(["BTC-USD", "ETH-USD"], pages=1, run_id="testrun")

    assert len(rows) == 4
    assert rows[0] == ("BTC-USD", "2026-05-19T20:13:42.123456Z", "67421.42", "coinbase/BTC-USD/testrun")
    assert rows[2] == ("ETH-USD", "2026-05-19T20:13:42.123456Z", "67421.42", "coinbase/ETH-USD/testrun")


def test_collect_rows_empty_when_fetch_yields_nothing():
    def fake_fetch(product, pages=1):
        return
        yield  # unreachable but makes this a generator

    with patch("ingest.fetch_prices", side_effect=fake_fetch):
        rows = ingest.collect_rows(["BTC-USD"], pages=1, run_id="testrun")
    assert rows == []
