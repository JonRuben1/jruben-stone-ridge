from unittest.mock import MagicMock

import pytest

from stream import flush_batch, parse_match

SOURCE = "coinbase-ws/test-run"


def test_parse_match_projects_match_message():
    msg = {
        "type": "match",
        "trade_id": 12345,
        "maker_order_id": "abc",
        "taker_order_id": "def",
        "side": "buy",
        "size": "0.0123",
        "price": "67421.42",
        "product_id": "BTC-USD",
        "sequence": 999,
        "time": "2026-05-30T20:13:42.123456Z",
    }
    assert parse_match(msg, SOURCE) == (
        "BTC-USD",
        "2026-05-30T20:13:42.123456Z",
        "67421.42",
        SOURCE,
    )


def test_parse_match_handles_last_match_snapshot():
    msg = {
        "type": "last_match",
        "trade_id": 1,
        "product_id": "ETH-USD",
        "price": "3500.00",
        "time": "2026-05-30T20:00:00.000000Z",
    }
    assert parse_match(msg, SOURCE) == (
        "ETH-USD",
        "2026-05-30T20:00:00.000000Z",
        "3500.00",
        SOURCE,
    )


def test_parse_match_skips_subscriptions_message():
    msg = {"type": "subscriptions", "channels": [{"name": "matches", "product_ids": ["BTC-USD"]}]}
    assert parse_match(msg, SOURCE) is None


def test_parse_match_skips_heartbeat():
    msg = {"type": "heartbeat", "last_trade_id": 1, "product_id": "BTC-USD", "sequence": 1}
    assert parse_match(msg, SOURCE) is None


def test_parse_match_skips_error_message():
    msg = {"type": "error", "message": "bad subscription"}
    assert parse_match(msg, SOURCE) is None


def test_parse_match_returns_none_on_missing_fields():
    msg = {"type": "match", "product_id": "BTC-USD"}  # no price, no time
    assert parse_match(msg, SOURCE) is None


def _row(i: int) -> tuple:
    return ("BTC-USD", f"2026-05-30T20:00:{i:02d}.000000Z", str(i), SOURCE)


def test_flush_batch_deletes_prefix_on_success():
    buffer = [_row(i) for i in range(5)]
    cur, conn = MagicMock(), MagicMock()

    n = flush_batch(cur, conn, buffer, batch_size=3)

    assert n == 3
    assert buffer == [_row(3), _row(4)]
    cur.executemany.assert_called_once()
    conn.commit.assert_called_once()


def test_flush_batch_leaves_buffer_intact_on_executemany_failure():
    buffer = [_row(i) for i in range(5)]
    cur, conn = MagicMock(), MagicMock()
    cur.executemany.side_effect = RuntimeError("snowflake unreachable")

    with pytest.raises(RuntimeError):
        flush_batch(cur, conn, buffer, batch_size=3)

    assert buffer == [_row(i) for i in range(5)]
    conn.commit.assert_not_called()


def test_flush_batch_leaves_buffer_intact_on_commit_failure():
    buffer = [_row(i) for i in range(5)]
    cur, conn = MagicMock(), MagicMock()
    conn.commit.side_effect = RuntimeError("commit timeout")

    with pytest.raises(RuntimeError):
        flush_batch(cur, conn, buffer, batch_size=3)

    assert buffer == [_row(i) for i in range(5)]


def test_flush_batch_preserves_rows_appended_during_flush():
    buffer = [_row(i) for i in range(3)]
    cur, conn = MagicMock(), MagicMock()
    cur.executemany.side_effect = lambda *_: buffer.extend([_row(10), _row(11)])

    n = flush_batch(cur, conn, buffer, batch_size=3)

    assert n == 3
    assert buffer == [_row(10), _row(11)]


def test_flush_batch_empty_buffer_is_noop():
    buffer = []
    cur, conn = MagicMock(), MagicMock()

    assert flush_batch(cur, conn, buffer, batch_size=500) == 0
    cur.executemany.assert_not_called()
    conn.commit.assert_not_called()
