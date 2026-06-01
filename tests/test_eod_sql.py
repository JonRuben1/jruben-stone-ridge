"""Tests the EoD logic against DuckDB.

The query under test is compute_eod.sql, written for Snowflake. DuckDB lets us
exercise the same window logic without a Snowflake account. Two adaptations:

    Snowflake: convert_timezone('UTC', 'America/New_York', observed_at)
    DuckDB:    observed_at AT TIME ZONE 'America/New_York'

    The CTE uses `MATERIALIZED` to work around a DuckDB optimizer bug that
    fails to cast TIMESTAMPTZ through inlined CTE expressions. Snowflake's
    planner doesn't have this issue, so the production query doesn't need it.

If compute_eod.sql changes, mirror the change here.
"""
import duckdb
import pytest

EOD_QUERY_DUCKDB = """
with within_window as materialized (
    select
        product_id,
        observed_at,
        price,
        (observed_at AT TIME ZONE 'America/New_York')::date as trade_date,
        (observed_at AT TIME ZONE 'America/New_York')::time as observed_time_et
    from prices
)
select product_id, trade_date, price as eod_price, observed_at as eod_observed_at
from within_window
where observed_time_et < '17:00:00'
qualify row_number() over (
    partition by product_id, trade_date
    order by observed_at desc
) = 1
order by product_id, trade_date
"""


@pytest.fixture
def conn_with_prices():
    con = duckdb.connect(":memory:")
    con.execute("""
        create table prices (
            product_id varchar,
            observed_at timestamptz,
            price decimal(18,8)
        )
    """)
    # Times are UTC. May 2026 is EDT (UTC-4), so:
    #   20:00 UTC = 16:00 ET
    #   21:00 UTC = 17:00 ET
    rows = [
        # BTC-USD on 2026-05-19
        ("BTC-USD", "2026-05-19T20:58:00Z", "67000"),  # 16:58 ET, before cutoff but not latest
        ("BTC-USD", "2026-05-19T20:59:59Z", "67500"),  # 16:59:59 ET, last before cutoff
        ("BTC-USD", "2026-05-19T21:00:00Z", "67600"),  # 17:00:00 ET exactly, excluded (strict <)
        ("BTC-USD", "2026-05-19T21:30:00Z", "67700"),  # 17:30 ET, after cutoff
        # BTC-USD on 2026-05-18
        ("BTC-USD", "2026-05-18T20:45:00Z", "65500"),  # 16:45 ET, only valid trade
        ("BTC-USD", "2026-05-19T03:00:00Z", "66000"),  # 23:00 ET on 5/18 in NY, after cutoff
        # ETH-USD on 2026-05-19
        ("ETH-USD", "2026-05-19T20:30:00Z", "3500"),   # 16:30 ET
    ]
    con.executemany("insert into prices values (?, ?, ?)", rows)
    yield con
    con.close()


def test_picks_last_trade_before_cutoff(conn_with_prices):
    result = conn_with_prices.execute(EOD_QUERY_DUCKDB).fetchall()
    by_key = {(r[0], r[1].isoformat()): r[2] for r in result}
    assert by_key[("BTC-USD", "2026-05-19")] == 67500


def test_strict_less_than_excludes_17_00_exactly(conn_with_prices):
    result = conn_with_prices.execute(EOD_QUERY_DUCKDB).fetchall()
    by_key = {(r[0], r[1].isoformat()): r[2] for r in result}
    assert by_key[("BTC-USD", "2026-05-19")] != 67600


def test_post_cutoff_trades_excluded(conn_with_prices):
    result = conn_with_prices.execute(EOD_QUERY_DUCKDB).fetchall()
    by_key = {(r[0], r[1].isoformat()): r[2] for r in result}
    assert by_key[("BTC-USD", "2026-05-19")] != 67700


def test_previous_trading_day_resolved_correctly(conn_with_prices):
    result = conn_with_prices.execute(EOD_QUERY_DUCKDB).fetchall()
    by_key = {(r[0], r[1].isoformat()): r[2] for r in result}
    assert by_key[("BTC-USD", "2026-05-18")] == 65500


def test_eth_separate_eod(conn_with_prices):
    result = conn_with_prices.execute(EOD_QUERY_DUCKDB).fetchall()
    by_key = {(r[0], r[1].isoformat()): r[2] for r in result}
    assert by_key[("ETH-USD", "2026-05-19")] == 3500


def test_one_row_per_product_per_trade_date(conn_with_prices):
    result = conn_with_prices.execute(EOD_QUERY_DUCKDB).fetchall()
    keys = [(r[0], r[1].isoformat()) for r in result]
    assert len(keys) == len(set(keys))
    assert len(result) == 3


def test_empty_prices_returns_no_rows():
    con = duckdb.connect(":memory:")
    con.execute("""
        create table prices (
            product_id varchar,
            observed_at timestamptz,
            price decimal(18,8)
        )
    """)
    result = con.execute(EOD_QUERY_DUCKDB).fetchall()
    assert result == []
