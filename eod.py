"""Compute EoD prices and print the latest per product.

Default mode runs compute_eod.sql, which has a 2-day lookback for incremental
performance. Use --backfill to run compute_eod_backfill.sql instead, which
scans the entire prices table (use after a historical CSV load).
"""
import argparse
import sys
from pathlib import Path

from conn import snowflake_connection

LATEST_EOD_SQL = """
select product_id, trade_date, eod_price, eod_observed_at
from eod_price
qualify row_number() over (partition by product_id order by trade_date desc) = 1
order by product_id
"""


def run_eod(backfill: bool = False) -> list[tuple]:
    sql_file = "compute_eod_backfill.sql" if backfill else "compute_eod.sql"
    merge_sql = Path(__file__).parent.joinpath(sql_file).read_text()
    with snowflake_connection() as conn:
        cur = conn.cursor()
        for _ in conn.execute_string(merge_sql):
            pass
        cur.execute(LATEST_EOD_SQL)
        return cur.fetchall()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="scan the entire prices table instead of the last 2 days",
    )
    args = parser.parse_args()

    rows = run_eod(backfill=args.backfill)

    if not rows:
        print(
            "no EoD rows in eod_price. either no prices have been ingested yet, "
            "or the ingested prices are all at/after 17:00 ET on their respective days. "
            "run `python ingest.py --pages 20` to backfill some pre-17:00 ET data and rerun, "
            "or `python eod.py --backfill` if you loaded a historical CSV."
        )
        return 1

    for product_id, trade_date, eod_price, eod_observed_at in rows:
        print(f"{product_id} EoD for {trade_date}: ${eod_price} (last observed at {eod_observed_at})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
