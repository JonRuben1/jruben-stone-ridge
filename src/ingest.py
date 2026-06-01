"""Poll Coinbase for recent prices and insert them into Snowflake."""
import argparse
import uuid
from datetime import datetime, timezone

from coinbase import fetch_prices
from conn import snowflake_connection

INSERT_SQL = """
insert into prices (product_id, observed_at, price, _source_file)
values (%s, %s, %s, %s)
"""

DEFAULT_PRODUCTS = ["BTC-USD", "ETH-USD"]


def collect_rows(products: list[str], pages: int, run_id: str) -> list[tuple]:
    rows = []
    for product in products:
        for obs in fetch_prices(product, pages=pages):
            rows.append((
                product,
                obs["time"],
                obs["price"],
                f"coinbase/{product}/{run_id}",
            ))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pages", type=int, default=1, help="how many 1000-trade pages to fetch per product")
    parser.add_argument(
        "--products",
        type=lambda s: [p.strip() for p in s.split(",")],
        default=DEFAULT_PRODUCTS,
        help="comma-separated product ids (default: BTC-USD,ETH-USD)",
    )
    args = parser.parse_args()

    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
    rows = collect_rows(args.products, args.pages, run_id)

    if not rows:
        print("no rows fetched from Coinbase, nothing to insert")
        return

    with snowflake_connection() as conn:
        cur = conn.cursor()
        cur.executemany(INSERT_SQL, rows)
        conn.commit()

    print(f"inserted {len(rows)} price observations across {len(args.products)} products")


if __name__ == "__main__":
    main()
