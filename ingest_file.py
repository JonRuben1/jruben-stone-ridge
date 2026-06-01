"""Ingest a CSV of historical prices into the prices table.

Targets the CryptoDataDownload schema (Coinbase_<sym>_<freq>.csv) by default but
works for any CSV that has a date/timestamp column and a close-price column.
"""
import argparse
import csv
import sys
from datetime import datetime, timezone
from typing import IO, Iterable

from conn import snowflake_connection

INSERT_SQL = """
insert into prices (product_id, observed_at, price, _source_file)
values (%s, %s, %s, %s)
"""


def _find_col(header: list[str], *substrings: str) -> int | None:
    for i, name in enumerate(header):
        n = name.lower().strip()
        for s in substrings:
            if s in n:
                return i
    return None


def _parse_unix(value: str) -> str:
    """Coerce either seconds (10 digits) or milliseconds (13 digits) to ISO 8601 UTC."""
    n = int(value)
    if n > 10**11:
        n = n // 1000
    return datetime.fromtimestamp(n, tz=timezone.utc).isoformat()


_DATE_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H-%p",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d",
]


def _parse_date(value: str) -> str:
    value = value.strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            continue
    raise ValueError(f"could not parse timestamp {value!r}")


def parse_csv(fileobj: IO, override_product: str | None = None, source_label: str = "bulk") -> list[tuple]:
    """Parse a CSV file-object into a list of insert tuples.

    Returns: [(product_id, observed_at_iso, price, _source_file), ...]
    """
    reader = csv.reader(fileobj)
    first = next(reader, None)
    if first is None:
        return []
    # CryptoDataDownload prepends a disclaimer line that starts with "http"
    if first and first[0].lower().startswith("http"):
        header = next(reader)
    else:
        header = first

    unix_idx = _find_col(header, "unix")
    date_idx = _find_col(header, "date")
    symbol_idx = _find_col(header, "symbol", "product", "ticker")
    close_idx = _find_col(header, "close", "price")

    if close_idx is None:
        raise ValueError(f"CSV needs a 'close' or 'price' column; got {header}")
    if unix_idx is None and date_idx is None:
        raise ValueError(f"CSV needs a 'unix' or 'date' column; got {header}")

    rows: list[tuple] = []
    for r in reader:
        if not r or not r[0].strip():
            continue
        if unix_idx is not None and r[unix_idx].strip():
            observed_at = _parse_unix(r[unix_idx])
        else:
            observed_at = _parse_date(r[date_idx])

        if override_product:
            product = override_product
        elif symbol_idx is not None:
            product = r[symbol_idx].replace("/", "-").upper()
        else:
            product = "BTC-USD"

        rows.append((product, observed_at, r[close_idx], source_label))
    return rows


def insert(rows: Iterable[tuple]) -> int:
    rows = list(rows)
    if not rows:
        return 0
    with snowflake_connection() as conn:
        cur = conn.cursor()
        cur.executemany(INSERT_SQL, rows)
        conn.commit()
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file", help="path to CSV file")
    parser.add_argument("--product", default=None, help="override product_id for all rows")
    args = parser.parse_args()

    with open(args.file, newline="") as f:
        rows = parse_csv(f, override_product=args.product, source_label=f"bulk/{args.file.split('/')[-1]}")

    inserted = insert(rows)
    print(f"inserted {inserted} rows from {args.file}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
