"""Run setup.sql against Snowflake to create the tables."""
from pathlib import Path

from conn import snowflake_connection


def main() -> None:
    sql = (Path(__file__).parent.parent / "sql" / "setup.sql").read_text()
    with snowflake_connection() as conn:
        for stmt in conn.execute_string(sql):
            pass
    print("setup complete: prices, eod_price")


if __name__ == "__main__":
    main()
