"""Install or update the Snowflake Task that runs the EoD MERGE daily at 17:05 ET.

Reads compute_eod.sql so the SQL stays single-sourced. `create or replace task`
makes this safe to rerun any time the MERGE body changes.

Run `python install_task.py --suspend` to pause the task without dropping it.
"""
import argparse
import sys
from pathlib import Path

from conn import snowflake_connection

TASK_NAME = "compute_eod_daily"
SCHEDULE = "USING CRON 5 17 * * * America/New_York"


def install_sql() -> str:
    merge_sql = (Path(__file__).parent.parent / "sql" / "compute_eod.sql").read_text().strip().rstrip(";")
    return f"""
create or replace task {TASK_NAME}
  warehouse = compute_wh
  schedule  = '{SCHEDULE}'
as
{merge_sql};

alter task {TASK_NAME} resume;
"""


def suspend_sql() -> str:
    return f"alter task {TASK_NAME} suspend;"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--suspend",
        action="store_true",
        help="pause the task without dropping it (resume with a plain install)",
    )
    args = parser.parse_args()

    sql = suspend_sql() if args.suspend else install_sql()
    with snowflake_connection() as conn:
        for _ in conn.execute_string(sql):
            pass

    if args.suspend:
        print(f"suspended: {TASK_NAME}")
    else:
        print(f"scheduled: {TASK_NAME} runs at 17:05 America/New_York daily")
    return 0


if __name__ == "__main__":
    sys.exit(main())
