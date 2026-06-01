# Stoneridge Data Services SWE Project

A Coinbase price pipeline into Snowflake, with an end-of-day price computed once a day. Part 1 of the project is the design write-up (see `docs/design.md`), Part 2 is this implementation.

The short version of the architecture: prices come in three ways (REST poll, WebSocket stream, or bulk CSV) and all land in the same Snowflake `prices` table. A `MERGE` rolls those prices up into an `eod_price` table, picking the last trade observed strictly before 17:00 ET as the EoD price for each `(product, date)`. A small Streamlit dashboard reads from both tables.

## What's in the box

- `ingest.py` — REST polls `GET /products/{id}/trades` for BTC-USD and ETH-USD.
- `stream.py` — opens a WebSocket to `wss://ws-feed.exchange.coinbase.com` and streams every trade in real time, with batched inserts and automatic reconnect.
- `ingest_file.py` — loads a historical CSV (the "bulk drop" path).
- `compute_eod.sql` — the EoD `MERGE`, idempotent, with a 2-day lookback for incremental runs.
- `compute_eod_backfill.sql` — same `MERGE`, no lookback. For one-shot historical loads.
- `dashboard.py` — Streamlit UI over both tables, including a drag-and-drop CSV uploader.
- `tests/` — 25 tests covering the REST client, the WebSocket parser and flush logic, env handling, and the EoD SQL itself (against an in-memory DuckDB).

## Prereqs

- Python 3.11 through 3.14
- A Snowflake account. The [free trial](https://signup.snowflake.com/) works fine — 30 days, $400 of credits, no credit card. The trial gives you a default warehouse (`COMPUTE_WH`); you create the database yourself in the steps below.
- Coinbase: nothing. The public market data endpoints don't require auth.

## One-time setup

### Clone the repo

```bash
git clone <this-repo>
cd jruben-stone-ridge
```

### Set up a Python virtual environment

A virtual environment ("venv") is just an isolated folder that holds a Python interpreter and any packages you install for this project. Keeps the project's dependencies from polluting your system Python and vice versa. The `.venv/` folder is gitignored.

```bash
python3.13 -m venv .venv     # swap in python3.11 / python3.12 if that's what you have
source .venv/bin/activate    # mac/linux; on windows it's .venv\Scripts\activate
```

You'll know it worked because your shell prompt picks up a `(.venv)` prefix. From here on, `python` and `pip` refer to the venv's copies. When you're done working on the project, run `deactivate` to leave it; come back in with `source .venv/bin/activate` next time.

### Install dependencies

```bash
make install
```

That just runs `pip install -r requirements.txt` inside the venv.

### Create the Snowflake database

The trial account ships with a default warehouse (`COMPUTE_WH`) and a `PUBLIC` schema inside every new database, so the only object you have to create yourself is the database. In Snowsight (Snowflake's web UI), open a worksheet and run:

```sql
create database stoneridge;
```

That's it. `STONERIDGE.PUBLIC` now exists. `make setup` (a few steps down) fills the schema with the two tables. If you'd rather use different names, pick whatever you want and make `.env` match.

### Configure Snowflake credentials

Create a `.env` file in the repo root with your Snowflake credentials. It's gitignored so it won't get committed.

```bash
# .env
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_org-your_account
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=STONERIDGE
SNOWFLAKE_SCHEMA=PUBLIC
```

### Create the tables

```bash
make setup
```

`make setup` runs `setup.sql` against your Snowflake account, creating `prices` and `eod_price` in the schema your `.env` points at. Re-running it is safe (it uses `create table if not exists`).

## Run it end-to-end

```bash
make all
```

Equivalent to `make setup && make ingest && make eod`. You should see the latest EoD price per product printed to stdout. If it says "no EoD rows yet," see the note below about fresh-setup timing.

To see it in a UI:

```bash
make dashboard
```

Opens Streamlit at `http://localhost:8501` with EoD cards, an EoD history chart, an ingest-activity summary, and a recent-prices chart. Queries are cached for 60 seconds; there's a refresh button and a backfill button.

Or run both in one shot:

```bash
make demo
```

Runs `make all` first (so you see the EoD printout as proof the pipeline worked), then opens the dashboard.

## The full command reference

| Command | What it does |
|---|---|
| `make setup` | Creates `prices` and `eod_price` in Snowflake. |
| `make ingest` | REST-polls Coinbase for the most recent 1000 trades per product (BTC-USD and ETH-USD by default) and inserts them into `prices`. Pass `--pages N` via `python ingest.py --pages N` to paginate backwards for more history. |
| `make stream` | Opens a WebSocket to Coinbase's `matches` channel and streams trades into `prices` in real time. Batches inserts (500 rows or every 5 seconds). Reconnects on drop with exponential backoff. Ctrl+C flushes the buffer and exits. |
| `make bulk-ingest FILE=path/to/file.csv` | Loads a historical CSV into `prices`. Targets the CryptoDataDownload format by default; any CSV with a date or unix column plus a close or price column works. |
| `make eod` | Runs the EoD `MERGE` (with the 2-day lookback) and prints the latest EoD per product. Exits non-zero with a helpful message if `eod_price` is empty. |
| `make backfill-eod` | Runs the EoD `MERGE` without the lookback. Use this after `make bulk-ingest` to compute EoD across the full history in `prices`. The dashboard has a button that does the same thing. |
| `make schedule-eod` | Installs a Snowflake Task that runs the EoD `MERGE` automatically every day at 17:05 ET. One-time setup; the schedule lives inside Snowflake and survives across machines. Rerun any time to pick up changes to `compute_eod.sql`. |
| `make unschedule-eod` | Suspends the scheduled task without dropping it. Rerun `make schedule-eod` to resume. |
| `make dashboard` | Starts the Streamlit UI. |
| `make demo` | `make all` followed by `make dashboard`. The one-command "show me everything" path. |
| `make test` | Runs the test suite. No Snowflake credentials needed. |
| `make clean` | Removes Python cache directories. |

## Keeping the pipeline running

`make schedule-eod` once. That installs a Snowflake Task that runs the EoD MERGE daily at 17:05 ET, so the deliverable computes itself without anything running on your machine.

## How the EoD is calculated

For each `(product_id, trade_date)`, the EoD price is the price of the last trade with `observed_at < 17:00:00 America/New_York` on that date. Strict less-than: a trade at exactly 17:00:00 belongs to the next session. Time zones are handled with Snowflake's `convert_timezone`, cast through `::timestamp_ntz` before extracting `::date` and `::time` so the result is independent of the session timezone. The `MERGE` is idempotent: rerunning produces the same row.

## A note on first-run timing

The pipeline assumes continuous polling. In production you'd run `make ingest` every few minutes via cron and `make eod` once after 17:00 ET. On a one-shot fresh-setup run, you only have ~3 minutes of data (the latest 1000 trades), so two cases are worth knowing:

- If the latest 1000 trades all happened before 17:00 ET today, `make eod` will produce a row.
- If the latest 1000 trades all happened after 17:00 ET (likely if you're running this late in the evening), the EoD query finds nothing and `eod_price` stays empty. Fix it with `python ingest.py --pages 50` to paginate backwards far enough to include pre-17:00 trades, then rerun `make eod`. Or run `make stream` for a while.

Ingest just polls and stores; the EoD calc operates on whatever's already in `prices`. The non-zero exit on an empty `eod_price` is intentional and points you at the fix.

## Project layout

```
.
├── README.md
├── Makefile
├── requirements.txt
├── conn.py              # build a Snowflake connection from env vars
├── coinbase.py          # fetch_prices(product_id, pages=N) over REST
├── setup.py             # runs setup.sql
├── setup.sql            # DDL for prices and eod_price
├── ingest.py            # polls Coinbase /trades, inserts into prices
├── ingest_file.py       # loads a historical CSV into prices
├── stream.py            # streams Coinbase matches over WebSocket into prices
├── eod.py               # runs compute_eod.sql (or --backfill), prints latest EoD
├── install_task.py      # installs the Snowflake Task that runs EoD daily at 17:05 ET
├── compute_eod.sql      # the EoD MERGE (2-day lookback, for scheduled runs)
├── compute_eod_backfill.sql  # same MERGE, no lookback (for historical loads)
├── dashboard.py         # Streamlit UI over prices + eod_price
├── docs/                # Part 1 design + Part 2 design notes
└── tests/               # unit + SQL tests (DuckDB-backed)
```

## Where to read the design

- [docs/design.md](docs/design.md) — Part 1: architecture diagram, component descriptions, V1 vs V2 with explicit triggers for each deferred item, and tradeoffs.
- [docs/part2-notes.md](docs/part2-notes.md) — Part 2: the schema, REST vs WebSocket vs bulk tradeoffs, the EoD calc in detail, and the idempotency / late-arrival story.

## Assumptions

- Snowflake auth is username/password via env vars. Key-pair would be the production preference; password is enough here.
- The reviewer creates the database and schema in Snowflake themselves. `setup.sql` only creates the two tables inside whatever schema `.env` points at.
- Coinbase's public `/trades` endpoint and `wss://ws-feed.exchange.coinbase.com` don't require auth, so the pipeline doesn't carry a Coinbase key at all. If Coinbase ever moves these behind auth, that's a one-function change in `coinbase.py` and `stream.py`.
- BTC-USD and ETH-USD are the two feeds. The set is a CLI flag (`--products`) on both `ingest.py` and `stream.py`, so adding more is one argument.
- One Snowflake user/role for V1. Multi-role RBAC is in the V2 list with a trigger; see `docs/design.md`.
