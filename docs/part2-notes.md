# Part 2: notes

This is the design for the Coinbase EoD pipeline. The implementation lives at the repo root; see [README.md](../README.md) for run instructions.

## Requirements

The spec is to ingest at least two crypto price feeds (one of them BTC-USD) into Snowflake, and then compute the EoD BTC-USD price, which Radu clarified is the last trade observed strictly before `17:00:00.000000 ET`.

The data per observation is pretty minimal: I really only need `(product_id, observed_at, price)`. Coinbase happens to expose this through their `/trades` endpoint, but the spec asks for prices, so prices are what we store. The `trade_id`, `size`, and `side` fields that Coinbase also returns get thrown away on the way in.

## REST polling vs WebSocket

There are two ways to get Coinbase prices and I ended up building both. REST is the default; WebSocket is there for the live-feed case.

REST (`ingest.py`) hits `GET https://api.exchange.coinbase.com/products/BTC-USD/trades`, which returns the most recent ~1000 observations. Polling every few minutes via cron gives you a full day of prices by 17:00 ET. There's no connection management, no reconnection logic, no buffering. It's the path I'd reach for first because it's almost impossible to get wrong.

WebSocket (`stream.py`) opens a single connection to `wss://ws-feed.exchange.coinbase.com`, subscribes to the `matches` channel for the requested products, and gets every executed trade pushed in real time. Three things make this non-trivial that REST doesn't have to deal with:

- Disconnects. The library handles ping/pong automatically (`ping_interval=20`), and the main loop wraps `websockets.connect` in a reconnect loop with exponential backoff (1, 2, 5, 15, 30, 60 seconds). On reconnect Coinbase will send a `last_match` snapshot of the most recent trade, which the parser treats the same as a `match`.
- Backpressure. Inserting one row per trade would melt Snowflake. The stream consumer hands incoming matches to an in-memory list; a separate flusher coroutine drains the list to the warehouse every 5 seconds or every 500 rows, whichever hits first. The flusher uses `asyncio.to_thread` so the synchronous Snowflake call doesn't block the websocket recv loop.
- Dedup. Not handled at ingest time. The same dedup story as REST applies: dedup happens inside the EoD MERGE (`qualify row_number() ... = 1`), so even if the same trade lands twice in `prices` it never makes it into `eod_price` twice.

Both paths (and the bulk CSV loader, `ingest_file.py`) land in the same `prices` table with the same `(product_id, observed_at, price)` shape, so the EoD MERGE doesn't know or care which one filled the table. That's the architectural point: the streaming-vs-batch decision is contained inside the ingest layer. At Coinbase volume (a few hundred matches per second at peak across all products) the buffered-`executemany` path is plenty; a broker fronting the streamer is the upgrade once there's more than one downstream consumer.

## Snowflake objects

Just two tables.

```sql
create table prices (
    product_id      varchar,
    observed_at     timestamp_tz,
    price           number(18,8),
    _loaded_at      timestamp_tz default current_timestamp(),
    _source_file    varchar
);

create table eod_price (
    product_id      varchar,
    trade_date      date,
    eod_price       number(18,8),
    eod_observed_at timestamp_tz,
    computed_at     timestamp_tz default current_timestamp()
);
```

`prices` is the raw layer; REST polls, WebSocket streams, and bulk CSV loads all append to it. `eod_price` is the deliverable, recomputed on a schedule.

## The EoD calc

This is a single `MERGE` run on a schedule. Re-running it just refreshes the same row, so it's idempotent.

```sql
merge into eod_price t
using (
    with within_window as (
        select
            product_id,
            observed_at,
            price,
            convert_timezone('America/New_York', observed_at)::timestamp_ntz::date as trade_date,
            convert_timezone('America/New_York', observed_at)::timestamp_ntz::time as observed_time_et
        from prices
        where observed_at >= dateadd(day, -2, current_timestamp())
    )
    select
        product_id,
        trade_date,
        price       as eod_price,
        observed_at as eod_observed_at
    from within_window
    where observed_time_et < '17:00:00'
    qualify row_number() over (
        partition by product_id, trade_date
        order by observed_at desc
    ) = 1
) s
on t.product_id = s.product_id and t.trade_date = s.trade_date
when matched then update set
    eod_price = s.eod_price,
    eod_observed_at = s.eod_observed_at,
    computed_at = current_timestamp()
when not matched then insert
    (product_id, trade_date, eod_price, eod_observed_at)
    values (s.product_id, s.trade_date, s.eod_price, s.eod_observed_at);
```

A couple of things worth pointing out about this:

- `convert_timezone` is the Snowflake function for time-zone conversion, and doing this any other way (string math, offset arithmetic) is honestly asking for bugs. The 2-arg form `convert_timezone(target_tz, ts)` is the right one when `ts` is already `TIMESTAMP_TZ`; the 3-arg form is for `TIMESTAMP_NTZ` inputs. The cast through `::timestamp_ntz` before `::date` / `::time` makes the result independent of the session timezone.
- `qualify row_number() over (...)` is the Snowflake-flavored idiom for "give me one row per group, picked by an ordering." It's just a cleaner way to write a subquery with a rank filter.
- I'm using strict `<` on `17:00:00` because the spec says "before 17:00:00.000000," which I'm reading as strictly less than. A trade at exactly 17:00 belongs to the next session.
- The lookback window of 2 days makes the MERGE incremental in practice while still catching late arrivals within roughly the last day. A 30-day backfill would just bump that to `-30`.
- Late observations within the same day get picked up on the next `MERGE` automatically, so there's no special path for them.

The sentinel lives in `eod.py`, not in `compute_eod.sql`. The Python wrapper runs the MERGE, queries the latest EoD per product, and exits non-zero with a helpful message if `eod_price` is empty. That covers the most common failure mode (we polled but never saw any pre-17:00 ET data); a tighter "today's EoD exists for BTC-USD" check is a one-line predicate change if a stronger SLO ever shows up. A bare `select` statement can't fail anything on its own, so the exit code is what makes it a guardrail rather than a comment.

## How rows get into Snowflake

Every ingest path uses the same pattern: collect rows into a list of tuples, then `cursor.executemany` against a parameterized `INSERT`. The connection lives in `conn.py` and is built from `.env`. The actual call sites are `ingest.py` (REST), `stream.py::flush_batch` (WebSocket, batched), and `ingest_file.py` (bulk CSV); the shape is the same in all three:

```python
cur.executemany(
    "insert into prices (product_id, observed_at, price, _source_file) values (%s, %s, %s, %s)",
    rows,
)
conn.commit()
```

No pandas, no staging, no `COPY INTO`. For the volume here (a few thousand rows per poll, a few hundred matches per second peak on the WebSocket), `executemany` is plenty.

At higher volumes the heavier alternatives are:

- `snowflake.connector.pandas_tools.write_pandas(conn, df, 'PRICES')`, which takes a pandas DataFrame, stages a Parquet file behind the scenes, and runs `COPY INTO` under the hood. Better for bigger batches.
- Writing rows to a CSV file and then running `PUT file://prices.csv @%prices` followed by `COPY INTO prices FROM @%prices`. Highest throughput, most moving parts.
- Snowpipe Streaming, which is real-time row-by-row ingest via the SDK. Worth it when row-level latency is genuinely the requirement.

`executemany` is the right default. The others are scale-up paths.

## Idempotency and late arrivals

A few things to call out:

- Each poll just appends to `prices`. Dedup happens inside the EoD calc itself (which picks one row per `(product_id, trade_date)`), so re-running ingest is safe.
- A trade that arrives late but is still within the lookback window (last 2 days) gets picked up on the next `MERGE` automatically.
- A trade that's older than the lookback would need a rerun of the `MERGE` for that date, which is a one-line predicate change.
- If `prices` doesn't yet have any pre-17:00 ET data for the current trading day (likely on a fresh setup with one poll), `eod.py` exits non-zero with a clear message pointing at `python src/ingest.py --pages N` (or `python src/eod.py --backfill` for the historical-CSV case). The non-zero exit is the contract: it fails loudly so cron and the Snowflake Task don't silently swallow an empty result.

## Repo layout

```
.
├── README.md
├── Makefile
├── requirements.txt
├── src/
│   ├── conn.py                   # build Snowflake connection from env
│   ├── coinbase.py               # fetch_prices(product_id, pages=N)
│   ├── setup.py                  # runs sql/setup.sql
│   ├── ingest.py                 # polls Coinbase /trades, inserts into prices
│   ├── ingest_file.py            # loads a historical CSV into prices
│   ├── stream.py                 # streams Coinbase matches over WebSocket into prices
│   ├── eod.py                    # runs sql/compute_eod.sql (or --backfill), prints latest EoD
│   ├── install_task.py           # installs the Snowflake Task that runs the EoD MERGE daily at 17:05 ET
│   └── dashboard.py              # Streamlit UI over prices + eod_price, with CSV drop and backfill button
├── sql/
│   ├── setup.sql                 # DDL for prices + eod_price
│   ├── compute_eod.sql           # the EoD MERGE (2-day lookback, scheduled runs)
│   └── compute_eod_backfill.sql  # same MERGE, no lookback (one-shot historical loads)
└── tests/                        # unit + SQL tests (DuckDB-backed)
```

Someone reading this should be able to clone the repo, fill in their Snowflake credentials in `.env`, run `make all`, and see an EoD price come out the other end. `make dashboard` brings up the Streamlit UI on top of the same tables for poking at the data, and `make schedule-eod` wires the daily MERGE to a Snowflake Task so the whole thing keeps running without anyone at a keyboard.

## What I'm explicitly not doing in Part 2

- No Docker. `pip install -r requirements.txt` and you're good.
- No external orchestrator (Airflow / Dagster). The daily MERGE is scheduled inside Snowflake via `install_task.py` (`make schedule-eod`); ingest scheduling is the OS's job (cron, launchd).
- No dbt. The transforms are a single MERGE (`compute_eod.sql`, plus a no-lookback backfill variant in `compute_eod_backfill.sql`).
- No FastAPI. SQL is the API.
- No RBAC. One Snowflake user. (Called out in `design.md` as a V2 must-have.)
- No pandas. `executemany` is enough.
- No staging layer in Snowflake. Just `prices` and `eod_price`.

Each of those is a defensible V2 upgrade, and they're all listed in `design.md`.
