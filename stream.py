"""Stream Coinbase trades into Snowflake via the matches WebSocket channel.

Opens one connection to wss://ws-feed.exchange.coinbase.com, subscribes to the
matches channel for the given products, buffers incoming trades, and flushes to
the prices table every BATCH_INTERVAL_SECS or when the buffer hits BATCH_SIZE.
Reconnects on drop with exponential backoff. Ctrl+C flushes and exits cleanly.
"""
import argparse
import asyncio
import json
import signal
import time
import uuid
from datetime import datetime, timezone

import websockets

from conn import snowflake_connection

WS_URL = "wss://ws-feed.exchange.coinbase.com"
INSERT_SQL = """
insert into prices (product_id, observed_at, price, _source_file)
values (%s, %s, %s, %s)
"""
DEFAULT_PRODUCTS = ["BTC-USD", "ETH-USD"]
BATCH_SIZE = 500
BATCH_INTERVAL_SECS = 5.0
RECONNECT_BACKOFF_SECS = [1, 2, 5, 15, 30, 60]
RECV_TIMEOUT_SECS = 1.0
# Soft cap on the in-memory buffer if Snowflake stays unreachable. Past this we
# drop oldest rows to keep memory bounded. With a real broker in front of this
# (Kafka/Redpanda, see V2 list in design.md) the spill would go to disk instead.
MAX_BUFFER_ROWS = 50_000


def parse_match(msg: dict, source: str) -> tuple | None:
    """Project a Coinbase websocket message to a prices row, or None to skip."""
    if msg.get("type") not in ("match", "last_match"):
        return None
    try:
        return (msg["product_id"], msg["time"], msg["price"], source)
    except KeyError:
        return None


async def _subscribe(ws, products: list[str]) -> None:
    await ws.send(json.dumps({
        "type": "subscribe",
        "product_ids": products,
        "channels": ["matches"],
    }))


async def _consume(ws, buffer: list, source: str, stop: asyncio.Event) -> None:
    """Read from ws until stop is set or the connection closes."""
    dropped = 0
    while not stop.is_set():
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT_SECS)
        except asyncio.TimeoutError:
            continue
        row = parse_match(json.loads(raw), source)
        if row is None:
            continue
        if len(buffer) >= MAX_BUFFER_ROWS:
            del buffer[0]
            dropped += 1
            if dropped % 1000 == 1:
                print(f"WARNING: buffer at cap ({MAX_BUFFER_ROWS}); dropped {dropped} oldest rows")
        buffer.append(row)


def flush_batch(cur, conn, buffer: list, batch_size: int) -> int:
    """Snapshot a prefix of buffer and insert it. Only delete the prefix on
    successful commit, so a failed flush leaves rows in place for the next try.
    Returns rows inserted; raises on insert/commit failure.
    """
    if not buffer:
        return 0
    rows = list(buffer[:batch_size])
    cur.executemany(INSERT_SQL, rows)
    conn.commit()
    del buffer[: len(rows)]
    return len(rows)


async def _flusher(buffer: list, conn, stop: asyncio.Event) -> None:
    """Periodically drain buffer into Snowflake. Runs until stop is set."""
    cur = conn.cursor()
    last_flush = time.monotonic()
    consecutive_failures = 0
    try:
        while not stop.is_set() or buffer:
            await asyncio.sleep(0.25)
            now = time.monotonic()
            ready = len(buffer) >= BATCH_SIZE or (now - last_flush) >= BATCH_INTERVAL_SECS
            if not ready or not buffer:
                if stop.is_set() and not buffer:
                    return
                continue
            try:
                n = await asyncio.to_thread(flush_batch, cur, conn, buffer, BATCH_SIZE)
            except Exception as e:
                consecutive_failures += 1
                backoff = RECONNECT_BACKOFF_SECS[
                    min(consecutive_failures - 1, len(RECONNECT_BACKOFF_SECS) - 1)
                ]
                print(
                    f"flush failed ({type(e).__name__}: {e}); "
                    f"{len(buffer)} rows held for retry in {backoff}s"
                )
                await asyncio.sleep(backoff)
                continue
            consecutive_failures = 0
            print(f"flushed {n} rows")
            last_flush = time.monotonic()
    finally:
        cur.close()


async def stream(products: list[str]) -> None:
    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
    source = f"coinbase-ws/{run_id}"
    buffer: list[tuple] = []
    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass  # windows

    conn = snowflake_connection()
    flusher_task = asyncio.create_task(_flusher(buffer, conn, stop))

    try:
        attempt = 0
        while not stop.is_set():
            try:
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                    await _subscribe(ws, products)
                    print(f"streaming {', '.join(products)} (ctrl+c to stop)")
                    attempt = 0
                    await _consume(ws, buffer, source, stop)
            except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
                if stop.is_set():
                    break
                backoff = RECONNECT_BACKOFF_SECS[min(attempt, len(RECONNECT_BACKOFF_SECS) - 1)]
                print(f"connection dropped ({type(e).__name__}: {e}); reconnecting in {backoff}s")
                await asyncio.sleep(backoff)
                attempt += 1
    finally:
        stop.set()
        await flusher_task
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--products",
        type=lambda s: [p.strip() for p in s.split(",") if p.strip()],
        default=DEFAULT_PRODUCTS,
        help="comma-separated product ids (default: BTC-USD,ETH-USD)",
    )
    args = parser.parse_args()
    asyncio.run(stream(args.products))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
