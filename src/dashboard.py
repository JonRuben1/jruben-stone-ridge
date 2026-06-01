"""Streamlit dashboard for the EoD pipeline. Run with: streamlit run dashboard.py"""
import io

import pandas as pd
import streamlit as st

from conn import snowflake_connection
from eod import run_eod
from ingest_file import insert as bulk_insert
from ingest_file import parse_csv

st.set_page_config(page_title="Stoneridge EoD Dashboard", layout="wide")


@st.cache_data(ttl=60)
def query(sql: str) -> pd.DataFrame:
    with snowflake_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        return pd.DataFrame(cur.fetchall(), columns=[c[0] for c in cur.description])


st.title("Stoneridge EoD Dashboard")
st.caption("Live view of the Coinbase price pipeline. Data refreshes every 60s, or use the buttons below.")

col_refresh, col_backfill = st.columns([1, 3])
if col_refresh.button("Refresh now"):
    st.cache_data.clear()
    st.rerun()
if col_backfill.button("Recompute EoD across all history (backfill)"):
    with st.spinner("Running backfill MERGE against the full prices table..."):
        rows = run_eod(backfill=True)
    st.success(f"backfill complete. eod_price now has at least {len(rows)} latest-per-product rows.")
    st.cache_data.clear()

# -- Bulk file upload ---------------------------------------------------------

with st.expander("Upload a bulk price file (CSV)"):
    st.write(
        "Drop in a CryptoDataDownload-style CSV (or any CSV with a date/unix column and a close/price column). "
        "Rows go into the `prices` table; the EoD calc picks them up on the next `make eod`."
    )
    uploaded = st.file_uploader("CSV file", type=["csv"], label_visibility="collapsed")
    override = st.text_input(
        "Override product_id (optional)",
        placeholder="leave empty to read from the CSV's Symbol column",
    )
    if uploaded is not None:
        try:
            text_stream = io.StringIO(uploaded.getvalue().decode("utf-8"))
            rows = parse_csv(
                text_stream,
                override_product=override.strip() or None,
                source_label=f"bulk/{uploaded.name}",
            )
        except Exception as e:
            st.error(f"could not parse CSV: {e}")
        else:
            st.write(f"Parsed {len(rows)} rows. Sample:")
            if rows:
                st.dataframe(
                    pd.DataFrame(rows[:5], columns=["product_id", "observed_at", "price", "_source_file"]),
                    width="stretch",
                )
            if st.button(f"Insert {len(rows)} rows into prices", type="primary"):
                inserted = bulk_insert(rows)
                st.success(f"inserted {inserted} rows from {uploaded.name}. Refresh to see them below.")
                st.cache_data.clear()

# -- Latest EoD per product ----------------------------------------------------

st.header("Latest EoD price")

latest_eod = query("""
    select product_id, trade_date, eod_price, eod_observed_at, computed_at
    from eod_price
    qualify row_number() over (partition by product_id order by trade_date desc) = 1
    order by product_id
""")

if latest_eod.empty:
    st.warning("No EoD rows yet. Run `make ingest` (or `python src/ingest.py --pages 200` for a deeper backfill) and `make eod`.")
else:
    cols = st.columns(len(latest_eod))
    for col, (_, row) in zip(cols, latest_eod.iterrows()):
        col.metric(
            label=f"{row['PRODUCT_ID']} on {row['TRADE_DATE']}",
            value=f"${float(row['EOD_PRICE']):,.2f}",
            help=f"Last observed at {row['EOD_OBSERVED_AT']}",
        )

# -- EoD history --------------------------------------------------------------

st.header("EoD history")

eod_history = query("""
    select product_id, trade_date, eod_price
    from eod_price
    order by product_id, trade_date
""")

if not eod_history.empty:
    pivot = eod_history.pivot(index="TRADE_DATE", columns="PRODUCT_ID", values="EOD_PRICE").astype(float)
    st.line_chart(pivot)
    with st.expander("Raw EoD table"):
        st.dataframe(eod_history, width="stretch")

# -- Ingest activity ----------------------------------------------------------

st.header("Ingest activity")

activity = query("""
    select
        product_id,
        count(*) as row_count,
        min(observed_at) as earliest,
        max(observed_at) as latest,
        max(_loaded_at) as last_loaded
    from prices
    group by product_id
    order by product_id
""")

if activity.empty:
    st.info("No prices ingested yet.")
else:
    st.dataframe(activity, width="stretch")

# -- Recent prices ------------------------------------------------------------

st.header("Recent price observations")

product_filter = st.selectbox(
    "Product",
    options=activity["PRODUCT_ID"].tolist() if not activity.empty else ["BTC-USD", "ETH-USD"],
)

# Pull a reasonable window of recent observations for the chart, then trim the
# table to the most recent N for display.
recent = query(f"""
    select observed_at, price
    from prices
    where product_id = '{product_filter}'
    order by observed_at desc
    limit 1000
""")

if not recent.empty:
    chart_data = recent.set_index("OBSERVED_AT").astype({"PRICE": float}).sort_index()
    st.line_chart(chart_data, y="PRICE")
    with st.expander(f"Last 50 {product_filter} ticks"):
        st.dataframe(recent.head(50), width="stretch")
