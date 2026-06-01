create table if not exists prices (
    product_id      varchar,
    observed_at     timestamp_tz,
    price           number(18,8),
    _loaded_at      timestamp_tz default current_timestamp(),
    _source_file    varchar
);

create table if not exists eod_price (
    product_id      varchar,
    trade_date      date,
    eod_price       number(18,8),
    eod_observed_at timestamp_tz,
    computed_at     timestamp_tz default current_timestamp()
);
