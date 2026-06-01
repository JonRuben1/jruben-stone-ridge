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
