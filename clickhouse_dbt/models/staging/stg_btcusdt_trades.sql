{{
    config(
        materialized = 'incremental',
        engine = 'MergeTree()',
        order_by = 'trade_date',
        partition_by = 'toYYYYMM(trade_date)',
        unique_key = 'trade_id',
        incremental_strategy = 'delete+insert'
    )
}}

with source as (
    select * 
    from {{source("binance","btcusdt_trades")}}
    {% if is_incremental() %}
        where datetime = (
            select date(max(datetime)) from {{ source("binance","btcusdt_trades") }}
        ) - 1
    {% endif %}
),

staged as (
    select
        distinct
        trade_id,
        price,
        quantity,
        price * quantity                 as trade_value,
        timestamp_utc,
        is_maker,
        best_match,
        datetime,
        toDate(datetime)                 as trade_date,
        toHour(datetime)                 as trade_hour
    from source
)

select * from staged