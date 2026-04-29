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

with btc as (
    select 
        concat('btcusdt-', toString(trade_id)) as trade_id,
        price,
        quantity,
        trade_value as value,
        'BTCUSDT' as instrument,
        datetime as datetime,
        trade_date
    from {{ref("stg_btcusdt_trades")}}
    {% if is_incremental() %}
        where datetime = (
            select date(max(datetime)) from {{ref("stg_btcusdt_trades")}}
        ) - 1
    {% endif %}

)

, eth as (
    select 
        concat('ethusdt-', toString(trade_id)) as trade_id,
        price,
        quantity,
        trade_value as value,
        'ETHUSDT' as instrument,
        datetime as datetime,
        trade_date
    from {{ref("stg_ethusdt_trades")}}
    {% if is_incremental() %}
        where datetime = (
            select date(max(datetime)) from {{ref("stg_ethusdt_trades")}}
        ) - 1
    {% endif %}
)

, source as (
    select * from btc 
    union all 
    select * from eth
)


select
    trade_id,
    toFloat64(price)                             as price,
    toFloat64(quantity)                          as quantity,
    toFloat64(value)                             as value,
    instrument,
    toDateTime64(datetime, 3)                    as datetime,
    toDate(trade_date)                           as trade_date
from source
EOF