{{
    config(
        materialized = 'incremental',
        engine = 'MergeTree()',
        order_by = 'trade_date',
        partition_by = 'toYYYYMM(`trade_date`)',
        unique_key = 'id',
        incremental_strategy = 'delete+insert'
    )
}}

with source as (
    select *
    from {{ref('int_trades')}}
    {% if is_incremental() %}
        where datetime = (
            select max(trade_date) from {{ref('int_trades')}}
        ) - 1
    {% endif %}

)

,agg as (

    select 
        trade_date,
        instrument,
        stddevSamp(price) price_std,
        max(price) - min(price) as price_range,
        (max(price) - min(price))/first_value(price) as price_range_pct,
        avg(quantity)/count(id) as avg_trade_size
    from source 
    group by trade_date,instrument
)

select 
    concat(toString(trade_date),'-',instrument) as id,
    trade_date,
    instrument,
    price_std,
    price_range,
    price_range_pct,
    avg_trade_size
from agg
