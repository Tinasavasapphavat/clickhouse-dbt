{{
    config(
        materialized = 'incremental',
        engine = 'MergeTree()',
        order_by = 'date',
        partition_by = 'toYYYYMM(`date`)',
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

, agg as(
    select
        trade_date as `date`,
        instrument,
        first_value(price) as open_price,
        max(price) as high_price,
        min(price) as low_price,
        last_value(price) as close_price,
        sum(quantity) as total_quantity,
        sum(value) as total_value,
        count(trade_id) as total_trades
    from source
    group by trade_date,instrument


)

select
    concat(toString(`date`),'-',instrument) as id,
    `date`,
    instrument,
    open_price,
    high_price,
    low_price,
    close_price,
    total_quantity,
    total_value,
    total_trades
from agg 