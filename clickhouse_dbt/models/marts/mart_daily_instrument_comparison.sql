{{
    config(
        materialized = 'incremental',
        engine = 'MergeTree()',
        order_by = 'trade_date',
        partition_by = 'toYYYYMM(`trade_date`)',
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
        sum(if(instrument = 'BTCUSDT',quantity,0)) as btc_volume,
        sum(if(instrument = 'ETHUSDT',quantity,0)) as eth_volume,
        count(if(instrument = 'BTCUSDT',trade_id,null)) as btc_trades,
        count(if(instrument = 'ETHUSDT',trade_id,null)) as eth_trades,
        avg(if(instrument = 'BTCUSDT',price,0)) as btc_avg_price,
        avg(if(instrument = 'ETHUSDT',price,0)) as eth_avg_price,
        sum(if(instrument = 'BTCUSDT',quantity,0))/sum(if(instrument = 'ETHUSDT',quantity,0)) as volume_ratio
    from source  
    group by trade_date

)

select
    trade_date,
    btc_volume,
    eth_volume,
    btc_trades,
    eth_trades,
    btc_avg_price,
    eth_avg_price,
    volume_ratio
from agg 