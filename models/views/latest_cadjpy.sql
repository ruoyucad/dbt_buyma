with old_rates as (
    select * from {{source('buyma', 'exchange_rates')}}
),

new_rate as (
    select CADJPY from {{source('buyma', 'latest_cadjpy')}}
)

UPDATE old_rates

SET rate = (select CADJPY from new_rate)

WHERE currency = 'CADJPY'