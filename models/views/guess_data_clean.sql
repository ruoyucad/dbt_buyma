{{ config(materialized='table') }}

with guess_data as (
    select * from {{source('buyma', 'guess_data')}}
),

rates as (
    select * from {{source('buyma', 'exchange_rates')}}
),

guess_data_transform as (
    select
        id,
        url,
        'Guess' as brand,
        size,
        color,
        ifnull(LEFT(old_price, 1),'0') as old_price,
        ifnull(LEFT(new_price, 1),'0') as new_price,
        name,
        description,
        image_str,
        '1' AS echange_rate_id, 

    from guess_data
),

final as (
    select 
        id,
        url,
        brand,
        size,
        color,
        safe_cast(old_price as FLOAT64) * rates.rate as old_price_jpy,
        safe_cast(new_price as FLOAT64) * rates.rate as new_price_jpy,
        name,
        description,
        image_str,
        current_timestamp as dbt_updated_at

    from guess_data_transform
    left join rates
    on rates.rate_id = guess_data_transform.echange_rate_id

) 

select * from final