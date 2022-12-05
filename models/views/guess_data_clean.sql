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
        'Guess' as brand,
        size,
        color,
        LEFT(old_price, 1) as old_price,
        LEFT(new_price, 1) as new_price,
        old_price * rates.rate as old_price_jpy,
        new_price * rates.rate as new_price_jpy,
        name,
        description,
        image_str,
        '1' AS echange_rate_id, 

    from guess_data
    left join rates
    on rates.rate_id = guess_data.echange_rate_id

)


