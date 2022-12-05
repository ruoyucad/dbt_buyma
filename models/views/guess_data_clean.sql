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
        ifnull(REPLACE(old_price,'CAD',''),'0') as old_price,
        ifnull(REPLACE(new_price,'CAD',''),'0') as new_price,
        name,
        description,
        image_str,
        '1' AS echange_rate_id, 

    from guess_data
),


 --https://www.guess.com/ca/en/men/apparel/jeans-and-denim/tapered/eco-distressed-tapered-jeans-blue/M2YAS2D4K32-FRMN.html
-- https://www.guess.com/ca/en/men/apparel/jackets-and-coats/puffer-coats/arctic-patch-metallic-puffer-jacket-silver/M3RL50RCXP0-G9F9.html

final as (
    select 
        id,
        url,
        brand,
        CASE 
            when SPLIT(url,'/')[OFFSET(6)] = 'features' then SPLIT(url,'/')[OFFSET(8)] 
            when SPLIT(url,'/')[OFFSET(6)] = 'apparel' then SPLIT(url,'/')[OFFSET(7)] 
        end as category,
        size,
        color,
        ROUND(safe_cast(old_price as FLOAT64) * rates.rate,1) as old_price_jpy,
        ROUND(safe_cast(new_price as FLOAT64) * rates.rate,1) as new_price_jpy,
        name,
        description,
        image_str,
        SPLIT(image_str,'|')[OFFSET(0)] AS Thumbnail, --the first image
        current_timestamp as dbt_updated_at

    from guess_data_transform
    left join rates
    on rates.rate_id = guess_data_transform.echange_rate_id
    where id <> 'GMDOLO-MBRLL'
) 

select * from final