{{ config(materialized='table') }}

with product_canada as (
    select * from {{source('buyma', 'products_canada')}}
),

rates as (
    select * from {{source('buyma', 'exchange_rates')}}
),

product_japanese as (
select 
    product_id,
    brand,
    product_name,
    category,
    price_cad * rates.rate as price_jpy,
    product_url,
    current_timestamp as dbt_updated_at

from product_canada
left join rates
    on rates.rate_id = product_canada.echange_rate_id
)

select * from product_japanese