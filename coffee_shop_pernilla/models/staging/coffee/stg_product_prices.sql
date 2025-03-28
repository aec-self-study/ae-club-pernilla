with source as (
    select *
    from {{source('coffee_shop','product_prices')}}
),

rename as (
    select 
    id as product_price_id,
    product_id,
    price,
    created_at as product_price_creation_date,
    ended_at
    from source
)

select product_price_id, product_id, price, product_price_creation_date, ended_at
from rename