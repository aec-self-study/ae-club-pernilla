with source as (
    select *
    from {{source('coffee_shop','order_items')}}
),

rename as (
    select 
    id as order_item_id,
    order_id,
    product_id
    from source
)

select order_item_id, order_id, product_id
from rename