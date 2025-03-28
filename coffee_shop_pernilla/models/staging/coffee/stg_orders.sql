with source as (
    select *
    from {{source('coffee_shop','orders')}}
),

rename as (
    select
    id as order_id,
    customer_id,
    created_at as order_created_at,
    total,
    state
    from source
)

select order_id, order_created_at, customer_id, total, state
from rename