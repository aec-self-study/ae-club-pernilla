with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

product_prices as (
    select * from {{ ref('stg_product_prices') }}
),

joined as (
    select
        order_items.order_item_id,
        order_items.order_id,
        order_items.product_id,
        orders.order_created_at as sold_at,
        products.product_category as product_category,
        product_prices.price as amount

    from order_items

    left join orders
        on order_items.order_id = orders.order_id

    left join products
        on order_items.product_id = products.product_id

    left join product_prices
        on order_items.product_id = product_prices.product_id
        and orders.order_created_at between product_prices.product_price_creation_date and product_prices.ended_at
)

select * from joined