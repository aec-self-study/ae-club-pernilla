with orders as (
    select * from {{ ref('stg_orders') }}
),

final as (
    select
        order_id,
        customer_id,

        row_number() over (
            partition by customer_id
            order by order_created_at
        ) = 1 as is_first_order,

        total,

        order_created_at as sold_at

    from orders

)

select * from final