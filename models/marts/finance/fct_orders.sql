with orders as (
    select * from {{ref('stg_jaffle_shop_orders')}}
),
payments as (
    select * from {{ref('stg_stripe_payments')}}
),
final as (
    select
      orders.order_id,
      orders.customer_id,
        sum(payments.amount) as total_amount
    from orders
    left join payments
    on orders.order_id = payments.order_id
    group by orders.order_id, orders.customer_id
)
select * from final