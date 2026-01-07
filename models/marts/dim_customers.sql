with customers as (

<<<<<<< HEAD:models/marts/dim_customers.sql
    select * from {{ref('stg_jaffle_shop_customers')}}
=======
    select
        id as CUSTOMER_ID,
        first_name,
        last_name

    from raw.jaffle_shop.customers
>>>>>>> 9502d61816376e2e3ab92ec29099303ec64d03b0:models/customers.sql

),

orders as (

    select * from {{ref('stg_jaffle_shop_orders')}}


),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),


final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders

    from customers

    left join customer_orders using (customer_id)

)

select * from final
