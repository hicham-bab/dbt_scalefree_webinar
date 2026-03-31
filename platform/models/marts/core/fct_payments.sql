with payments as (
    select * from {{ ref('stg_payments') }}
),

orders as (
    select order_id, customer_id, order_date
    from {{ ref('stg_orders') }}
),

final as (
    select
        p.payment_id,
        p.order_id,
        o.customer_id,
        p.payment_date,
        o.order_date,
        p.payment_method,
        p.gateway,
        p.currency,
        p.amount                                                                as amount_original,
        p.amount_eur,
        p.payment_status,
        case when p.payment_status = 'refunded' then true else false end        as is_refunded,
        datediff('day', o.order_date, p.payment_date)                          as days_to_payment
    from payments p
    left join orders o on p.order_id = o.order_id
)

select * from final
