-- Summarizes payment information at order level.
-- Grain: one row per order (most recent / primary payment record).
with payments as (
    select * from {{ ref('stg_payments') }}
),

orders as (
    select order_id, order_date from {{ ref('stg_orders') }}
),

-- Aggregate payments to order level (an order may have retried payments)
payment_agg as (
    select
        p.order_id,
        sum(case when p.payment_status = 'completed' then p.amount_eur else 0 end) as total_paid_eur,
        max(p.payment_date)                                                          as latest_payment_date,
        -- Take the most recent completed payment method
        max(case when p.payment_status = 'completed' then p.payment_method end)     as payment_method,
        max(case when p.payment_status = 'completed' then p.gateway end)            as gateway,
        max(p.payment_status)                                                        as payment_status,
        count(case when p.payment_status = 'refunded' then 1 end)                   as refund_count,
        sum(case when p.payment_status = 'refunded' then p.amount_eur else 0 end)   as total_refunded_eur
    from payments p
    group by p.order_id
),

final as (
    select
        pa.order_id,
        pa.total_paid_eur,
        pa.payment_method,
        pa.gateway,
        pa.payment_status,
        pa.refund_count,
        pa.total_refunded_eur,
        case when pa.refund_count > 0 then true else false end                       as is_refunded,
        datediff('day', o.order_date, pa.latest_payment_date)                       as days_to_payment
    from payment_agg pa
    left join orders o on pa.order_id = o.order_id
)

select * from final
