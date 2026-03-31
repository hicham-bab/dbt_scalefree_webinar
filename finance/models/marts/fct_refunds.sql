-- Refund facts combining payment refund data with order context.
-- Grain: one row per refunded payment.
with payments as (
    select
        payment_id,
        order_id,
        customer_id,
        payment_date,
        order_date,
        amount_eur,
        payment_status,
        is_refunded,
        days_to_payment
    from {{ ref('platform', 'fct_payments') }}
    where is_refunded = true or payment_status = 'refunded'
),

orders as (
    select
        order_id,
        order_date,
        channel,
        shipping_country,
        order_status,
        net_revenue_eur
    from {{ ref('platform', 'fct_orders') }}
),

monthly_totals as (
    select
        date_trunc('month', p.payment_date)                        as refund_month,
        sum(p.amount_eur)                                          as total_refunds_eur,
        count(p.payment_id)                                        as refund_count
    from payments p
    group by date_trunc('month', p.payment_date)
),

monthly_revenue as (
    select
        date_trunc('month', o.order_date)                          as order_month,
        sum(o.net_revenue_eur)                                     as total_revenue_eur
    from orders o
    group by date_trunc('month', o.order_date)
),

final as (
    select
        p.payment_id,
        p.order_id,
        p.customer_id,
        p.payment_date                                             as refund_date,
        p.order_date,
        o.channel,
        o.shipping_country,
        o.order_status,
        p.amount_eur                                               as refund_amount_eur,
        datediff('day', p.order_date, p.payment_date)             as days_to_refund,
        mt.refund_count                                            as monthly_refund_count,
        mt.total_refunds_eur                                       as monthly_total_refunds_eur,
        round(
            mt.total_refunds_eur / nullif(mr.total_revenue_eur, 0) * 100,
            2
        )                                                          as monthly_refund_rate_pct
    from payments p
    left join orders o on p.order_id = o.order_id
    left join monthly_totals mt
        on date_trunc('month', p.payment_date) = mt.refund_month
    left join monthly_revenue mr
        on date_trunc('month', p.order_date) = mr.order_month
)

select * from final
order by refund_date desc
