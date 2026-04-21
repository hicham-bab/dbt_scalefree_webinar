{{
  config(
    materialized = 'table',
    schema = 'core',
    tags = ['mart', 'core', 'returns']
  )
}}

with returns as (
    select * from {{ ref('stg_returns') }}
),

orders as (
    select
        order_id,
        customer_id,
        order_date,
        order_status,
        channel,
        shipping_country,
        shipping_method,
        discount_amount
    from {{ ref('stg_orders') }}
),

order_items as (
    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price_at_order,
        discount_pct,
        line_revenue
    from {{ ref('stg_order_items') }}
),

order_revenue as (
    select
        order_id,
        sum(line_revenue)                                                    as order_gross_revenue_eur,
        sum(line_revenue) - max(coalesce(discount_amount, 0))                as order_net_revenue_eur
    from order_items
    left join orders using (order_id)
    group by 1
),

final as (
    select
        r.return_id,
        r.order_id,
        r.order_item_id,
        o.customer_id,
        oi.product_id,
        o.order_date,
        cast(to_char(o.order_date, 'YYYYMMDD') as integer)                   as order_date_key,
        r.return_requested_at,
        cast(to_char(r.return_requested_at, 'YYYYMMDD') as integer)          as return_requested_date_key,
        r.return_received_at,
        cast(to_char(r.return_received_at, 'YYYYMMDD') as integer)           as return_received_date_key,
        datediff('day', o.order_date, r.return_requested_at)                 as days_to_return_request,
        datediff('day', o.order_date, r.return_received_at)                  as days_to_return_received,
        r.return_reason,
        r.refund_status,
        o.channel,
        o.order_status,
        o.shipping_country,
        o.shipping_method,
        oi.quantity,
        oi.unit_price_at_order                                               as unit_price_eur,
        oi.discount_pct,
        oi.line_revenue                                                      as item_revenue_eur,
        rr.order_gross_revenue_eur,
        o.discount_amount                                                    as order_discount_amount_eur,
        rr.order_net_revenue_eur,
        r.refund_amount                                                      as refund_amount_eur,
        least(coalesce(r.refund_amount, 0), coalesce(oi.line_revenue, 0))    as revenue_impact_eur,
        case
            when coalesce(rr.order_net_revenue_eur, 0) = 0 then null
            else round(r.refund_amount / rr.order_net_revenue_eur, 4)
        end                                                                  as refund_share_of_order_revenue
    from returns r
    left join orders o on r.order_id = o.order_id
    left join order_items oi on r.order_item_id = oi.order_item_id
    left join order_revenue rr on r.order_id = rr.order_id
)

select * from final
