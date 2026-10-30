with orders as (
    select * from {{ ref('int_orders_enriched') }}
),

payments as (
    select * from {{ ref('int_payment_summary') }}
),

shipments as (
    select * from {{ ref('int_shipment_summary') }}
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        cast(to_char(o.order_date, 'YYYYMMDD') as integer)                     as order_date_key,
        o.channel,
        o.order_status,
        o.shipping_country,
        o.shipping_method,
        o.discount_code,
        o.item_count,
        o.total_quantity,
        o.gross_revenue                                                         as gross_revenue_eur,
        o.discount_amount_hdr                                                   as discount_amount_eur,
        o.net_revenue                                                           as net_revenue_eur,
        o.product_categories,
        o.has_returned,
        o.is_first_order,
        -- Payment details
        coalesce(p.payment_method, 'unknown')                                   as payment_method,
        coalesce(p.payment_status, 'unknown')                                   as payment_status,
        p.total_paid_eur,
        p.is_refunded,
        -- Shipment details
        s.carrier,
        s.shipment_status,
        coalesce(s.days_to_ship, 0)                                             as days_to_ship,
        s.days_to_deliver,
        coalesce(s.is_late_delivery, false)                                     as is_late_delivery
    from orders o
    left join payments p on o.order_id = p.order_id
    left join shipments s on o.order_id = s.order_id
)

select * from final
