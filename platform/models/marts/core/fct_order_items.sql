with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select
        order_id,
        customer_id,
        order_date,
        order_status
    from {{ ref('stg_orders') }}
),

returns as (
    select order_item_id
    from {{ ref('stg_returns') }}
    where refund_status != 'rejected'
    group by order_item_id
),

final as (
    select
        oi.order_item_id,
        oi.order_id,
        oi.product_id,
        o.customer_id,
        o.order_date,
        o.order_status,
        oi.quantity,
        oi.unit_price_at_order                                                  as unit_price_eur,
        oi.discount_pct,
        oi.line_revenue                                                         as line_revenue_eur,
        case when r.order_item_id is not null then true else false end           as has_been_returned
    from order_items oi
    join orders o on oi.order_id = o.order_id
    left join returns r on oi.order_item_id = r.order_item_id
)

select * from final
