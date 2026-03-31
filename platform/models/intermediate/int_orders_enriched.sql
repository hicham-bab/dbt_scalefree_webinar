-- Joins orders with items and products to produce order-level revenue metrics.
-- Grain: one row per order.
with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select product_id, category from {{ ref('stg_products') }}
),

returns as (
    select order_id from {{ ref('stg_returns') }}
    group by order_id
),

-- Aggregate items to order level
item_agg as (
    select
        oi.order_id,
        count(oi.order_item_id)                                  as item_count,
        sum(oi.quantity)                                         as total_quantity,
        sum(oi.line_revenue)                                     as gross_revenue,
        listagg(distinct p.category, ', ')
            within group (order by p.category)                  as product_categories
    from order_items oi
    left join products p on oi.product_id = p.product_id
    group by oi.order_id
),

-- Determine first order per customer
first_orders as (
    select
        customer_id,
        min(order_date) as first_order_date
    from orders
    group by customer_id
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.channel,
        o.order_status,
        o.shipping_country,
        o.shipping_method,
        o.discount_code,
        o.discount_amount,
        coalesce(ia.item_count, 0)                               as item_count,
        coalesce(ia.total_quantity, 0)                           as total_quantity,
        coalesce(ia.gross_revenue, 0)                            as gross_revenue,
        o.discount_amount                                        as discount_amount_hdr,
        coalesce(ia.gross_revenue, 0) - o.discount_amount       as net_revenue,
        ia.product_categories,
        case when r.order_id is not null then true else false end as has_returned,
        case when o.order_date = fo.first_order_date then true else false end as is_first_order
    from orders o
    left join item_agg ia on o.order_id = ia.order_id
    left join returns r on o.order_id = r.order_id
    left join first_orders fo on o.customer_id = fo.customer_id
)

select * from final
