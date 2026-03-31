-- Aggregates order history to customer level for use in customer dimensions.
-- Grain: one row per customer.
with orders as (
    select * from {{ ref('int_orders_enriched') }}
    where order_status not in ('cancelled', 'pending')
),

items as (
    select * from {{ ref('stg_order_items') }}
),

returns as (
    select * from {{ ref('stg_returns') }}
),

-- Most frequently ordered category per customer
category_orders as (
    select
        o.customer_id,
        p.category,
        count(oi.order_item_id) as category_count,
        row_number() over (
            partition by o.customer_id
            order by count(oi.order_item_id) desc
        ) as rn
    from {{ ref('stg_orders') }} o
    join {{ ref('stg_order_items') }} oi on o.order_id = oi.order_id
    join {{ ref('stg_products') }} p on oi.product_id = p.product_id
    where o.order_status not in ('cancelled', 'pending')
    group by o.customer_id, p.category
),

fav_category as (
    select customer_id, category as favorite_category
    from category_orders
    where rn = 1
),

-- Return count per customer
return_summary as (
    select
        o.customer_id,
        count(r.return_id) as total_items_returned
    from {{ ref('stg_orders') }} o
    join returns r on o.order_id = r.order_id
    group by o.customer_id
),

final as (
    select
        o.customer_id,
        count(o.order_id)                                        as total_orders,
        sum(o.net_revenue)                                       as total_revenue,
        avg(o.net_revenue)                                       as avg_order_value,
        min(o.order_date)                                        as first_order_date,
        max(o.order_date)                                        as last_order_date,
        datediff('day', max(o.order_date), current_date())      as days_since_last_order,
        datediff('day', min(o.order_date), current_date())      as customer_tenure_days,
        fc.favorite_category,
        coalesce(rs.total_items_returned, 0)                    as total_items_returned
    from orders o
    left join fav_category fc on o.customer_id = fc.customer_id
    left join return_summary rs on o.customer_id = rs.customer_id
    group by
        o.customer_id,
        fc.favorite_category,
        rs.total_items_returned
)

select * from final
