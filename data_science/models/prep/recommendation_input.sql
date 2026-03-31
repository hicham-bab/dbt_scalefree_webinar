-- User-item interaction matrix for collaborative filtering recommendation models.
-- Grain: one row per (customer_id, product_id) combination.
with order_items as (
    select
        customer_id,
        product_id,
        order_date,
        quantity,
        line_revenue_eur
    from {{ ref('platform', 'fct_order_items') }}
    where order_status not in ('cancelled', 'pending')
),

products as (
    select
        product_id,
        product_name,
        category,
        current_price
    from {{ ref('platform', 'dim_products') }}
),

-- Aggregate purchases per customer-product pair
interactions as (
    select
        customer_id,
        product_id,
        count(*)                                                   as purchase_count,
        sum(quantity)                                              as total_units,
        sum(line_revenue_eur)                                      as revenue,
        max(order_date)                                            as last_purchased_date,
        min(order_date)                                            as first_purchased_date
    from order_items
    group by customer_id, product_id
),

final as (
    select
        i.customer_id,
        i.product_id,
        p.product_name,
        p.category,
        i.purchase_count,
        i.total_units,
        i.revenue,
        i.last_purchased_date,
        i.first_purchased_date,
        -- Implicit rating: log transform of revenue for collaborative filtering
        round(ln(i.revenue + 1), 4)                               as implicit_rating,
        -- Recency weight: more recent = higher weight
        datediff('day', i.last_purchased_date, current_date())    as days_since_purchase
    from interactions i
    join products p on i.product_id = p.product_id
)

select * from final
