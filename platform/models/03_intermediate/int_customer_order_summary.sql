{# Aggregates order history to customer level for use in customer dimensions.
   Grain: one row per customer. #}

with orders as (
    select * from {{ ref('int_orders_enriched') }}
    where order_status not in ('cancelled', 'pending')
),

returns as (
    select r.return_id, o.order_id
    from {{ ref('return_h') }} r
    join {{ ref('return_order_order_item_l') }} rl on r.hk_return_h = rl.hk_return_h
    join {{ ref('order_h') }} o                    on rl.hk_order_h = o.hk_order_h
    where r.hk_return_h not in ({{ unknown_key() }}, {{ error_key() }})
),

-- Most frequently ordered category per customer
category_orders as (
    select
        o.customer_id,
        ps.category,
        count(nl.hk_order_item_h)                               as category_count,
        row_number() over (
            partition by o.customer_id
            order by count(nl.hk_order_item_h) desc
        )                                                       as rn
    from orders o
    join {{ ref('order_h') }} oh on o.order_id = oh.order_id
    join {{ ref('order_item_order_product_nl') }} nl on oh.hk_order_h = nl.hk_order_h
    join {{ ref('product_h') }} ph on nl.hk_product_h = ph.hk_product_h
    join (
        select hk_product_h, category
        from {{ ref('product_pim_n_s_v1') }}
        where is_current = true
    ) ps on ph.hk_product_h = ps.hk_product_h
    where ph.hk_product_h not in ({{ unknown_key() }}, {{ error_key() }})
    group by o.customer_id, ps.category
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
    from orders o
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
