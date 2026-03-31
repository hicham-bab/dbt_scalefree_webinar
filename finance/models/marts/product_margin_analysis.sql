-- Product margin analysis combining order items with product catalog costs.
-- Grain: one row per product (plus summary rows by category and collection).
with products as (
    select
        product_id,
        product_name,
        category,
        subcategory,
        collection,
        current_price,
        cost_price,
        margin_pct                                                  as catalog_margin_pct
    from {{ ref('platform', 'dim_products') }}
),

order_items as (
    select
        product_id,
        order_id,
        customer_id,
        order_date,
        quantity,
        unit_price_eur,
        line_revenue_eur
    from {{ ref('platform', 'fct_order_items') }}
    where order_status not in ('cancelled', 'pending')
),

-- Product-level aggregation
product_agg as (
    select
        oi.product_id,
        count(distinct oi.order_id)                                as total_orders,
        sum(oi.quantity)                                           as units_sold,
        sum(oi.line_revenue_eur)                                   as total_revenue_eur,
        sum(oi.quantity * p.cost_price)                            as total_cost_eur,
        avg(oi.unit_price_eur)                                     as avg_selling_price_eur
    from order_items oi
    join products p on oi.product_id = p.product_id
    group by oi.product_id
),

product_final as (
    select
        p.product_id,
        p.product_name,
        p.category,
        p.subcategory,
        p.collection,
        p.current_price,
        p.cost_price,
        p.catalog_margin_pct,
        coalesce(pa.total_orders, 0)                              as total_orders,
        coalesce(pa.units_sold, 0)                                as units_sold,
        coalesce(pa.total_revenue_eur, 0)                         as total_revenue_eur,
        coalesce(pa.total_cost_eur, 0)                            as total_cost_eur,
        coalesce(pa.total_revenue_eur, 0) - coalesce(pa.total_cost_eur, 0)  as gross_margin_eur,
        case
            when coalesce(pa.total_revenue_eur, 0) > 0
            then round(
                (coalesce(pa.total_revenue_eur, 0) - coalesce(pa.total_cost_eur, 0))
                / coalesce(pa.total_revenue_eur, 0) * 100, 2
            )
            else p.catalog_margin_pct
        end                                                        as realized_margin_pct,
        coalesce(pa.avg_selling_price_eur, p.current_price)       as avg_selling_price_eur,
        'product'                                                  as aggregation_level
    from products p
    left join product_agg pa on p.product_id = pa.product_id
),

-- Category rollup
category_rollup as (
    select
        null                                                       as product_id,
        null                                                       as product_name,
        category,
        null                                                       as subcategory,
        null                                                       as collection,
        avg(current_price)                                        as current_price,
        avg(cost_price)                                           as cost_price,
        avg(catalog_margin_pct)                                   as catalog_margin_pct,
        sum(total_orders)                                         as total_orders,
        sum(units_sold)                                           as units_sold,
        sum(total_revenue_eur)                                    as total_revenue_eur,
        sum(total_cost_eur)                                       as total_cost_eur,
        sum(gross_margin_eur)                                     as gross_margin_eur,
        round(sum(gross_margin_eur) / nullif(sum(total_revenue_eur), 0) * 100, 2) as realized_margin_pct,
        avg(avg_selling_price_eur)                                as avg_selling_price_eur,
        'category'                                                as aggregation_level
    from product_final
    group by category
)

select * from product_final
union all
select * from category_rollup
order by aggregation_level, category, total_revenue_eur desc
