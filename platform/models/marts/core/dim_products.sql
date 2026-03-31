with products as (
    select * from {{ ref('stg_products') }}
),

performance as (
    select * from {{ ref('int_product_performance') }}
),

-- Compute revenue percentile tiers
revenue_tiers as (
    select
        p.product_id,
        pp.total_revenue,
        ntile(4) over (order by coalesce(pp.total_revenue, 0) desc) as revenue_quartile
    from products p
    left join performance pp on p.product_id = pp.product_id
),

final as (
    select
        p.product_id,
        p.product_name,
        p.category,
        p.subcategory,
        p.material,
        p.color,
        p.unit_price                                                as current_price,
        p.cost_price,
        p.margin_pct,
        p.sku_code,
        p.collection,
        p.is_active,
        p.launched_at,
        coalesce(pp.total_orders, 0)                               as total_orders,
        coalesce(pp.total_units_sold, 0)                           as total_units_sold,
        coalesce(pp.total_revenue, 0)                              as total_revenue,
        coalesce(pp.return_rate, 0)                                as return_rate,
        pp.avg_days_to_return,
        pp.first_sold_date,
        pp.last_sold_date,
        case rt.revenue_quartile
            when 1 then 'hero'
            when 2 then 'core'
            when 3 then 'niche'
            when 4 then 'long_tail'
            else 'unranked'
        end                                                        as performance_tier
    from products p
    left join performance pp on p.product_id = pp.product_id
    left join revenue_tiers rt on p.product_id = rt.product_id
)

select * from final
