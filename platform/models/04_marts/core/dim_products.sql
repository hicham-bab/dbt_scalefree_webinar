with current_s as (
    select *
    from {{ ref('product_pim_n_s_v1') }}
    where is_current = true
),

products as (
    select
        h.product_id,
        s.product_name,
        s.category,
        s.subcategory,
        s.material,
        s.color,
        s.unit_price,
        s.cost_price,
        cast(s.unit_price - s.cost_price as number(10,2))      as gross_margin,
        case
            when s.unit_price > 0
            then round((s.unit_price - s.cost_price) / s.unit_price * 100, 2)
            else null
        end                                                     as margin_pct,
        s.sku_code,
        s.collection,
        s.is_active,
        s.launched_at,
        h.ldts                                                  as loaded_at
    from {{ ref('product_h') }} h
    join current_s s on h.hk_product_h = s.hk_product_h
    where h.hk_product_h not in ({{ unknown_key() }}, {{ error_key() }})
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
        cast(coalesce(pp.return_rate, 0) as number(10,4))           as return_rate,
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
