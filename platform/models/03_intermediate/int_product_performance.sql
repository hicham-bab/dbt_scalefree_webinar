{# Aggregates sales and return metrics to product level.
   Grain: one row per product. #}

with order_items as (
    select
        oi.order_item_id,
        o.order_id,
        p.product_id,
        s.quantity,
        round(
            s.quantity
            * s.unit_price_at_order
            * (1 - coalesce(s.discount_pct, 0) / 100),
            2
        )                               as line_revenue
    from {{ ref('order_item_order_product_nl') }} nl
    join {{ ref('order_item_order_product_nl_s_v1') }} s
        on nl.hk_order_item_order_product_nl = s.hk_order_item_order_product_nl
        and s.is_current = true
    join {{ ref('order_item_h') }} oi  on nl.hk_order_item_h = oi.hk_order_item_h
    join {{ ref('order_h') }} o        on nl.hk_order_h      = o.hk_order_h
    join {{ ref('product_h') }} p      on nl.hk_product_h    = p.hk_product_h
    where oi.hk_order_item_h not in ({{ unknown_key() }}, {{ error_key() }})
),

current_order_sat as (
    select *
    from {{ ref('order_oms_n_s_v1') }}
    where is_current = true
),

orders as (
    select o.order_id, s.order_date
    from {{ ref('order_h') }} o
    join current_order_sat s on o.hk_order_h = s.hk_order_h
    where s.order_status not in ('cancelled', 'pending')
      and o.hk_order_h not in ({{ unknown_key() }}, {{ error_key() }})
),

returns as (
    select
        r.return_id,
        oi.order_item_id,
        rs.return_requested_at
    from {{ ref('return_h') }} r
    join {{ ref('return_order_order_item_l') }} rl on r.hk_return_h     = rl.hk_return_h
    left join {{ ref('order_item_h') }} oi         on rl.hk_order_item_h = oi.hk_order_item_h
    join (
        select hk_return_h, return_requested_at
        from {{ ref('return_oms_n_s_v1') }}
        where is_current = true
    ) rs on r.hk_return_h = rs.hk_return_h
    where r.hk_return_h not in ({{ unknown_key() }}, {{ error_key() }})
),

-- Return rate per product
return_agg as (
    select
        oi.product_id,
        count(r.return_id)                                                       as return_count,
        avg(
            case when r.return_id is not null
            then datediff('day', o.order_date, r.return_requested_at)
            end
        )                                                                        as avg_days_to_return
    from order_items oi
    join orders o on oi.order_id = o.order_id
    left join returns r on oi.order_item_id = r.order_item_id
    group by oi.product_id
),

-- Sales aggregation per product
sales_agg as (
    select
        oi.product_id,
        count(distinct oi.order_id)                                              as total_orders,
        sum(oi.quantity)                                                         as total_units_sold,
        sum(oi.line_revenue)                                                     as total_revenue,
        min(o.order_date)                                                        as first_sold_date,
        max(o.order_date)                                                        as last_sold_date
    from order_items oi
    join orders o on oi.order_id = o.order_id
    group by oi.product_id
),

final as (
    select
        sa.product_id,
        sa.total_orders,
        sa.total_units_sold,
        sa.total_revenue,
        sa.first_sold_date,
        sa.last_sold_date,
        coalesce(ra.return_count, 0)                                            as return_count,
        case
            when sa.total_units_sold > 0
            then round(coalesce(ra.return_count, 0) / cast(sa.total_units_sold as float) * 100, 2)
            else 0
        end                                                                      as return_rate,
        ra.avg_days_to_return
    from sales_agg sa
    left join return_agg ra on sa.product_id = ra.product_id
)

select * from final
