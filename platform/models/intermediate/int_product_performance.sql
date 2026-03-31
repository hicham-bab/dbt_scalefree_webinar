-- Aggregates sales and return metrics to product level.
-- Grain: one row per product.
with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select order_id, order_date
    from {{ ref('stg_orders') }}
    where order_status not in ('cancelled', 'pending')
),

returns as (
    select * from {{ ref('stg_returns') }}
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
