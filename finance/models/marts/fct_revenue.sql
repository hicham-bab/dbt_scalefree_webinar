-- Daily revenue facts with MTD and YTD cumulative totals.
-- Grain: one row per (date_day, channel, country).
with orders as (
    select
        order_date,
        channel,
        shipping_country,
        order_id,
        gross_revenue_eur,
        discount_amount_eur,
        net_revenue_eur
    from {{ ref('platform', 'fct_orders') }}
    where order_status not in ('cancelled', 'pending')
),

daily_agg as (
    select
        order_date                                                  as date_day,
        channel,
        shipping_country                                            as country,
        count(distinct order_id)                                    as order_count,
        sum(gross_revenue_eur)                                      as gross_revenue_eur,
        sum(discount_amount_eur)                                    as discount_eur,
        sum(net_revenue_eur)                                        as net_revenue_eur,
        avg(net_revenue_eur)                                        as avg_order_value_eur
    from orders
    group by order_date, channel, shipping_country
),

final as (
    select
        date_day,
        channel,
        country,
        order_count,
        gross_revenue_eur,
        discount_eur,
        net_revenue_eur,
        avg_order_value_eur,
        -- Month-to-date cumulative revenue
        sum(net_revenue_eur) over (
            partition by channel, country, date_trunc('month', date_day)
            order by date_day
            rows between unbounded preceding and current row
        )                                                           as cumulative_mtd_revenue,
        -- Year-to-date cumulative revenue
        sum(net_revenue_eur) over (
            partition by channel, country, date_trunc('year', date_day)
            order by date_day
            rows between unbounded preceding and current row
        )                                                           as cumulative_ytd_revenue
    from daily_agg
)

select * from final
order by date_day desc, net_revenue_eur desc
