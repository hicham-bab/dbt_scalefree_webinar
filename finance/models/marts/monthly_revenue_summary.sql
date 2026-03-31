-- Monthly revenue summary with MoM and YoY growth comparisons.
-- Grain: one row per month.
with orders as (
    select
        order_id,
        customer_id,
        order_date,
        net_revenue_eur,
        is_first_order
    from {{ ref('platform', 'fct_orders') }}
    where order_status not in ('cancelled', 'pending')
),

monthly as (
    select
        date_trunc('month', order_date)                            as month_start,
        to_char(date_trunc('month', order_date), 'YYYY-MM')        as year_month,
        year(order_date)                                           as year,
        month(order_date)                                          as month_num,
        count(distinct order_id)                                   as total_orders,
        sum(net_revenue_eur)                                       as revenue_eur,
        avg(net_revenue_eur)                                       as avg_order_value_eur,
        count(distinct customer_id)                                as active_customers,
        sum(case when is_first_order = true then net_revenue_eur else 0 end)  as new_customer_revenue_eur,
        sum(case when is_first_order = false then net_revenue_eur else 0 end) as returning_customer_revenue_eur,
        count(case when is_first_order = true then 1 end)          as new_customers,
        count(case when is_first_order = false then 1 end)         as returning_customers
    from orders
    group by
        date_trunc('month', order_date),
        to_char(date_trunc('month', order_date), 'YYYY-MM'),
        year(order_date),
        month(order_date)
),

final as (
    select
        year_month,
        year,
        month_num,
        total_orders,
        revenue_eur,
        avg_order_value_eur,
        active_customers,
        new_customers,
        returning_customers,
        new_customer_revenue_eur,
        returning_customer_revenue_eur,
        -- Month-over-month growth
        round(
            (revenue_eur - lag(revenue_eur) over (order by month_start))
            / nullif(lag(revenue_eur) over (order by month_start), 0) * 100,
            2
        )                                                          as mom_revenue_growth_pct,
        -- Year-over-year growth (same month prior year)
        round(
            (revenue_eur - lag(revenue_eur, 12) over (order by month_start))
            / nullif(lag(revenue_eur, 12) over (order by month_start), 0) * 100,
            2
        )                                                          as yoy_revenue_growth_pct,
        -- MoM order count growth
        round(
            (cast(total_orders as float) - lag(total_orders) over (order by month_start))
            / nullif(lag(total_orders) over (order by month_start), 0) * 100,
            2
        )                                                          as mom_orders_growth_pct
    from monthly
)

select * from final
order by year, month_num
