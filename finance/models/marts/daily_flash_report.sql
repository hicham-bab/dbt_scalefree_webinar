-- Daily flash report: last 30 days metrics vs same period prior year.
-- Grain: one row per date_day.
with orders as (
    select
        order_date,
        order_id,
        net_revenue_eur,
        gross_revenue_eur
    from {{ ref('platform', 'fct_orders') }}
    where order_status not in ('cancelled', 'pending')
),

daily as (
    select
        order_date                                                 as date_day,
        count(distinct order_id)                                   as orders,
        sum(net_revenue_eur)                                       as revenue_eur,
        avg(net_revenue_eur)                                       as aov_eur
    from orders
    group by order_date
),

-- Join current period to prior year same date
with_prior_year as (
    select
        curr.date_day,
        curr.orders,
        curr.revenue_eur,
        curr.aov_eur,
        coalesce(py.orders, 0)                                     as py_orders,
        coalesce(py.revenue_eur, 0)                                as py_revenue_eur,
        coalesce(py.aov_eur, 0)                                    as py_aov_eur,
        round(
            (curr.revenue_eur - coalesce(py.revenue_eur, 0))
            / nullif(coalesce(py.revenue_eur, 0), 0) * 100,
            2
        )                                                          as revenue_vs_py_pct,
        round(
            (cast(curr.orders as float) - coalesce(py.orders, 0))
            / nullif(coalesce(py.orders, 0), 0) * 100,
            2
        )                                                          as orders_vs_py_pct
    from daily curr
    left join daily py
        on py.date_day = dateadd('year', -1, curr.date_day)
    where curr.date_day >= dateadd('day', -30, current_date())
)

select * from with_prior_year
order by date_day desc
