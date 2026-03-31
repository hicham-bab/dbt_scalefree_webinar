-- Revenue breakdown by country with % of total and YoY growth.
-- Grain: one row per country.
with orders as (
    select
        shipping_country,
        order_id,
        order_date,
        net_revenue_eur
    from {{ ref('platform', 'fct_orders') }}
    where order_status not in ('cancelled', 'pending')
),

current_year as (
    select
        shipping_country,
        count(distinct order_id)                                   as total_orders,
        sum(net_revenue_eur)                                       as total_revenue_eur
    from orders
    where year(order_date) = year(current_date())
    group by shipping_country
),

prior_year as (
    select
        shipping_country,
        sum(net_revenue_eur)                                       as py_revenue_eur
    from orders
    where year(order_date) = year(current_date()) - 1
    group by shipping_country
),

total as (
    select sum(total_revenue_eur) as grand_total_eur from current_year
),

final as (
    select
        cy.shipping_country                                        as country,
        cy.total_orders,
        cy.total_revenue_eur,
        round(cy.total_revenue_eur / nullif(t.grand_total_eur, 0) * 100, 2) as pct_of_total_revenue,
        coalesce(py.py_revenue_eur, 0)                            as py_revenue_eur,
        round(
            (cy.total_revenue_eur - coalesce(py.py_revenue_eur, 0))
            / nullif(coalesce(py.py_revenue_eur, 0), 0) * 100,
            2
        )                                                          as yoy_growth_pct
    from current_year cy
    left join prior_year py on cy.shipping_country = py.shipping_country
    cross join total t
)

select * from final
order by total_revenue_eur desc
