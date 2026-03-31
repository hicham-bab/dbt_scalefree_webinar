-- Monthly cohort retention analysis.
-- Tracks how revenue and order counts evolve per signup cohort over time.
-- Grain: one row per (signup_cohort_month, order_cohort_month).
with customers as (
    select
        customer_id,
        to_char(signup_date, 'YYYY-MM')                            as signup_cohort_month
    from {{ ref('platform', 'dim_customers') }}
),

orders as (
    select
        customer_id,
        order_id,
        order_date,
        net_revenue_eur
    from {{ ref('platform', 'fct_orders') }}
    where order_status not in ('cancelled', 'pending', 'returned')
),

-- Join customers to orders and compute cohort period
cohort_data as (
    select
        c.customer_id,
        c.signup_cohort_month,
        to_char(o.order_date, 'YYYY-MM')                           as order_cohort_month,
        o.order_id,
        o.net_revenue_eur
    from customers c
    join orders o on c.customer_id = o.customer_id
),

-- Count unique customers in base cohort
cohort_sizes as (
    select
        signup_cohort_month,
        count(distinct customer_id)                                 as cohort_base_size
    from customers
    group by signup_cohort_month
),

-- Aggregate metrics per cohort x period
cohort_metrics as (
    select
        cd.signup_cohort_month,
        cd.order_cohort_month,
        count(distinct cd.customer_id)                             as active_customers,
        count(distinct cd.order_id)                                as cohort_order_count,
        sum(cd.net_revenue_eur)                                    as cohort_revenue
    from cohort_data cd
    group by cd.signup_cohort_month, cd.order_cohort_month
),

final as (
    select
        cm.signup_cohort_month,
        cm.order_cohort_month,
        cs.cohort_base_size,
        cm.active_customers,
        cm.cohort_order_count,
        cm.cohort_revenue,
        round(cm.active_customers / cast(cs.cohort_base_size as float) * 100, 2) as retention_rate_pct,
        -- Period index (months since signup)
        datediff(
            'month',
            cast(cm.signup_cohort_month || '-01' as date),
            cast(cm.order_cohort_month || '-01' as date)
        )                                                           as period_number
    from cohort_metrics cm
    join cohort_sizes cs on cm.signup_cohort_month = cs.signup_cohort_month
)

select * from final
order by signup_cohort_month, period_number
