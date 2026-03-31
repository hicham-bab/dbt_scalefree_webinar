-- Customer LTV analysis with cohort curves and LTV segmentation.
-- Uses platform public models dim_customers and fct_orders.
-- Grain: one row per customer.
with customers as (
    select
        customer_id,
        full_name,
        country,
        loyalty_tier,
        acquisition_channel,
        signup_date,
        first_order_date,
        last_order_date,
        total_orders,
        lifetime_value_eur,
        avg_order_value_eur,
        days_since_last_order,
        customer_tenure_days
    from {{ ref('platform', 'dim_customers') }}
    where total_orders > 0
),

orders as (
    select
        customer_id,
        order_id,
        order_date,
        net_revenue_eur,
        is_first_order
    from {{ ref('platform', 'fct_orders') }}
    where order_status not in ('cancelled', 'pending')
),

-- Monthly order totals per customer
monthly_orders as (
    select
        o.customer_id,
        date_trunc('month', o.order_date)                           as order_month,
        sum(o.net_revenue_eur)                                      as monthly_revenue,
        count(o.order_id)                                           as monthly_orders
    from orders o
    group by o.customer_id, date_trunc('month', o.order_date)
),

-- Average inter-order gap for LTV projection
order_gaps as (
    select
        customer_id,
        avg(gap_days)                                               as avg_days_between_orders
    from (
        select
            customer_id,
            order_date,
            lag(order_date) over (
                partition by customer_id order by order_date
            )                                                       as prev_order_date,
            datediff('day',
                lag(order_date) over (
                    partition by customer_id order by order_date
                ),
                order_date
            )                                                       as gap_days
        from orders
    ) t
    where gap_days is not null
    group by customer_id
),

final as (
    select
        c.customer_id,
        c.full_name,
        c.country,
        c.loyalty_tier,
        c.acquisition_channel,
        c.signup_date,
        to_char(c.first_order_date, 'YYYY-MM')                     as cohort_month,
        c.total_orders,
        c.lifetime_value_eur,
        c.avg_order_value_eur,
        c.days_since_last_order,
        c.customer_tenure_days,
        -- Estimated annual purchase frequency
        case
            when coalesce(og.avg_days_between_orders, 0) > 0
            then round(365.0 / og.avg_days_between_orders, 2)
            else c.total_orders
        end                                                         as estimated_annual_frequency,
        -- Predicted 12-month LTV = AOV * estimated annual frequency
        round(
            c.avg_order_value_eur * case
                when coalesce(og.avg_days_between_orders, 0) > 0
                then round(365.0 / og.avg_days_between_orders, 2)
                else 1
            end,
            2
        )                                                           as predicted_12m_ltv_eur,
        -- LTV segment
        case
            when c.lifetime_value_eur >= 2000              then 'High LTV'
            when c.lifetime_value_eur >= 600               then 'Medium LTV'
            else 'Low LTV'
        end                                                         as ltv_segment,
        current_date()                                              as calculated_at
    from customers c
    left join order_gaps og on c.customer_id = og.customer_id
)

select * from final
