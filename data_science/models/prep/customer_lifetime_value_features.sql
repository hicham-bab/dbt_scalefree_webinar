-- Feature set for LTV prediction models.
-- Includes cumulative behavioral snapshots and LTV tier labels for supervised learning.
-- Grain: one row per customer.
with customer_features as (
    select * from {{ ref('customer_features') }}
),

orders as (
    select
        customer_id,
        order_id,
        order_date,
        net_revenue_eur
    from {{ ref('platform', 'fct_orders') }}
    where order_status not in ('cancelled', 'pending')
),

-- Average inter-order gap
order_gaps as (
    select
        customer_id,
        avg(
            datediff(
                'day',
                lag(order_date) over (partition by customer_id order by order_date),
                order_date
            )
        )                                                          as avg_inter_order_days
    from orders
    group by customer_id
),

-- Monthly order trend (growth in orders per month)
monthly_trend as (
    select
        customer_id,
        count(distinct date_trunc('month', order_date))           as active_months,
        count(distinct order_id) / cast(
            nullif(count(distinct date_trunc('month', order_date)), 0) as float
        )                                                          as orders_per_active_month
    from orders
    group by customer_id
),

final as (
    select
        cf.customer_id,
        cf.snapshot_date,
        -- Time features
        floor(cf.days_since_signup / 30)                          as months_since_signup,
        cf.customer_tenure_days,
        -- Cumulative order and revenue as of snapshot
        cf.frequency_orders                                        as order_count_so_far,
        cf.monetary_value                                          as cumulative_revenue_so_far,
        cf.avg_order_value,
        -- Predicted next order timing
        coalesce(og.avg_inter_order_days, 0)                      as avg_inter_order_days,
        case
            when coalesce(og.avg_inter_order_days, 0) > 0
            then round(
                cf.recency_days / cast(og.avg_inter_order_days as float) * 100,
                1
            )
            else null
        end                                                        as pct_of_avg_cycle_elapsed,
        -- Monthly purchase rate
        coalesce(mt.orders_per_active_month, 0)                   as orders_per_active_month,
        coalesce(mt.active_months, 0)                             as active_purchase_months,
        -- All customer features for model training
        cf.loyalty_tier_encoded,
        cf.preferred_channel,
        cf.preferred_category,
        cf.has_returned_items,
        cf.return_rate,
        cf.gender,
        cf.country,
        cf.acquisition_channel,
        -- LTV tier label for supervised learning
        case
            when cf.monetary_value >= 3000    then 'elite'
            when cf.monetary_value >= 1500    then 'high'
            when cf.monetary_value >= 600     then 'medium'
            when cf.monetary_value >= 200     then 'low'
            else 'very_low'
        end                                                        as ltv_tier_label
    from customer_features cf
    left join order_gaps og on cf.customer_id = og.customer_id
    left join monthly_trend mt on cf.customer_id = mt.customer_id
)

select * from final
