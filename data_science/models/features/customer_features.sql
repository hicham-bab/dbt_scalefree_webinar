-- ML feature table for customer-level models (churn, LTV, propensity).
-- Combines customer demographic attributes with behavioral order signals.
-- Grain: one row per customer (snapshot as of current_date).
with customers as (
    select
        customer_id,
        gender,
        country,
        signup_date,
        loyalty_tier,
        acquisition_channel,
        total_orders,
        lifetime_value_eur,
        avg_order_value_eur,
        days_since_last_order,
        customer_tenure_days,
        favorite_category,
        total_items_returned,
        customer_status
    from {{ ref('platform', 'dim_customers') }}
),

orders as (
    select
        customer_id,
        order_id,
        order_date,
        net_revenue_eur,
        channel
    from {{ ref('platform', 'fct_orders') }}
    where order_status not in ('cancelled', 'pending')
),

-- Standard deviation of order value (measures spend variability)
order_variance as (
    select
        customer_id,
        stddev(net_revenue_eur)                                    as std_order_value,
        count(distinct channel)                                    as distinct_channels_used
    from orders
    group by customer_id
),

-- Most used channel
channel_pref as (
    select
        customer_id,
        channel                                                    as preferred_channel,
        row_number() over (
            partition by customer_id order by count(*) desc
        )                                                          as rn
    from orders
    group by customer_id, channel
),

final as (
    select
        c.customer_id,
        current_date()                                             as snapshot_date,
        -- RFM raw inputs
        c.days_since_last_order                                    as recency_days,
        c.total_orders                                             as frequency_orders,
        c.lifetime_value_eur                                       as monetary_value,
        -- Order value stats
        c.avg_order_value_eur                                      as avg_order_value,
        coalesce(ov.std_order_value, 0)                            as std_order_value,
        -- Channel preference
        cp.preferred_channel,
        coalesce(ov.distinct_channels_used, 0)                     as distinct_channels_used,
        c.favorite_category                                        as preferred_category,
        -- Time-based features
        datediff('day', c.signup_date, current_date())             as days_since_signup,
        -- Loyalty tier encoded as ordinal (for ML)
        case c.loyalty_tier
            when 'Bronze'   then 1
            when 'Silver'   then 2
            when 'Gold'     then 3
            when 'Platinum' then 4
            else 0
        end                                                        as loyalty_tier_encoded,
        -- Return behavior
        case when c.total_items_returned > 0 then true else false end as has_returned_items,
        case
            when c.total_orders > 0
            then round(c.total_items_returned / cast(c.total_orders as float), 4)
            else 0
        end                                                        as return_rate,
        -- Active flag for churn label
        case
            when c.customer_status in ('active') then true else false
        end                                                        as active_flag,
        -- Customer lifecycle attributes
        c.gender,
        c.country,
        c.acquisition_channel,
        c.customer_status,
        c.customer_tenure_days
    from customers c
    left join order_variance ov on c.customer_id = ov.customer_id
    left join channel_pref cp on c.customer_id = cp.customer_id and cp.rn = 1
)

select * from final
