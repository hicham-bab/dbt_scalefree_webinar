-- Churn prediction model training dataset.
-- Combines all customer features with churn labels.
-- Definition: churned = no purchase in the last 180 days.
-- Grain: one row per customer.
with customer_features as (
    select * from {{ ref('customer_features') }}
),

final as (
    select
        cf.customer_id,
        cf.snapshot_date,
        -- Churn label
        case
            when cf.recency_days is null         then true  -- no orders ever
            when cf.recency_days > 180           then true  -- churned
            else false
        end                                                        as is_churned,
        cf.recency_days                                            as days_since_last_purchase,
        -- All features
        cf.frequency_orders,
        cf.monetary_value,
        cf.avg_order_value,
        cf.std_order_value,
        cf.preferred_channel,
        cf.distinct_channels_used,
        cf.preferred_category,
        cf.days_since_signup,
        cf.loyalty_tier_encoded,
        cf.has_returned_items,
        cf.return_rate,
        cf.active_flag,
        cf.gender,
        cf.country,
        cf.acquisition_channel,
        cf.customer_tenure_days,
        -- Placeholder: predicted probability would be filled by external ML model
        -- and written back to Snowflake as a separate table
        cast(null as float)                                        as predicted_churn_probability_placeholder
    from customer_features cf
)

select * from final
