-- RFM-based customer segmentation using platform public dim_customers model.
-- Computes Recency, Frequency, and Monetary scores and derives segment labels.
-- Grain: one row per customer.
with customers as (
    select
        customer_id,
        full_name,
        email,
        country,
        loyalty_tier,
        acquisition_channel,
        customer_status,
        total_orders                                                 as frequency,
        lifetime_value_eur                                          as monetary,
        days_since_last_order                                       as recency_days,
        last_order_date,
        first_order_date,
        favorite_category
    from {{ ref('platform', 'dim_customers') }}
),

-- Score each dimension 1-5 using ntile
rfm_scores as (
    select
        customer_id,
        full_name,
        email,
        country,
        loyalty_tier,
        acquisition_channel,
        customer_status,
        frequency,
        monetary,
        recency_days,
        last_order_date,
        first_order_date,
        favorite_category,
        -- Recency: lower days = better (score 5), so reverse order
        case
            when recency_days is null                          then 1
            when recency_days <= 30                            then 5
            when recency_days <= 60                            then 4
            when recency_days <= 90                            then 3
            when recency_days <= 180                           then 2
            else 1
        end                                                          as recency_score,
        -- Frequency: more orders = better
        case
            when frequency = 0                                 then 1
            when frequency = 1                                 then 2
            when frequency <= 3                                then 3
            when frequency <= 6                                then 4
            else 5
        end                                                          as frequency_score,
        -- Monetary: higher spend = better
        case
            when monetary = 0                                  then 1
            when monetary < 200                                then 2
            when monetary < 600                                then 3
            when monetary < 1500                               then 4
            else 5
        end                                                          as monetary_score
    from customers
),

final as (
    select
        customer_id,
        full_name,
        email,
        country,
        loyalty_tier,
        acquisition_channel,
        customer_status,
        frequency                                                    as order_count,
        monetary                                                     as lifetime_value_eur,
        recency_days                                                 as days_since_last_order,
        last_order_date,
        first_order_date,
        favorite_category,
        recency_score,
        frequency_score,
        monetary_score,
        recency_score + frequency_score + monetary_score            as rfm_total_score,
        -- Segment assignment based on RFM profile
        case
            when recency_score >= 4 and frequency_score >= 4 and monetary_score >= 4
                then 'Champions'
            when recency_score >= 3 and frequency_score >= 3
                then 'Loyal'
            when recency_score >= 4 and frequency_score <= 2
                then 'New'
            when recency_score >= 3 and monetary_score >= 4
                then 'Potential Loyalist'
            when recency_score <= 2 and frequency_score >= 3 and monetary_score >= 3
                then 'At Risk'
            when recency_score <= 2 and frequency_score >= 3
                then 'Cant Lose Them'
            when recency_score <= 2 and frequency_score <= 2 and monetary_score >= 3
                then 'Hibernating'
            when recency_score <= 1
                then 'Lost'
            else 'Promising'
        end                                                          as rfm_segment,
        current_date()                                               as segmented_at
    from rfm_scores
)

select * from final
