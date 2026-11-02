-- One row per product recommendation made to a customer. Names and emails stay in
-- staging; marketing analysis does not need them.

select
    recommendation_id,
    recommendation_customer_id,
    customer_segment,
    customer_lifecycle_stage,
    account_balance,
    product_id,
    product_name,
    product_type,
    product_terms,
    recommended_product,
    recommendation_score,
    recommendation_date,
    recommendation_status,
    recommendation_status = 'Accepted'                      as is_accepted,
    customer_product_affinity,
    affinity_trend,
    customer_transaction_value,
    customer_transaction_count,
    product_sales_date,
    product_sales_amount,
    customer_satisfaction_score,
    customer_churn_probability,
    lifecycle_stage_transition_date

from {{ ref('int_product_recommendations') }}