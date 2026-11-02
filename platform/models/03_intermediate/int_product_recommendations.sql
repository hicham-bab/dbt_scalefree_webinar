-- Current version of each product recommendation. customer_id here is its own id space
-- (CUST_000123) and does not join to int_customers (CUST-0123).

with current_n as (
    select *
    from {{ ref('recommendation_fpr_n_s_v1') }}
    where is_current = true
),

current_p as (
    select *
    from {{ ref('recommendation_fpr_p_s_v1') }}
    where is_current = true
)

select
    h.recommendation_id,
    n.customer_id                                           as recommendation_customer_id,
    p.customer_name,
    lower(trim(p.customer_email))                           as customer_email,
    cast(n.account_balance as number(14, 2))                as account_balance,
    n.product_id,
    n.product_name,
    n.product_type,
    n.product_terms,
    n.product_recommendation                                as recommended_product,
    cast(n.recommendation_score as number(6, 3))            as recommendation_score,
    n.customer_segment,
    n.customer_lifecycle_stage,
    cast(n.customer_transaction_value as number(14, 2))     as customer_transaction_value,
    cast(n.customer_transaction_count as integer)           as customer_transaction_count,
    cast(n.customer_product_affinity as number(6, 3))       as customer_product_affinity,
    cast(n.product_sales_date as date)                      as product_sales_date,
    cast(n.product_sales_amount as number(14, 2))           as product_sales_amount,
    cast(n.customer_satisfaction_score as number(6, 2))     as customer_satisfaction_score,
    cast(n.customer_churn_probability as number(6, 3))      as customer_churn_probability,
    cast(n.customer_lifecycle_stage_transition_date as date) as lifecycle_stage_transition_date,
    cast(n.product_recommendation_date as date)             as recommendation_date,
    n.product_recommendation_status                         as recommendation_status,
    n.customer_product_affinity_trend                       as affinity_trend,
    n.ldts                                                  as synced_at

from {{ ref('recommendation_h') }} h
inner join current_n n on h.hk_recommendation_h = n.hk_recommendation_h
left join current_p p  on h.hk_recommendation_h = p.hk_recommendation_h
where h.hk_recommendation_h not in ({{ unknown_key() }}, {{ error_key() }})
  and not n.is_deleted