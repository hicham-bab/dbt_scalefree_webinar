-- One row per customer x institution relationship. Profiles and performance metrics
-- share that grain exactly (2,948 rows each, every row matches), and each
-- relationship holds a single product.

with profiles as (

    select * from {{ ref('int_risk_profiles') }}

),

performance as (

    select * from {{ ref('int_relationship_performance') }}

)

select
    profiles.customer_id || '|' || profiles.institution_id      as relationship_id,
    profiles.risk_profile_id,
    performance.performance_id,
    profiles.customer_id,
    profiles.institution_id,
    profiles.product_type,
    profiles.product_type in ('Mortgage', 'Business Loan')      as is_secured_product,
    profiles.risk_pattern,
    profiles.base_risk_score,
    profiles.total_exposure,
    profiles.relationship_length_months,
    profiles.products_held,
    profiles.primary_risk_factors,
    profiles.repayment_history_score,
    profiles.recent_transaction_volatility,
    profiles.collateral_quality_score,
    profiles.liquidity_ratio,
    profiles.projected_cash_flow_rating,
    performance.avg_risk_score,
    performance.risk_score_volatility,
    performance.risk_trend,
    performance.risk_trend_pct,
    performance.total_transaction_volume,
    performance.most_common_recommendation,
    performance.source_approval_pct,
    performance.avg_fraud_probability,
    performance.risk_adjusted_return,
    performance.customer_value_score,
    performance.customer_value_category,
    performance.risk_assessment_accuracy,
    performance.optimization_priority,
    performance.optimization_opportunity

from profiles
inner join performance
    on  profiles.customer_id = performance.customer_id
    and profiles.institution_id = performance.institution_id
    and profiles.product_type = performance.product_type