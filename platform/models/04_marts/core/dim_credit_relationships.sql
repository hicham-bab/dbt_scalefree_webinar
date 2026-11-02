-- One row per customer x institution credit relationship: the product held, its
-- exposure, its risk profile, its 36-period performance summary and where it stood
-- at the latest assessment (as_of_date).

with relationships as (

    select * from {{ ref('int_relationships_joined') }}

),

latest as (

    select * from {{ ref('int_relationship_latest_assessment') }}

)

select
    relationships.relationship_id,
    relationships.customer_id,
    relationships.institution_id,
    relationships.product_type,
    relationships.is_secured_product,
    relationships.risk_pattern,
    relationships.base_risk_score,
    relationships.total_exposure,
    relationships.relationship_length_months,
    relationships.products_held,
    relationships.primary_risk_factors,
    relationships.repayment_history_score,
    relationships.recent_transaction_volatility,
    relationships.collateral_quality_score,
    relationships.liquidity_ratio,
    relationships.projected_cash_flow_rating,
    relationships.avg_risk_score,
    relationships.risk_score_volatility,
    relationships.risk_trend,
    relationships.risk_trend_pct,
    relationships.total_transaction_volume,
    relationships.most_common_recommendation,
    relationships.source_approval_pct,
    relationships.avg_fraud_probability,
    relationships.risk_adjusted_return,
    relationships.customer_value_score,
    relationships.customer_value_category,
    relationships.risk_assessment_accuracy,
    relationships.optimization_priority,
    latest.latest_risk_score,
    latest.latest_risk_level,
    latest.latest_risk_level in ('High', 'Very High')   as is_latest_high_risk,
    latest.latest_approval_recommendation,
    latest.latest_assessment_date                        as as_of_date

from relationships
left join latest
    on relationships.relationship_id = latest.relationship_id