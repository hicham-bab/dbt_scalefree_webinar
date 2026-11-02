-- Current performance metrics of each customer x institution relationship.

with current_s as (
    select *
    from {{ ref('customer_institution_performance_n_s_v1') }}
    where is_current = true
)

select
    cast(s.performance_id as integer)                       as performance_id,
    c.customer_id,
    i.institution_id,
    s.product_type,
    cast(s.avg_risk_score as number(5, 3))                  as avg_risk_score,
    cast(s.risk_score_volatility as number(6, 3))           as risk_score_volatility,
    cast(s.risk_trend_percentage as number(8, 1))           as risk_trend_pct,
    s.risk_trend,
    cast(s.avg_monthly_transaction_volume as number(16, 2)) as avg_period_transaction_volume,
    cast(s.total_transaction_volume as number(18, 2))       as total_transaction_volume,
    cast(s.transaction_volatility as number(8, 3))          as transaction_volatility,
    s.most_common_recommendation,
    -- Source definition: share of periods recommended 'Approve' OR 'Approve with Conditions'.
    -- 'Approve with Strict Conditions' is NOT counted. 0..100.
    cast(s.approval_percentage as number(5, 1))             as source_approval_pct,
    cast(s.anomaly_percentage as number(5, 1))              as source_anomaly_pct,
    cast(s.avg_fraud_probability as number(5, 3))           as avg_fraud_probability,
    cast(s.risk_adjusted_return as number(6, 3))            as risk_adjusted_return,
    cast(s.customer_value_score as integer)                 as customer_value_score,
    s.customer_value_category,
    cast(s.risk_assessment_accuracy as number(5, 1))        as risk_assessment_accuracy,
    s.optimization_priority,
    s.optimization_opportunity,
    cast(s.total_exposure as number(16, 2))                 as total_exposure,
    s.ldts                                                  as synced_at

from {{ ref('customer_institution_l') }} l
inner join current_s s              on l.hk_customer_institution_l = s.hk_customer_institution_l
inner join {{ ref('customer_h') }} c    on l.hk_customer_h = c.hk_customer_h
inner join {{ ref('institution_h') }} i on l.hk_institution_h = i.hk_institution_h
where l.hk_customer_institution_l not in ({{ unknown_key() }}, {{ error_key() }})
  and not s.is_deleted