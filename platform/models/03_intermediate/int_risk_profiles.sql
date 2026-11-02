-- Current risk profile of each customer x institution relationship.

with current_s as (
    select *
    from {{ ref('customer_institution_risk_profile_n_s_v1') }}
    where is_current = true
)

select
    cast(s.risk_profile_id as integer)                      as risk_profile_id,
    c.customer_id,
    i.institution_id,
    s.product_type,
    s.risk_pattern,
    cast(s.base_risk_score as number(5, 3))                 as base_risk_score,
    cast(s.total_exposure as number(16, 2))                 as total_exposure,
    cast(s.relationship_length_months as integer)           as relationship_length_months,
    cast(s.products_held as integer)                        as products_held,
    s.primary_risk_factors,
    s.risk_factor_weights,
    cast(s.repayment_history_score as number(5, 2))         as repayment_history_score,
    cast(s.recent_transaction_volatility as number(8, 3))   as recent_transaction_volatility,
    -- These three land as VARCHAR and are empty where the measure does not apply to the
    -- product, not where data is missing (verified on all 2,948 rows):
    --   collateral_quality_score   -> Mortgage and Business Loan only (secured lending)
    --   liquidity_ratio            -> Business Loan only
    --   projected_cash_flow_rating -> Business Loan and Personal Loan only
    try_cast(nullif(s.collateral_quality_score, '') as number(4, 1))   as collateral_quality_score,
    try_cast(nullif(s.liquidity_ratio, '') as number(8, 2))            as liquidity_ratio,
    try_cast(nullif(s.projected_cash_flow_rating, '') as number(4, 1)) as projected_cash_flow_rating,
    s.ldts                                                  as synced_at

from {{ ref('customer_institution_l') }} l
inner join current_s s              on l.hk_customer_institution_l = s.hk_customer_institution_l
inner join {{ ref('customer_h') }} c    on l.hk_customer_h = c.hk_customer_h
inner join {{ ref('institution_h') }} i on l.hk_institution_h = i.hk_institution_h
where l.hk_customer_institution_l not in ({{ unknown_key() }}, {{ error_key() }})
  and not s.is_deleted