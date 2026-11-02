-- One row per risk assessment, rebuilt from the non-historized link and its satellite.

with current_s as (
    select *
    from {{ ref('assessment_customer_institution_nl_s_v1') }}
    where is_current = true
)

select
    cast(a.assessment_id as integer)                        as assessment_id,
    c.customer_id,
    i.institution_id,
    -- "date" lands as VARCHAR. The source month/year columns are dropped on purpose:
    -- periods are 30 days apart, so month/year double-count January and May 2022.
    cast(s."DATE" as date)                                  as assessment_date,
    cast(s.current_risk_score as number(5, 3))              as risk_score,
    s.risk_level,
    cast(s.monthly_transaction_volume as number(16, 2))     as transaction_volume,
    cast(s.transaction_count as integer)                    as transaction_count,
    s.approval_recommendation,
    cast(s.approval_confidence as number(5, 3))             as approval_confidence,
    s.primary_explanation_factors,
    cast(s.fraud_probability as number(5, 3))               as fraud_probability,
    lower(s.is_anomaly) = 'true'                            as is_anomaly,
    -- 'None' is the source's way of saying "no anomaly"
    nullif(s.anomaly_type, 'None')                          as anomaly_type,
    -- VARCHAR, empty on each relationship's first period
    try_cast(nullif(s.risk_change_from_previous, '') as number(6, 3)) as risk_change_from_previous,
    s.ldts                                                  as synced_at

from {{ ref('assessment_customer_institution_nl') }} nl
inner join current_s s                  on nl.hk_assessment_customer_institution_nl = s.hk_assessment_customer_institution_nl
inner join {{ ref('assessment_h') }} a  on nl.hk_assessment_h = a.hk_assessment_h
inner join {{ ref('customer_h') }} c    on nl.hk_customer_h = c.hk_customer_h
inner join {{ ref('institution_h') }} i on nl.hk_institution_h = i.hk_institution_h
where nl.hk_assessment_customer_institution_nl not in ({{ unknown_key() }}, {{ error_key() }})
  and not s.is_deleted