-- Current version of each member institution. One row per institution_id.

with current_n as (
    select *
    from {{ ref('institution_risk_platform_n_s_v1') }}
    where is_current = true
)

select
    h.institution_id,
    n.institution_name,
    n.institution_type,
    n.institution_size,
    n.region,
    cast(n.assets_under_management_billions as number(12, 2))  as assets_under_management_billions,
    cast(n.years_in_operation as integer)                       as years_in_operation,
    -- 0..1 scale
    cast(n.risk_appetite as number(4, 2))                       as risk_appetite,
    -- 1 (best) .. 5
    cast(n.regulatory_rating as integer)                        as regulatory_rating,
    cast(n.customer_base_millions as number(10, 2))             as customer_base_millions,
    -- 1..10
    cast(n.digital_maturity_score as integer)                   as digital_maturity_score,
    n.primary_risk_model,
    cast(n.avg_customer_lifetime_years as number(6, 2))         as avg_customer_lifetime_years,
    -- already in percent (0.05 means 0.05%, not 5%)
    cast(n.fraud_loss_percentage as number(6, 3))               as fraud_loss_pct,
    cast(n.default_rate_percentage as number(6, 2))             as default_rate_pct,
    h.institution_id like 'FI-C%'                               as is_showcase_persona,
    n.ldts                                                      as synced_at

from {{ ref('institution_h') }} h
inner join current_n n on h.hk_institution_h = n.hk_institution_h
where h.hk_institution_h not in ({{ unknown_key() }}, {{ error_key() }})
  and not n.is_deleted