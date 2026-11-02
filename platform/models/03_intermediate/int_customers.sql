-- Current version of each risk-platform customer, rebuilt from the raw vault with the
-- business rules applied: precise types, 'True' / 'False' strings parsed to booleans,
-- Fivetran soft deletes removed. One row per customer_id.

with current_n as (
    select *
    from {{ ref('customer_risk_platform_n_s_v1') }}
    where is_current = true
),

current_p as (
    select *
    from {{ ref('customer_risk_platform_p_s_v1') }}
    where is_current = true
)

select
    h.customer_id,
    p.customer_name,
    n.segment                                               as customer_segment,
    cast(n.annual_income as number(14, 2))                  as annual_income,
    n.income_bracket,
    cast(n.credit_score as integer)                         as credit_score,
    n.credit_score_range,
    n.education_level,
    n.employment_sector,
    cast(n.years_of_credit_history as integer)              as years_of_credit_history,
    cast(n.debt_to_income_ratio as number(5, 2))            as debt_to_income_ratio,
    cast(n.num_credit_accounts as integer)                  as num_credit_accounts,
    cast(n.num_delinquencies_last_2_years as integer)       as num_delinquencies_last_2_years,
    cast(n.num_recent_inquiries as integer)                 as num_recent_inquiries,
    -- booleans land as the strings 'True' / 'False'
    lower(n.is_homeowner) = 'true'                          as is_homeowner,
    lower(n.has_previous_bankruptcy) = 'true'               as has_previous_bankruptcy,
    -- CUST-C001..CUST-C005 are hand-built showcase personas, not random customers
    h.customer_id like 'CUST-C%'                            as is_showcase_persona,
    n.ldts                                                  as synced_at

from {{ ref('customer_h') }} h
inner join current_n n on h.hk_customer_h = n.hk_customer_h
left join current_p p  on h.hk_customer_h = p.hk_customer_h
where h.hk_customer_h not in ({{ unknown_key() }}, {{ error_key() }})
  and not n.is_deleted