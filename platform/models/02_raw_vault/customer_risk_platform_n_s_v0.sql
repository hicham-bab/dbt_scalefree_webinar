{{ config(
    materialized='incremental',
    tags=['satellite', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_model: stg__risk_platform__risk_assess_customers
parent_hashkey: hk_customer_h
src_hashdiff: hd_customer_risk_platform_n_s
src_payload:
    - segment
    - annual_income
    - income_bracket
    - credit_score
    - credit_score_range
    - education_level
    - employment_sector
    - years_of_credit_history
    - debt_to_income_ratio
    - num_credit_accounts
    - num_delinquencies_last_2_years
    - num_recent_inquiries
    - is_homeowner
    - has_previous_bankruptcy
    - is_deleted
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}