{{ config(
    materialized='incremental',
    tags=['satellite', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_model: stg__risk_platform__risk_assess_risk_profiles
parent_hashkey: hk_customer_institution_l
src_hashdiff: hd_customer_institution_risk_profile_n_s
src_payload:
    - risk_profile_id
    - product_type
    - risk_pattern
    - base_risk_score
    - total_exposure
    - relationship_length_months
    - products_held
    - primary_risk_factors
    - risk_factor_weights
    - repayment_history_score
    - recent_transaction_volatility
    - collateral_quality_score
    - liquidity_ratio
    - projected_cash_flow_rating
    - is_deleted
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}