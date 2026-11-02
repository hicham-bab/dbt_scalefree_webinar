{{ config(materialized='view', tags=['vault_staging', 'risk_platform']) }}

{%- set yaml_metadata -%}
source_model:
    financial_services: 'risk_assess_risk_profiles'
ldts: '_fivetran_synced'
rsrc: '!RISK_PLATFORM.risk_assess_risk_profiles'
derived_columns:
    is_deleted:
        value: 'coalesce(_fivetran_deleted, false)'
        datatype: 'BOOLEAN'
hashed_columns:
    hk_customer_h:
        - customer_id
    hk_institution_h:
        - institution_id
    hk_customer_institution_l:
        - customer_id
        - institution_id
    hd_customer_institution_risk_profile_n_s:
        is_hashdiff: true
        columns:
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

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}