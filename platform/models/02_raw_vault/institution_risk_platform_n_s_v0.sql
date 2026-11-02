{{ config(
    materialized='incremental',
    tags=['satellite', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_model: stg__risk_platform__risk_assess_financial_institutions
parent_hashkey: hk_institution_h
src_hashdiff: hd_institution_risk_platform_n_s
src_payload:
    - institution_name
    - institution_type
    - institution_size
    - region
    - assets_under_management_billions
    - years_in_operation
    - risk_appetite
    - regulatory_rating
    - customer_base_millions
    - digital_maturity_score
    - primary_risk_model
    - avg_customer_lifetime_years
    - fraud_loss_percentage
    - default_rate_percentage
    - is_deleted
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}