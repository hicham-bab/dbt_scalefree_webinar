{{ config(
    materialized='incremental',
    tags=['satellite', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_model: stg__risk_platform__risk_assess_monthly_assessments
parent_hashkey: hk_assessment_customer_institution_nl
src_hashdiff: hd_assessment_customer_institution_nl_s
src_payload:
    - date
    - month
    - year
    - current_risk_score
    - risk_level
    - monthly_transaction_volume
    - transaction_count
    - approval_recommendation
    - approval_confidence
    - primary_explanation_factors
    - fraud_probability
    - is_anomaly
    - anomaly_type
    - risk_change_from_previous
    - is_deleted
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}