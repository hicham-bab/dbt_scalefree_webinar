{{ config(
    materialized='incremental',
    tags=['satellite', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_model: stg__risk_platform__risk_assess_performance_metrics
parent_hashkey: hk_customer_institution_l
src_hashdiff: hd_customer_institution_performance_n_s
src_payload:
    - performance_id
    - product_type
    - avg_risk_score
    - risk_score_volatility
    - risk_trend_percentage
    - risk_trend
    - avg_monthly_transaction_volume
    - total_transaction_volume
    - transaction_volatility
    - most_common_recommendation
    - approval_percentage
    - anomaly_percentage
    - avg_fraud_probability
    - risk_adjusted_return
    - customer_value_score
    - customer_value_category
    - risk_assessment_accuracy
    - optimization_priority
    - optimization_opportunity
    - total_exposure
    - is_deleted
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}