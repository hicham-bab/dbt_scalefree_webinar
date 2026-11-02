{{ config(
    materialized='incremental',
    tags=['satellite', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_model: stg__recommendations__fpr_records
parent_hashkey: hk_recommendation_h
src_hashdiff: hd_recommendation_fpr_n_s
src_payload:
    - customer_id
    - account_balance
    - product_id
    - product_name
    - product_type
    - product_terms
    - product_recommendation
    - recommendation_score
    - customer_segment
    - customer_lifecycle_stage
    - customer_transaction_value
    - customer_transaction_count
    - customer_product_affinity
    - product_sales_date
    - product_sales_amount
    - customer_satisfaction_score
    - customer_churn_probability
    - customer_lifecycle_stage_transition_date
    - product_recommendation_date
    - product_recommendation_status
    - customer_product_affinity_trend
    - is_deleted
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}