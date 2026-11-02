{{ config(materialized='view', tags=['vault_staging', 'recommendations']) }}

{#
  transaction_history, customer_product_interests, customer_product_usage and
  customer_product_usage_trend hold placeholders ("Product Usage 42") in every row
  and are not loaded into the vault. customer_id here is its own id space
  (CUST_000123) and is NOT a key of customer_h (CUST-0123), so it stays a payload
  column instead of becoming a link.
#}

{%- set yaml_metadata -%}
source_model:
    financial_services: 'fpr_records'
ldts: '_fivetran_synced'
rsrc: '!RECOMMENDATIONS.fpr_records'
derived_columns:
    recommendation_id:
        value: 'record_id'
        datatype: 'VARCHAR'
    is_deleted:
        value: 'coalesce(_fivetran_deleted, false)'
        datatype: 'BOOLEAN'
hashed_columns:
    hk_recommendation_h:
        - recommendation_id
    hd_recommendation_fpr_n_s:
        is_hashdiff: true
        columns:
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
    hd_recommendation_fpr_p_s:
        is_hashdiff: true
        columns:
            - customer_name
            - customer_email
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}