{{ config(
    materialized='incremental',
    tags=['hub', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__risk_platform__risk_assess_customers
      bk_columns:
          - customer_id
hashkey: hk_customer_h
business_keys:
    - customer_id
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}