{{ config(
    materialized='incremental',
    tags=['hub', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__payment_gateway__raw_payments
      bk_columns:
          - payment_id
hashkey: hk_payment_h
business_keys:
    - payment_id
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}
