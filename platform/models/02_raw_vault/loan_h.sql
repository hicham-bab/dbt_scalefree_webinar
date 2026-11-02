{{ config(
    materialized='incremental',
    tags=['hub', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__lending__loan
      bk_columns:
          - loan_id
hashkey: hk_loan_h
business_keys:
    - loan_id
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}