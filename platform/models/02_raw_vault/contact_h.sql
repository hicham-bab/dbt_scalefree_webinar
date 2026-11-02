{{ config(
    materialized='incremental',
    tags=['hub', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__bank_marketing__predict_term_deposit
      bk_columns:
          - contact_id
hashkey: hk_contact_h
business_keys:
    - contact_id
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}