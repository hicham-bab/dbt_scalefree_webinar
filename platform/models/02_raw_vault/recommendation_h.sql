{{ config(
    materialized='incremental',
    tags=['hub', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__recommendations__fpr_records
      bk_columns:
          - recommendation_id
hashkey: hk_recommendation_h
business_keys:
    - recommendation_id
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}