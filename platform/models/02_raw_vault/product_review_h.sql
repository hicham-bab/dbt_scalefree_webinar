{{ config(
    materialized='incremental',
    tags=['hub', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__oms__raw_product_reviews
      bk_columns:
          - review_id
hashkey: hk_product_review_h
business_keys:
    - review_id
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}
