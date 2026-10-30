{{ config(
    materialized='incremental',
    tags=['hub', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__pim__raw_products
      bk_columns:
          - product_id
hashkey: hk_product_h
business_keys:
    - product_id
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}
