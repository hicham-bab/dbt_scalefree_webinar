{{ config(
    materialized='incremental',
    tags=['satellite', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_model: stg__pim__raw_products
parent_hashkey: hk_product_h
src_hashdiff: hd_product_pim_n_s
src_payload:
    - product_name
    - category
    - subcategory
    - material
    - color
    - unit_price
    - cost_price
    - sku_code
    - collection
    - is_active
    - launched_at
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
