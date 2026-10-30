{{ config(
    materialized='incremental',
    tags=['satellite', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_model: stg__oms__raw_order_items
parent_hashkey: hk_order_item_order_product_nl
src_hashdiff: hd_order_item_order_product_nl_s
src_payload:
    - quantity
    - unit_price_at_order
    - discount_pct
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
