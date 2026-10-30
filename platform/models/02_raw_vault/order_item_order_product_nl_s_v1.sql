{{ config(
    materialized='view',
    tags=['satellite', 'v1', 'raw_vault']
) }}

{%- set yaml_metadata -%}
sat_v0: order_item_order_product_nl_s_v0
hashkey: hk_order_item_order_product_nl
hashdiff: hd_order_item_order_product_nl_s
add_is_current_flag: true
{%- endset -%}

{{ datavault4dbt.sat_v1(yaml_metadata=yaml_metadata) }}
