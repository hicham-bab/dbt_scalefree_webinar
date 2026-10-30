{{ config(materialized='view', tags=['vault_staging', 'oms']) }}

{%- set yaml_metadata -%}
source_model: "raw_order_items"
ldts: '_loaded_at'
rsrc: '!OMS.raw_order_items'
hashed_columns:
    hk_order_item_h:
        - order_item_id
    hk_order_h:
        - order_id
    hk_product_h:
        - product_id
    hk_order_item_order_product_nl:
        - order_item_id
        - order_id
        - product_id
    hd_order_item_order_product_nl_s:
        is_hashdiff: true
        columns:
            - quantity
            - unit_price_at_order
            - discount_pct
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}
