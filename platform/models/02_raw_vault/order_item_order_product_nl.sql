{{ config(
    materialized='incremental',
    tags=['non_historized_link', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__oms__raw_order_items
      link_hashkey: hk_order_item_order_product_nl
      foreign_hashkeys:
          - hk_order_item_h
          - hk_order_h
          - hk_product_h
link_hashkey: hk_order_item_order_product_nl
foreign_hashkeys:
    - hk_order_item_h
    - hk_order_h
    - hk_product_h
{%- endset -%}

{{ datavault4dbt.link(yaml_metadata=yaml_metadata) }}
