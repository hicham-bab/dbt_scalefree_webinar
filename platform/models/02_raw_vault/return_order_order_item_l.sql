{{ config(
    materialized='incremental',
    tags=['link', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__oms__raw_returns
      link_hashkey: hk_return_order_order_item_l
      foreign_hashkeys:
          - hk_return_h
          - hk_order_h
          - hk_order_item_h
link_hashkey: hk_return_order_order_item_l
foreign_hashkeys:
    - hk_return_h
    - hk_order_h
    - hk_order_item_h
{%- endset -%}

{{ datavault4dbt.link(yaml_metadata=yaml_metadata) }}
