{{ config(
    materialized='incremental',
    tags=['link', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__oms__raw_orders
      link_hashkey: hk_order_customer_l
      foreign_hashkeys:
          - hk_order_h
          - hk_customer_h
link_hashkey: hk_order_customer_l
foreign_hashkeys:
    - hk_order_h
    - hk_customer_h
{%- endset -%}

{{ datavault4dbt.link(yaml_metadata=yaml_metadata) }}
