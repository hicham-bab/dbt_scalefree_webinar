{{ config(
    materialized='incremental',
    tags=['non_historized_link', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__payment_gateway__raw_payments
      link_hashkey: hk_payment_order_nl
      foreign_hashkeys:
          - hk_payment_h
          - hk_order_h
link_hashkey: hk_payment_order_nl
foreign_hashkeys:
    - hk_payment_h
    - hk_order_h
{%- endset -%}

{{ datavault4dbt.link(yaml_metadata=yaml_metadata) }}
