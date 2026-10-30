{{ config(
    materialized='incremental',
    tags=['satellite', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_model: stg__oms__raw_orders
parent_hashkey: hk_order_h
src_hashdiff: hd_order_oms_n_s
src_payload:
    - order_date
    - order_status
    - channel
    - shipping_city
    - shipping_country
    - shipping_method
    - discount_code
    - discount_amount
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
