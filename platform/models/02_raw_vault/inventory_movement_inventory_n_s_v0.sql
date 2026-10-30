{{ config(
    materialized='incremental',
    tags=['satellite', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_model: stg__inventory__raw_inventory_movements
parent_hashkey: hk_inventory_movement_h
src_hashdiff: hd_inventory_movement_inventory_n_s
src_payload:
    - movement_type
    - quantity
    - unit_cost
    - reason_code
    - occurred_at
    - product_id
    - warehouse_id
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
