{{ config(
    materialized='incremental',
    tags=['satellite', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_model: stg__inventory__raw_inventory_movements
parent_hashkey: hk_warehouse_h
src_hashdiff: hd_warehouse_inventory_n_s
src_payload:
    - warehouse_name
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
