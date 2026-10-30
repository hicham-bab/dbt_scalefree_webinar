{{ config(
    materialized='incremental',
    tags=['hub', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__inventory__raw_inventory_movements
      bk_columns:
          - movement_id
hashkey: hk_inventory_movement_h
business_keys:
    - movement_id
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}
