{{ config(
    materialized='incremental',
    tags=['hub', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__inventory__raw_inventory_movements
      bk_columns:
          - warehouse_id
hashkey: hk_warehouse_h
business_keys:
    - warehouse_id
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}
