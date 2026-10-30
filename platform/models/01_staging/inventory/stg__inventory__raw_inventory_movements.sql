{{ config(materialized='view', tags=['vault_staging', 'inventory']) }}

{%- set yaml_metadata -%}
source_model: "raw_inventory_movements"
ldts: '_loaded_at'
rsrc: '!Inventory.raw_inventory_movements'
hashed_columns:
    hk_inventory_movement_h:
        - movement_id
    hk_warehouse_h:
        - warehouse_id
    hk_product_h:
        - product_id
    hk_movement_product_warehouse_l:
        - movement_id
        - product_id
        - warehouse_id
    hd_inventory_movement_inventory_n_s:
        is_hashdiff: true
        columns:
            - movement_type
            - quantity
            - unit_cost
            - reason_code
            - occurred_at
            - product_id
            - warehouse_id
    hd_warehouse_inventory_n_s:
        is_hashdiff: true
        columns:
            - warehouse_name
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}
