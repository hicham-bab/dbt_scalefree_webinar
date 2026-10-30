{{ config(
    materialized='incremental',
    tags=['link', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__inventory__raw_inventory_movements
      link_hashkey: hk_movement_product_warehouse_l
      foreign_hashkeys:
          - hk_inventory_movement_h
          - hk_product_h
          - hk_warehouse_h
link_hashkey: hk_movement_product_warehouse_l
foreign_hashkeys:
    - hk_inventory_movement_h
    - hk_product_h
    - hk_warehouse_h
{%- endset -%}

{{ datavault4dbt.link(yaml_metadata=yaml_metadata) }}
