{{ config(
    materialized='incremental',
    tags=['non_historized_link', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_models:
    - name: stg__logistics__raw_shipments
      link_hashkey: hk_shipment_order_nl
      foreign_hashkeys:
          - hk_shipment_h
          - hk_order_h
link_hashkey: hk_shipment_order_nl
foreign_hashkeys:
    - hk_shipment_h
    - hk_order_h
{%- endset -%}

{{ datavault4dbt.link(yaml_metadata=yaml_metadata) }}
