{{ config(
    materialized='incremental',
    tags=['satellite', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_model: stg__logistics__raw_shipments
parent_hashkey: hk_shipment_h
src_hashdiff: hd_shipment_logistics_n_s
src_payload:
    - carrier
    - tracking_number
    - estimated_delivery_date
    - shipped_at
    - delivered_at
    - shipment_status
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
