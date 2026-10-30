{{ config(materialized='view', tags=['vault_staging', 'logistics']) }}

{%- set yaml_metadata -%}
source_model: "raw_shipments"
ldts: '_loaded_at'
rsrc: '!Logistics.raw_shipments'
hashed_columns:
    hk_shipment_h:
        - shipment_id
    hk_order_h:
        - order_id
    hk_shipment_order_nl:
        - shipment_id
        - order_id
    hd_shipment_logistics_n_s:
        is_hashdiff: true
        columns:
            - carrier
            - tracking_number
            - estimated_delivery_date
            - shipped_at
            - delivered_at
            - shipment_status
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}
