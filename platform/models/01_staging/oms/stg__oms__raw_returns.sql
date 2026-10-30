{{ config(materialized='view', tags=['vault_staging', 'oms']) }}

{%- set yaml_metadata -%}
source_model: "raw_returns"
ldts: '_loaded_at'
rsrc: '!OMS.raw_returns'
hashed_columns:
    hk_return_h:
        - return_id
    hk_order_h:
        - order_id
    hk_order_item_h:
        - order_item_id
    hk_return_order_order_item_l:
        - return_id
        - order_id
        - order_item_id
    hd_return_oms_n_s:
        is_hashdiff: true
        columns:
            - return_reason
            - return_requested_at
            - return_received_at
            - refund_amount
            - refund_status
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}
