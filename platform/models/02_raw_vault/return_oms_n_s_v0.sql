{{ config(
    materialized='incremental',
    tags=['satellite', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_model: stg__oms__raw_returns
parent_hashkey: hk_return_h
src_hashdiff: hd_return_oms_n_s
src_payload:
    - return_reason
    - return_requested_at
    - return_received_at
    - refund_amount
    - refund_status
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
