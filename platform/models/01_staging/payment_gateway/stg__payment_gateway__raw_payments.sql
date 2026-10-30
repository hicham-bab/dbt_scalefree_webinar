{{ config(materialized='view', tags=['vault_staging', 'payment_gateway']) }}

{%- set yaml_metadata -%}
source_model: "raw_payments"
ldts: '_loaded_at'
rsrc: '!Payment_Gateway.raw_payments'
hashed_columns:
    hk_payment_h:
        - payment_id
    hk_order_h:
        - order_id
    hk_payment_order_nl:
        - payment_id
        - order_id
    hd_payment_pgw_n_s:
        is_hashdiff: true
        columns:
            - payment_date
            - payment_method
            - amount
            - currency
            - payment_status
            - gateway
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}
