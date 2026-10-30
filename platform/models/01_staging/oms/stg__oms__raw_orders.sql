{{ config(materialized='view', tags=['vault_staging', 'oms']) }}

{%- set yaml_metadata -%}
source_model: "raw_orders"
ldts: '_loaded_at'
rsrc: '!OMS.raw_orders'
hashed_columns:
    hk_order_h:
        - order_id
    hk_customer_h:
        - customer_id
    hk_order_customer_l:
        - order_id
        - customer_id
    hd_order_oms_n_s:
        is_hashdiff: true
        columns:
            - order_date
            - order_status
            - channel
            - shipping_city
            - shipping_country
            - shipping_method
            - discount_code
            - discount_amount
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}
