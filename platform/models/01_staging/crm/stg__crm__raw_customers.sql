{{ config(materialized='view', tags=['vault_staging', 'crm']) }}

{%- set yaml_metadata -%}
source_model: "raw_customers"
ldts: '_loaded_at'
rsrc: '!CRM.raw_customers'
hashed_columns:
    hk_customer_h:
        - customer_id
    hd_customer_crm_n_s:
        is_hashdiff: true
        columns:
            - gender
            - city
            - country
            - signup_date
            - loyalty_tier
            - acquisition_channel
    hd_customer_crm_p_s:
        is_hashdiff: true
        columns:
            - first_name
            - last_name
            - email
            - phone
            - birth_date
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}
