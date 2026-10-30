{{ config(
    materialized='incremental',
    tags=['satellite', 'raw_vault']
) }}

{%- set yaml_metadata -%}
source_model: stg__crm__raw_customers
parent_hashkey: hk_customer_h
src_hashdiff: hd_customer_crm_p_s
src_payload:
    - first_name
    - last_name
    - email
    - phone
    - birth_date
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
