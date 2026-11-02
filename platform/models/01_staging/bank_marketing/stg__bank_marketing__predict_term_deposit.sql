{{ config(materialized='view', tags=['vault_staging', 'bank_marketing']) }}

{%- set yaml_metadata -%}
source_model:
    financial_services: 'predict_term_deposit'
ldts: '_fivetran_synced'
rsrc: '!BANK_MARKETING.predict_term_deposit'
derived_columns:
    contact_id:
        value: 'id'
        datatype: 'NUMBER(38,0)'
    is_deleted:
        value: 'coalesce(_fivetran_deleted, false)'
        datatype: 'BOOLEAN'
hashed_columns:
    hk_contact_h:
        - contact_id
    hd_contact_bank_marketing_n_s:
        is_hashdiff: true
        columns:
            - age
            - job
            - marital
            - education
            - c_default
            - balance
            - housing
            - loan
            - contact
            - day
            - month
            - duration
            - campaign
            - pdays
            - previous
            - poutcome
            - y
            - is_deleted
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}