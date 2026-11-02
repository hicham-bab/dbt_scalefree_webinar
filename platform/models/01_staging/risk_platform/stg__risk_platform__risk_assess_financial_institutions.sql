{{ config(materialized='view', tags=['vault_staging', 'risk_platform']) }}

{%- set yaml_metadata -%}
source_model:
    financial_services: 'risk_assess_financial_institutions'
ldts: '_fivetran_synced'
rsrc: '!RISK_PLATFORM.risk_assess_financial_institutions'
derived_columns:
    is_deleted:
        value: 'coalesce(_fivetran_deleted, false)'
        datatype: 'BOOLEAN'
hashed_columns:
    hk_institution_h:
        - institution_id
    hd_institution_risk_platform_n_s:
        is_hashdiff: true
        columns:
            - institution_name
            - institution_type
            - institution_size
            - region
            - assets_under_management_billions
            - years_in_operation
            - risk_appetite
            - regulatory_rating
            - customer_base_millions
            - digital_maturity_score
            - primary_risk_model
            - avg_customer_lifetime_years
            - fraud_loss_percentage
            - default_rate_percentage
            - is_deleted
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}