{{ config(materialized='view', tags=['vault_staging', 'risk_platform']) }}

{%- set yaml_metadata -%}
source_model:
    financial_services: 'risk_assess_customers'
ldts: '_fivetran_synced'
rsrc: '!RISK_PLATFORM.risk_assess_customers'
derived_columns:
    is_deleted:
        value: 'coalesce(_fivetran_deleted, false)'
        datatype: 'BOOLEAN'
hashed_columns:
    hk_customer_h:
        - customer_id
    hd_customer_risk_platform_n_s:
        is_hashdiff: true
        columns:
            - segment
            - annual_income
            - income_bracket
            - credit_score
            - credit_score_range
            - education_level
            - employment_sector
            - years_of_credit_history
            - debt_to_income_ratio
            - num_credit_accounts
            - num_delinquencies_last_2_years
            - num_recent_inquiries
            - is_homeowner
            - has_previous_bankruptcy
            - is_deleted
    hd_customer_risk_platform_p_s:
        is_hashdiff: true
        columns:
            - customer_name
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}