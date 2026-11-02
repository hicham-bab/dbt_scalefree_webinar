{{ config(materialized='view', tags=['vault_staging', 'risk_platform']) }}

{#
  The source month / year columns are loaded as delivered. They are derived from a
  date that steps 30 days at a time, so they double-count January and May 2022;
  03_intermediate drops them in favour of period_number.
#}

{%- set yaml_metadata -%}
source_model:
    financial_services: 'risk_assess_monthly_assessments'
ldts: '_fivetran_synced'
rsrc: '!RISK_PLATFORM.risk_assess_monthly_assessments'
derived_columns:
    is_deleted:
        value: 'coalesce(_fivetran_deleted, false)'
        datatype: 'BOOLEAN'
hashed_columns:
    hk_assessment_h:
        - assessment_id
    hk_customer_h:
        - customer_id
    hk_institution_h:
        - institution_id
    hk_assessment_customer_institution_nl:
        - assessment_id
        - customer_id
        - institution_id
    hd_assessment_customer_institution_nl_s:
        is_hashdiff: true
        columns:
            - date
            - month
            - year
            - current_risk_score
            - risk_level
            - monthly_transaction_volume
            - transaction_count
            - approval_recommendation
            - approval_confidence
            - primary_explanation_factors
            - fraud_probability
            - is_anomaly
            - anomaly_type
            - risk_change_from_previous
            - is_deleted
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}