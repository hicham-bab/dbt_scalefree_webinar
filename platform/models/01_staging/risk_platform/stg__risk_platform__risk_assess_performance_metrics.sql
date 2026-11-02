{{ config(materialized='view', tags=['vault_staging', 'risk_platform']) }}

{%- set yaml_metadata -%}
source_model:
    financial_services: 'risk_assess_performance_metrics'
ldts: '_fivetran_synced'
rsrc: '!RISK_PLATFORM.risk_assess_performance_metrics'
derived_columns:
    is_deleted:
        value: 'coalesce(_fivetran_deleted, false)'
        datatype: 'BOOLEAN'
hashed_columns:
    hk_customer_h:
        - customer_id
    hk_institution_h:
        - institution_id
    hk_customer_institution_l:
        - customer_id
        - institution_id
    hd_customer_institution_performance_n_s:
        is_hashdiff: true
        columns:
            - performance_id
            - product_type
            - avg_risk_score
            - risk_score_volatility
            - risk_trend_percentage
            - risk_trend
            - avg_monthly_transaction_volume
            - total_transaction_volume
            - transaction_volatility
            - most_common_recommendation
            - approval_percentage
            - anomaly_percentage
            - avg_fraud_probability
            - risk_adjusted_return
            - customer_value_score
            - customer_value_category
            - risk_assessment_accuracy
            - optimization_priority
            - optimization_opportunity
            - total_exposure
            - is_deleted
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}