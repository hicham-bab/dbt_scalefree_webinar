{#
  ============================================================================
  MART MODELS -- FACT TABLE (INCREMENTAL)
  ============================================================================
  A FACT table records measurable business events: here, one risk assessment
  of one customer at one institution in one 30-day period (106,128 rows).

  CONFIG EXPLAINED:

  - materialized = 'incremental'
      Only new or changed rows are processed on each run instead of a full
      rebuild. The risk platform appends one assessment per relationship per
      period, the textbook incremental pattern.

  - unique_key = 'assessment_id'
      Identifies a row. On incremental runs dbt MERGEs on it: new ids are
      inserted, re-scored assessments are updated.

  - incremental_strategy = 'merge'
      'merge'            -> MERGE INTO (inserts + updates)
      'delete+insert'    -> delete matching rows, then insert
      'insert_overwrite' -> replace whole partitions
      'append'           -> insert only, fastest, for immutable events

  - on_schema_change = 'append_new_columns'
      Adds new columns to the table when the SQL grows. Combined with the
      enforced contract in _core_models.yml, a column cannot silently change
      type or disappear: the build fails first.

  The is_incremental() filter below reprocesses the latest loaded period so a
  late re-score of the most recent assessment is picked up.
  ============================================================================
#}

{{
  config(
    materialized = 'incremental',
    unique_key = 'assessment_id',
    incremental_strategy = 'merge',
    on_schema_change = 'append_new_columns',
    tags = ['mart', 'core', 'credit_risk']
  )
}}

with assessments as (

    select * from {{ ref('int_risk_assessments') }}

    {% if is_incremental() %}
    where assessment_date >= (select max(assessment_date) from {{ this }})
    {% endif %}

),

periods as (

    select * from {{ ref('int_assessment_periods') }}

)

select
    assessments.assessment_id,
    assessments.customer_id || '|' || assessments.institution_id      as relationship_id,
    assessments.customer_id,
    assessments.institution_id,
    assessments.assessment_date,
    periods.period_number,
    assessments.risk_score,
    assessments.risk_level,
    assessments.risk_level in ('High', 'Very High')                   as is_high_risk,
    assessments.approval_recommendation,
    -- The source system's definition of "approved" (matches
    -- risk_assess_performance_metrics.approval_percentage): 'Approve' or
    -- 'Approve with Conditions'. 'Approve with Strict Conditions' is not approved.
    assessments.approval_recommendation in ('Approve', 'Approve with Conditions') as is_approved,
    assessments.approval_recommendation like 'Approve%'               as is_any_approval,
    assessments.approval_recommendation = 'Deny'                      as is_denied,
    assessments.approval_confidence,
    assessments.primary_explanation_factors,
    assessments.fraud_probability,
    assessments.is_anomaly,
    assessments.anomaly_type,
    coalesce(assessments.anomaly_type = 'Fraud Alert', false)         as is_fraud_alert,
    assessments.risk_change_from_previous,
    assessments.transaction_volume,
    assessments.transaction_count

from assessments
inner join periods
    on assessments.assessment_date = periods.assessment_date