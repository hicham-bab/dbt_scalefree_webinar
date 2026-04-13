{#
  ============================================================================
  WHAT IS A dbt MODEL?
  ============================================================================
  A dbt "model" is simply a SQL SELECT statement stored in a .sql file.
  dbt compiles it, wraps it in DDL (CREATE TABLE / VIEW), and runs it
  against your data warehouse. No INSERT, no procedural code -- just SELECT.

  THE CONFIG BLOCK below tells dbt HOW to build this model:

  - materialized = 'view'
      How dbt persists the result of this SELECT.
        'view'         -> CREATE VIEW  (re-runs the query every time it's read)
        'table'        -> CREATE TABLE (stores the result; rebuilds entirely each run)
        'incremental'  -> INSERT/MERGE (appends or upserts only new/changed rows)
        'ephemeral'    -> no object created; the SQL is inlined as a CTE wherever referenced

      Staging models are lightweight cleaning layers, so a VIEW is the right
      choice: no extra storage cost, always fresh when queried.

  - schema = 'staging'
      Controls which schema/dataset this model lands in.
      dbt builds the full name as: <target_database>.<target_schema>_<schema>.
      e.g. if your target schema is 'analytics', this model goes to 'analytics_staging'.

  - tags = ['staging', 'pii']
      Labels you can attach to any model for selective runs.
      Run only staging models:  dbt run --select tag:staging
      Run everything for PII:   dbt run --select tag:pii
  ============================================================================
#}

{{
  config(
    materialized = 'view',
    schema = 'staging',
    tags = ['staging', 'pii']
  )
}}

-- ============================================================================
-- STAGING MODEL: stg_customers
-- ============================================================================
-- Purpose : Clean, rename, and type-cast raw source data. No business logic.
-- Source  : {{ source('ecommerce_raw', 'raw_customers') }}
-- Grain   : One row per customer
-- ============================================================================

with source as (
    select * from {{ source('ecommerce_raw', 'raw_customers') }}
),

renamed as (
    select
        cast(customer_id as varchar(36))                    as customer_id,
        cast(first_name as varchar(100))                    as first_name,
        cast(last_name as varchar(100))                     as last_name,
        trim(lower(cast(email as varchar(255))))            as email,
        cast(phone as varchar(50))                         as phone,
        cast(birth_date as date)                           as birth_date,
        cast(gender as varchar(10))                        as gender,
        cast(city as varchar(100))                         as city,
        cast(country as varchar(100))                      as country,
        cast(signup_date as date)                          as signup_date,
        cast(loyalty_tier as varchar(20))                  as loyalty_tier,
        cast(acquisition_channel as varchar(50))           as acquisition_channel,
        cast(_loaded_at as timestamp_ntz)                  as loaded_at
    from source
)

select * from renamed
