{{
  config(

    -- MART MODELS are the final, business-facing layer.
    -- They expose clean, well-named columns that analysts and BI tools consume.
    -- Marts are split into DIMENSIONS (descriptive attributes, like customers
    -- or products) and FACTS (measurable events, like orders or payments).

    -- This is a DIMENSION table: it describes WHO the customer is.
    materialized = 'table',

    schema = 'core',

    tags = ['mart', 'core', 'customers', 'pii'],

    -- GRANTS: dbt can manage warehouse-level permissions declaratively.
    -- After building this table, dbt will run:
    --   GRANT SELECT ON dim_customers TO ROLE analyst_role;
    -- This means access control lives in version-controlled code, not manual SQL.
    grants = {'select': ['analyst_role', 'marketing_role']},

    -- CONTRACT: when enabled, dbt enforces that the model's columns and
    -- data types match the YAML schema definition exactly. If someone adds
    -- a column in SQL but not in the YAML (or vice versa), dbt build fails.
    -- This is critical for models consumed by other teams or external tools.
    contract = {'enforced': true}
  )
}}

-- ============================================================================
-- DIMENSION MODEL: dim_customers
-- ============================================================================
-- Purpose : Business-ready customer profile with lifetime metrics and status.
-- Grain   : One row per customer
-- Depends : stg_customers, int_customer_order_summary
-- Consumers: marketing team (segmentation), finance (LTV), BI dashboards
-- ============================================================================

with customers as (
    select * from {{ ref('stg_customers') }}
),

order_summary as (
    select * from {{ ref('int_customer_order_summary') }}
),

final as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.first_name || ' ' || c.last_name                                      as full_name,
        c.email,
        c.city,
        c.country,
        c.birth_date,
        datediff('year', c.birth_date, current_date())                          as age,
        c.gender,
        c.signup_date,
        c.loyalty_tier,
        c.acquisition_channel,
        coalesce(o.total_orders, 0)                                             as total_orders,
        coalesce(o.total_revenue, 0)                                            as lifetime_value_eur,
        coalesce(o.avg_order_value, 0)                                          as avg_order_value_eur,
        o.first_order_date,
        o.last_order_date,
        o.days_since_last_order,
        o.customer_tenure_days,
        o.favorite_category,
        coalesce(o.total_items_returned, 0)                                     as total_items_returned,
        case
            when o.total_orders is null                                         then 'prospect'
            when o.last_order_date >= dateadd('day', -90, current_date())      then 'active'
            when o.last_order_date >= dateadd('day', -365, current_date())     then 'at_risk'
            else 'churned'
        end                                                                      as customer_status
    from customers c
    left join order_summary o on c.customer_id = o.customer_id
)

select * from final
