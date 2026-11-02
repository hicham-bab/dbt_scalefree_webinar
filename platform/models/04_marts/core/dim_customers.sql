-- One row per risk-platform customer, with their relationship footprint.

with customers as (

    select * from {{ ref('int_customers') }}

),

relationships as (

    select
        rel.customer_id,
        count(*)                                                     as relationship_count,
        count(distinct rel.institution_id)                           as institution_count,
        sum(rel.total_exposure)                                      as total_exposure,
        avg(latest.latest_risk_score)                                as avg_latest_risk_score,
        count_if(latest.latest_risk_level in ('High', 'Very High'))  as high_risk_relationship_count

    from {{ ref('int_relationships_joined') }} as rel
    left join {{ ref('int_relationship_latest_assessment') }} as latest
        on rel.relationship_id = latest.relationship_id
    group by 1

)

select
    customers.customer_id,
    customers.customer_name,
    customers.customer_segment,
    customers.annual_income,
    customers.income_bracket,
    customers.credit_score,
    customers.credit_score_range,
    customers.education_level,
    customers.employment_sector,
    customers.years_of_credit_history,
    customers.debt_to_income_ratio,
    customers.num_credit_accounts,
    customers.num_delinquencies_last_2_years,
    customers.num_recent_inquiries,
    customers.is_homeowner,
    customers.has_previous_bankruptcy,
    customers.is_showcase_persona,
    cast(coalesce(relationships.relationship_count, 0) as integer)            as relationship_count,
    cast(coalesce(relationships.institution_count, 0) as integer)             as institution_count,
    cast(coalesce(relationships.total_exposure, 0) as number(18, 2))          as total_exposure,
    cast(relationships.avg_latest_risk_score as number(5, 3))                 as avg_latest_risk_score,
    cast(coalesce(relationships.high_risk_relationship_count, 0) as integer)  as high_risk_relationship_count

from customers
left join relationships
    on customers.customer_id = relationships.customer_id