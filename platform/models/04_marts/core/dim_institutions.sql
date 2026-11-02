-- One row per member institution of the risk platform.

with institutions as (

    select * from {{ ref('int_institutions') }}

),

relationships as (

    select
        institution_id,
        count(*)                        as relationship_count,
        count(distinct customer_id)     as customer_count,
        sum(total_exposure)             as total_exposure

    from {{ ref('int_relationships_joined') }}
    group by 1

)

select
    institutions.institution_id,
    institutions.institution_name,
    institutions.institution_type,
    institutions.institution_size,
    institutions.region,
    institutions.assets_under_management_billions,
    institutions.years_in_operation,
    institutions.risk_appetite,
    institutions.regulatory_rating,
    institutions.customer_base_millions,
    institutions.digital_maturity_score,
    institutions.primary_risk_model,
    institutions.avg_customer_lifetime_years,
    institutions.fraud_loss_pct,
    institutions.default_rate_pct,
    institutions.is_showcase_persona,
    cast(coalesce(relationships.relationship_count, 0) as integer)     as relationship_count,
    cast(coalesce(relationships.customer_count, 0) as integer)         as customer_count,
    cast(coalesce(relationships.total_exposure, 0) as number(18, 2))   as total_exposure

from institutions
left join relationships
    on institutions.institution_id = relationships.institution_id