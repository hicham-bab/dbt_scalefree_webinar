-- Customer, institution and relationship grains must all report the same total exposure.
with totals as (
    select 'relationships' as source, sum(total_exposure) as exposure from {{ ref('dim_credit_relationships') }}
    union all
    select 'customers', sum(total_exposure) from {{ ref('dim_customers') }}
    union all
    select 'institutions', sum(total_exposure) from {{ ref('dim_institutions') }}
)
select * from totals
where exposure <> (select sum(total_exposure) from {{ ref('dim_credit_relationships') }})