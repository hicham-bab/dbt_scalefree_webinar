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
