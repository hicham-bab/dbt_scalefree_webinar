-- Multi-touch channel attribution comparison.
-- Compares first-touch, last-touch, and linear revenue across channels.
-- Grain: one row per channel.
with orders as (
    select
        order_id,
        customer_id,
        channel,
        net_revenue_eur,
        is_first_order,
        order_date
    from {{ ref('platform', 'fct_orders') }}
    where order_status = 'delivered'
),

-- Last-touch: credit the channel where the order was placed
last_touch as (
    select
        channel,
        sum(net_revenue_eur)                                       as last_touch_revenue,
        count(order_id)                                            as last_touch_orders
    from orders
    group by channel
),

-- First-touch: credit the acquisition channel for new customers
first_touch as (
    select
        c.acquisition_channel                                      as channel,
        sum(o.net_revenue_eur)                                     as first_touch_revenue,
        count(o.order_id)                                          as first_touch_orders
    from orders o
    join {{ ref('platform', 'dim_customers') }} c on o.customer_id = c.customer_id
    where o.is_first_order = true
    group by c.acquisition_channel
),

-- Linear: divide revenue equally between acquisition and order channels
-- Simplification: split 50/50 between acquisition channel and order channel
linear_agg as (
    select
        channel_name,
        sum(attributed_rev)                                        as linear_revenue
    from (
        select
            o.channel                                              as channel_name,
            sum(o.net_revenue_eur) * 0.5                          as attributed_rev
        from orders o
        group by o.channel

        union all

        select
            c.acquisition_channel                                  as channel_name,
            sum(o.net_revenue_eur) * 0.5                          as attributed_rev
        from orders o
        join {{ ref('platform', 'dim_customers') }} c on o.customer_id = c.customer_id
        group by c.acquisition_channel
    ) t
    group by channel_name
),

-- Total for share calculation
total_revenue as (
    select sum(net_revenue_eur) as grand_total from orders
),

final as (
    select
        lt.channel,
        coalesce(lt.last_touch_revenue, 0)                        as last_touch_revenue_eur,
        coalesce(lt.last_touch_orders, 0)                         as last_touch_orders,
        coalesce(ft.first_touch_revenue, 0)                       as first_touch_revenue_eur,
        coalesce(ft.first_touch_orders, 0)                        as first_touch_orders,
        coalesce(la.linear_revenue, 0)                            as linear_revenue_eur,
        round(
            coalesce(lt.last_touch_revenue, 0) / nullif(tr.grand_total, 0) * 100,
            2
        )                                                          as last_touch_pct_of_total,
        current_date()                                             as calculated_at
    from last_touch lt
    left join first_touch ft on lt.channel = ft.channel
    left join linear_agg la on lt.channel = la.channel_name
    cross join total_revenue tr
)

select * from final
order by last_touch_revenue_eur desc
