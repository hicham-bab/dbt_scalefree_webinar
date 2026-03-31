-- Time-series order sequence features per customer.
-- Captures inter-order timing, AOV trend, and channel consistency.
-- Grain: one row per customer order (sequence number).
with orders as (
    select
        customer_id,
        order_id,
        order_date,
        net_revenue_eur,
        channel
    from {{ ref('platform', 'fct_orders') }}
    where order_status not in ('cancelled', 'pending')
),

-- Assign sequence numbers per customer
sequenced as (
    select
        customer_id,
        order_id,
        order_date,
        net_revenue_eur,
        channel,
        row_number() over (
            partition by customer_id order by order_date asc
        )                                                          as order_number,
        lag(order_date) over (
            partition by customer_id order by order_date asc
        )                                                          as prev_order_date,
        lag(net_revenue_eur) over (
            partition by customer_id order by order_date asc
        )                                                          as prev_order_value,
        lag(channel) over (
            partition by customer_id order by order_date asc
        )                                                          as prev_channel
    from orders
),

-- Channel consistency flag
channel_usage as (
    select
        customer_id,
        count(distinct channel)                                    as distinct_channels
    from orders
    group by customer_id
),

final as (
    select
        s.customer_id,
        s.order_id,
        s.order_number,
        s.order_date,
        s.net_revenue_eur,
        s.channel,
        -- Days between consecutive orders
        case
            when s.prev_order_date is not null
            then datediff('day', s.prev_order_date, s.order_date)
            else null
        end                                                        as days_between_orders,
        -- AOV trend vs previous order
        case
            when s.prev_order_value is null    then 'first_order'
            when s.net_revenue_eur > s.prev_order_value * 1.1 then 'growing'
            when s.net_revenue_eur < s.prev_order_value * 0.9 then 'declining'
            else 'stable'
        end                                                        as aov_trend,
        -- Channel consistency
        case
            when cu.distinct_channels = 1 then 'consistent'
            when cu.distinct_channels = 2 then 'mixed'
            else 'omnichannel'
        end                                                        as channel_consistency,
        cu.distinct_channels                                       as total_distinct_channels
    from sequenced s
    left join channel_usage cu on s.customer_id = cu.customer_id
)

select * from final
