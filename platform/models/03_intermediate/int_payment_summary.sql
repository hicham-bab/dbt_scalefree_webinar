{# Summarizes payment information at order level.
   Grain: one row per order (most recent / primary payment record). #}

with current_payment_sat as (
    select *
    from {{ ref('payment_pgw_n_s_v1') }}
    where is_current = true
),

payments as (
    select
        p.payment_id,
        o.order_id,
        s.payment_date,
        s.payment_method,
        s.gateway,
        s.currency,
        s.amount,
        round(
            s.amount *
            case upper(s.currency)
                when 'EUR' then 1.0
                when 'GBP' then 1.17
                when 'USD' then 0.92
                when 'CHF' then 1.03
                else 1.0
            end,
            2
        )                               as amount_eur,
        s.payment_status,
        p.ldts                          as loaded_at
    from {{ ref('payment_h') }} p
    join {{ ref('payment_order_nl') }} l   on p.hk_payment_h  = l.hk_payment_h
    join {{ ref('order_h') }} o            on l.hk_order_h    = o.hk_order_h
    join current_payment_sat s             on p.hk_payment_h  = s.hk_payment_h
    where p.hk_payment_h not in ({{ unknown_key() }}, {{ error_key() }})
),

current_order_sat as (
    select *
    from {{ ref('order_oms_n_s_v1') }}
    where is_current = true
),

orders as (
    select o.order_id, s.order_date
    from {{ ref('order_h') }} o
    join current_order_sat s on o.hk_order_h = s.hk_order_h
    where o.hk_order_h not in ({{ unknown_key() }}, {{ error_key() }})
),

-- Aggregate payments to order level (an order may have retried payments)
payment_agg as (
    select
        p.order_id,
        sum(case when p.payment_status = 'completed' then p.amount_eur else 0 end) as total_paid_eur,
        max(p.payment_date)                                                          as latest_payment_date,
        max(case when p.payment_status = 'completed' then p.payment_method end)     as payment_method,
        max(case when p.payment_status = 'completed' then p.gateway end)            as gateway,
        max(p.payment_status)                                                        as payment_status,
        count(case when p.payment_status = 'refunded' then 1 end)                   as refund_count,
        sum(case when p.payment_status = 'refunded' then p.amount_eur else 0 end)   as total_refunded_eur
    from payments p
    group by p.order_id
),

final as (
    select
        pa.order_id,
        pa.total_paid_eur,
        pa.payment_method,
        pa.gateway,
        pa.payment_status,
        pa.refund_count,
        pa.total_refunded_eur,
        case when pa.refund_count > 0 then true else false end                       as is_refunded,
        datediff('day', o.order_date, pa.latest_payment_date)                       as days_to_payment
    from payment_agg pa
    left join orders o on pa.order_id = o.order_id
)

select * from final
