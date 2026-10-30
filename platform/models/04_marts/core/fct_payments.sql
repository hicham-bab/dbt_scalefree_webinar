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
    select
        o.order_id,
        c.customer_id,
        s.order_date
    from {{ ref('order_h') }} o
    join {{ ref('order_customer_l') }} l   on o.hk_order_h   = l.hk_order_h
    join {{ ref('customer_h') }} c         on l.hk_customer_h = c.hk_customer_h
    join current_order_sat s               on o.hk_order_h   = s.hk_order_h
    where o.hk_order_h not in ({{ unknown_key() }}, {{ error_key() }})
),

final as (
    select
        p.payment_id,
        p.order_id,
        o.customer_id,
        p.payment_date,
        o.order_date,
        p.payment_method,
        p.gateway,
        p.currency,
        p.amount                                                                as amount_original,
        p.amount_eur,
        p.payment_status,
        case when p.payment_status = 'refunded' then true else false end        as is_refunded,
        datediff('day', o.order_date, p.payment_date)                          as days_to_payment
    from payments p
    left join orders o on p.order_id = o.order_id
)

select * from final
