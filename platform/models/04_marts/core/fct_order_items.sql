with order_items as (
    select
        oi.order_item_id,
        o.order_id,
        p.product_id,
        s.quantity,
        s.unit_price_at_order,
        coalesce(s.discount_pct, 0)                                             as discount_pct,
        round(
            s.quantity
            * s.unit_price_at_order
            * (1 - coalesce(s.discount_pct, 0) / 100),
            2
        )                                                                       as line_revenue
    from {{ ref('order_item_order_product_nl') }} nl
    join {{ ref('order_item_order_product_nl_s_v1') }} s
        on nl.hk_order_item_order_product_nl = s.hk_order_item_order_product_nl
        and s.is_current = true
    join {{ ref('order_item_h') }} oi  on nl.hk_order_item_h = oi.hk_order_item_h
    join {{ ref('order_h') }} o        on nl.hk_order_h      = o.hk_order_h
    join {{ ref('product_h') }} p      on nl.hk_product_h    = p.hk_product_h
    where oi.hk_order_item_h not in ({{ unknown_key() }}, {{ error_key() }})
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
        s.order_date,
        s.order_status
    from {{ ref('order_h') }} o
    join {{ ref('order_customer_l') }} l   on o.hk_order_h   = l.hk_order_h
    join {{ ref('customer_h') }} c         on l.hk_customer_h = c.hk_customer_h
    join current_order_sat s               on o.hk_order_h   = s.hk_order_h
    where o.hk_order_h not in ({{ unknown_key() }}, {{ error_key() }})
),

returns as (
    select oi.order_item_id
    from {{ ref('return_h') }} r
    join {{ ref('return_order_order_item_l') }} rl on r.hk_return_h      = rl.hk_return_h
    left join {{ ref('order_item_h') }} oi         on rl.hk_order_item_h = oi.hk_order_item_h
    join (
        select hk_return_h, refund_status
        from {{ ref('return_oms_n_s_v1') }}
        where is_current = true
    ) rs on r.hk_return_h = rs.hk_return_h
    where rs.refund_status != 'rejected'
    group by oi.order_item_id
),

final as (
    select
        oi.order_item_id,
        oi.order_id,
        oi.product_id,
        o.customer_id,
        o.order_date,
        o.order_status,
        oi.quantity,
        oi.unit_price_at_order                                                  as unit_price_eur,
        oi.discount_pct,
        oi.line_revenue                                                         as line_revenue_eur,
        case when r.order_item_id is not null then true else false end           as has_been_returned
    from order_items oi
    join orders o on oi.order_id = o.order_id
    left join returns r on oi.order_item_id = r.order_item_id
)

select * from final
