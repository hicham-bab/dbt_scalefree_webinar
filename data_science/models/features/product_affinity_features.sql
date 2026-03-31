-- Product affinity / co-purchase features.
-- Identifies the most frequently co-purchased product for each product.
-- Grain: one row per product.
with order_items as (
    select
        order_id,
        product_id
    from {{ ref('platform', 'fct_order_items') }}
    where order_status not in ('cancelled', 'pending')
),

products as (
    select
        product_id,
        product_name,
        category,
        current_price,
        performance_tier
    from {{ ref('platform', 'dim_products') }}
),

-- Self-join to find co-purchased products
co_purchase as (
    select
        a.product_id,
        b.product_id                                               as co_product_id,
        count(distinct a.order_id)                                 as co_purchase_count
    from order_items a
    join order_items b
        on a.order_id = b.order_id
        and a.product_id != b.product_id
    group by a.product_id, b.product_id
),

-- Rank to find most frequent co-purchase partner
co_purchase_ranked as (
    select
        product_id,
        co_product_id,
        co_purchase_count,
        row_number() over (
            partition by product_id order by co_purchase_count desc
        )                                                          as rn
    from co_purchase
),

-- Total basket size per product
basket_stats as (
    select
        a.product_id,
        avg(basket_size.items_in_order)                            as avg_basket_size_with_product
    from order_items a
    join (
        select order_id, count(distinct product_id) as items_in_order
        from order_items
        group by order_id
    ) basket_size on a.order_id = basket_size.order_id
    group by a.product_id
),

final as (
    select
        p.product_id,
        p.product_name,
        p.category,
        p.current_price,
        p.performance_tier,
        cp.co_product_id                                           as cross_sell_product_id,
        cp2.product_name                                           as cross_sell_product_name,
        cp2.category                                               as cross_sell_category,
        coalesce(cp.co_purchase_count, 0)                          as affinity_score,
        coalesce(bs.avg_basket_size_with_product, 1)              as avg_basket_size_with_product
    from products p
    left join co_purchase_ranked cp
        on p.product_id = cp.product_id and cp.rn = 1
    left join products cp2
        on cp.co_product_id = cp2.product_id
    left join basket_stats bs on p.product_id = bs.product_id
)

select * from final
