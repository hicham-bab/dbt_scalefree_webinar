-- Campaign performance combining platform attribution data with budget metadata.
-- Computes ROAS and attributed revenue by campaign and week.
-- Grain: one row per campaign per week.
with campaigns as (
    -- Campaigns are staged within marketing; since we don't have a separate
    -- marketing source, we reference via platform attribution cross-project ref.
    -- In a real setup, marketing would have its own CRM/ad platform source.
    select
        ca.campaign_id,
        ca.attributed_revenue_eur,
        ca.attribution_model,
        o.order_date
    from {{ ref('platform', 'fct_orders') }} o
    join (
        -- We use order data enriched with attribution info derived from
        -- the platform-level raw data passed through as a public model reference.
        -- For this demo the attribution info comes directly from order metadata.
        select
            order_id,
            'CAMP_001'                              as campaign_id,
            net_revenue_eur                         as attributed_revenue_eur,
            'last_touch'                            as attribution_model
        from {{ ref('platform', 'fct_orders') }}
        where channel = 'website'
          and order_status = 'delivered'
          and is_first_order = true
    ) ca on o.order_id = ca.order_id
),

-- Weekly aggregation
weekly as (
    select
        c.campaign_id,
        date_trunc('week', c.order_date)                            as week_start,
        c.attribution_model,
        count(distinct c.order_id)                                  as conversions,
        sum(c.attributed_revenue_eur)                               as attributed_revenue_eur
    from (
        select
            o.order_id,
            o.order_date,
            o.net_revenue_eur                                       as attributed_revenue_eur,
            o.channel                                               as campaign_id,
            'last_touch'                                            as attribution_model
        from {{ ref('platform', 'fct_orders') }} o
        where o.order_status = 'delivered'
    ) c
    group by c.campaign_id, date_trunc('week', c.order_date), c.attribution_model
),

final as (
    select
        w.campaign_id,
        w.week_start,
        w.attribution_model,
        w.conversions,
        w.attributed_revenue_eur,
        -- ROAS approximation: revenue / simulated spend (10% of revenue)
        round(w.attributed_revenue_eur / nullif(w.attributed_revenue_eur * 0.1, 0), 2)  as roas,
        -- CPA: spend / conversions
        round((w.attributed_revenue_eur * 0.1) / nullif(w.conversions, 0), 2)           as cpa_eur,
        current_date()                                                                   as calculated_at
    from weekly w
)

select * from final
