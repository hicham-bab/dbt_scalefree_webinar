with source as (
    select * from {{ source('ecommerce_raw', 'raw_products') }}
),

renamed as (
    select
        cast(product_id as varchar(36))                    as product_id,
        cast(product_name as varchar(255))                 as product_name,
        cast(category as varchar(100))                     as category,
        cast(subcategory as varchar(100))                  as subcategory,
        cast(material as varchar(100))                     as material,
        cast(color as varchar(50))                         as color,
        cast(unit_price as number(10,2))                   as unit_price,
        cast(cost_price as number(10,2))                   as cost_price,
        cast(unit_price - cost_price as number(10,2))      as gross_margin,
        case
            when cast(unit_price as number(10,2)) > 0
            then round(
                (cast(unit_price as number(10,2)) - cast(cost_price as number(10,2)))
                / cast(unit_price as number(10,2)) * 100,
                2
            )
            else null
        end                                                 as margin_pct,
        cast(sku_code as varchar(50))                      as sku_code,
        cast(collection as varchar(100))                   as collection,
        cast(is_active as boolean)                         as is_active,
        cast(launched_at as date)                          as launched_at,
        cast(_loaded_at as timestamp_ntz)                  as loaded_at
    from source
)

select * from renamed
