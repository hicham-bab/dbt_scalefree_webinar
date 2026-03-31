with source as (
    select * from {{ source('ecommerce_raw', 'raw_customers') }}
),

renamed as (
    select
        cast(customer_id as varchar(36))                    as customer_id,
        cast(first_name as varchar(100))                    as first_name,
        cast(last_name as varchar(100))                     as last_name,
        trim(lower(cast(email as varchar(255))))            as email,
        cast(phone as varchar(50))                         as phone,
        cast(birth_date as date)                           as birth_date,
        cast(gender as varchar(10))                        as gender,
        cast(city as varchar(100))                         as city,
        cast(country as varchar(100))                      as country,
        cast(signup_date as date)                          as signup_date,
        cast(loyalty_tier as varchar(20))                  as loyalty_tier,
        cast(acquisition_channel as varchar(50))           as acquisition_channel,
        cast(_loaded_at as timestamp_ntz)                  as loaded_at
    from source
)

select * from renamed
