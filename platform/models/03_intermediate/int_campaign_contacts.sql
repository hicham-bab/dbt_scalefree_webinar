-- Current version of each term-deposit campaign contact, sentinels turned into NULLs.

with current_s as (
    select *
    from {{ ref('contact_bank_marketing_n_s_v1') }}
    where is_current = true
)

select
    cast(h.contact_id as integer)                           as contact_id,
    cast(s.age as integer)                                  as age,
    nullif(s.job, 'unknown')                                as job,
    s.marital                                               as marital_status,
    nullif(s.education, 'unknown')                          as education,
    s.c_default = 'yes'                                     as has_credit_in_default,
    cast(s.balance as number(12, 2))                        as average_yearly_balance_eur,
    s.housing = 'yes'                                       as has_housing_loan,
    s.loan = 'yes'                                          as has_personal_loan,
    nullif(s.contact, 'unknown')                            as contact_channel,
    -- Day and month of the last contact. There is NO year column in the source.
    cast(s."DAY" as integer)                                as last_contact_day_of_month,
    s."MONTH"                                               as last_contact_month,
    -- Known only after the call ends. Never use it to predict "subscribed".
    cast(s.duration as integer)                             as call_duration_seconds,
    cast(s.campaign as integer)                             as contacts_this_campaign,
    -- -1 means "never contacted in a previous campaign"
    nullif(cast(s.pdays as integer), -1)                    as days_since_previous_campaign_contact,
    cast(s.previous as integer)                             as contacts_before_this_campaign,
    nullif(s.poutcome, 'unknown')                           as previous_campaign_outcome,
    s.y = 'yes'                                             as subscribed,
    s.ldts                                                  as synced_at

from {{ ref('contact_h') }} h
inner join current_s s on h.hk_contact_h = s.hk_contact_h
where h.hk_contact_h not in ({{ unknown_key() }}, {{ error_key() }})
  and not s.is_deleted