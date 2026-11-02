-- One row per client contacted in the term-deposit telemarketing campaign.
-- No year is recorded, so there is no time axis: compare segments, not periods.

with contacts as (

    select * from {{ ref('int_campaign_contacts') }}

)

select
    contact_id,
    age,
    case
        when age < 25 then '18-24'
        when age < 35 then '25-34'
        when age < 45 then '35-44'
        when age < 55 then '45-54'
        when age < 65 then '55-64'
        else '65+'
    end                                                     as age_band,
    job,
    marital_status,
    education,
    has_credit_in_default,
    average_yearly_balance_eur,
    has_housing_loan,
    has_personal_loan,
    contact_channel,
    last_contact_month,
    last_contact_day_of_month,
    contacts_this_campaign,
    contacts_before_this_campaign,
    days_since_previous_campaign_contact,
    contacts_before_this_campaign > 0                       as was_previously_contacted,
    previous_campaign_outcome,
    call_duration_seconds,
    subscribed

from contacts