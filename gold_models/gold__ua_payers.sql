{% set cohort_start_date = var('gold_ua_cohort_start_date', '2024-02-08') %}
{% set cohort_end_date = var('gold_ua_cohort_end_date', var('end_date', none)) %}

{{ config(
    materialized = "table",
    alias = "ua_payers",
    engine = "MergeTree()",
    order_by = ["application", "client_id", "user_lifetime_id"],
    partition_by = "toYYYYMM(cohort_date)",
    settings = {"allow_nullable_key": 1}
) }}

with users_base as (
    select
        cohort_date,
        cohort_month,
        cohort_week,
        platform,
        application,
        country,
        media_source,
        promo_campaign,
        promo_campaign_id,
        ad,
        adset,
        client_id,
        user_lifetime_id,
        lifetime_start_date,
        lifetime_end_date
    from {{ ref('gold__ua_cohort_users') }}
    where cohort_date >= toDate('{{ cohort_start_date }}')
      {% if cohort_end_date is not none %}
      and cohort_date <= toDate('{{ cohort_end_date }}')
      {% endif %}
),

inapp_purchases as (
    select
        app_name,
        client_id,
        toDate(event_time) as event_date,
        event_time as purchase_time,
        'inapp' as purchase_type,
        local_price as local_amount,
        currency,
        price as amount_usd
    from {{ ref('silver__inapp_table') }}
    where event_name = 'purchase'
      and toDate(event_time) >= toDate('{{ cohort_start_date }}')
      and isNull(client_id) = 0
      and client_id != ''
),

subscription_purchases as (
    select
        app_name,
        client_id,
        toDate(event_time) as event_date,
        event_time as purchase_time,
        'subscription' as purchase_type,
        local_price as local_amount,
        currency,
        price as amount_usd
    from {{ ref('silver__subscription_table') }}
    where event_name = 'purchase'
      and toDate(event_time) >= toDate('{{ cohort_start_date }}')
      and isNull(client_id) = 0
      and client_id != ''
),

all_purchases as (
    select * from inapp_purchases
    union all
    select * from subscription_purchases
),

matched_purchases as (
    select
        u.application as app_name,
        u.client_id as client_id,
        u.user_lifetime_id as user_lifetime_id,
        p.event_date as event_date,
        p.purchase_time as purchase_time,
        p.purchase_type as purchase_type,
        p.local_amount as local_amount,
        p.currency as currency,
        p.amount_usd as amount_usd
    from users_base as u
    inner join all_purchases as p
        on u.application = p.app_name
       and u.client_id = p.client_id
       and p.event_date >= u.lifetime_start_date
       and (
            isNull(u.lifetime_end_date) = 1
            or p.event_date < u.lifetime_end_date
       )
),
purchases_for_aggregation as (
    select
        app_name,
        client_id,
        event_date,
        purchase_time,
        purchase_type,
        local_amount,
        currency,
        amount_usd,
        user_lifetime_id
    from matched_purchases
),

users_for_join as (
    select
        cohort_date,
        cohort_month,
        cohort_week,
        platform,
        application,
        country,
        media_source,
        promo_campaign,
        promo_campaign_id,
        ad,
        adset,
        client_id,
        user_lifetime_id
    from users_base
),

aggregated as (
    select
        app_name,
        client_id,
        user_lifetime_id,
        toUInt64(count()) as purchases_total,
        toUInt64(countIf(purchase_type = 'subscription')) as subscription_purchases,
        toUInt64(countIf(purchase_type = 'inapp')) as inapp_purchases,
        if(
            countIf(purchase_type = 'inapp') = 0,
            cast(null as Nullable(String)),
            if(countDistinctIf(currency, purchase_type = 'inapp') = 1, anyIf(currency, purchase_type = 'inapp'), 'MULTI')
        ) as inapp_currency,
        if(
            countIf(purchase_type = 'subscription') = 0,
            cast(null as Nullable(String)),
            if(countDistinctIf(currency, purchase_type = 'subscription') = 1, anyIf(currency, purchase_type = 'subscription'), 'MULTI')
        ) as subscription_currency,
        if(
            countDistinctIf(currency, purchase_type = 'inapp') = 1,
            toDecimal64(sumIf(toFloat64(local_amount), purchase_type = 'inapp'), 8),
            cast(null as Nullable(Decimal(18, 8)))
        ) as inapp_amount_local_total,
        if(
            countDistinctIf(currency, purchase_type = 'subscription') = 1,
            toDecimal64(sumIf(toFloat64(local_amount), purchase_type = 'subscription'), 8),
            cast(null as Nullable(Decimal(18, 8)))
        ) as subscription_amount_local_total,
        toDecimal64(sumIf(toFloat64(amount_usd), purchase_type = 'inapp'), 8) as inapp_amount_usd_total,
        toDecimal64(sumIf(toFloat64(amount_usd), purchase_type = 'subscription'), 8) as subscription_amount_usd_total,
        min(purchase_time) as first_purchase_timestamp,
        toDate(min(purchase_time)) as first_purchase_date,
        if(
            countIf(purchase_type = 'inapp') = 0,
            cast(null as Nullable(Date)),
            minIf(event_date, purchase_type = 'inapp')
        ) as first_inapp_purchase_date,
        if(
            countIf(purchase_type = 'subscription') = 0,
            cast(null as Nullable(Date)),
            minIf(event_date, purchase_type = 'subscription')
        ) as first_subscription_purchase_date
    from purchases_for_aggregation
    group by
        app_name,
        client_id,
        user_lifetime_id
)

select
    u.cohort_date as cohort_date,
    u.cohort_month as cohort_month,
    u.cohort_week as cohort_week,
    u.platform as platform,
    u.application as application,
    u.country as country,
    u.media_source as media_source,
    u.promo_campaign as promo_campaign,
    u.promo_campaign_id as promo_campaign_id,
    u.ad as ad,
    u.adset as adset,
    u.client_id as client_id,
    u.user_lifetime_id as user_lifetime_id,
    toUInt8(a.inapp_purchases > 0) as is_inapp_payer,
    toUInt8(a.subscription_purchases > 0) as is_subscription_payer,
    a.purchases_total as purchases_total,
    a.subscription_purchases as subscription_purchases,
    a.inapp_purchases as inapp_purchases,
    a.inapp_currency as inapp_currency,
    a.subscription_currency as subscription_currency,
    a.inapp_amount_local_total as inapp_amount_local_total,
    a.subscription_amount_local_total as subscription_amount_local_total,
    a.inapp_amount_usd_total as inapp_amount_usd_total,
    a.subscription_amount_usd_total as subscription_amount_usd_total,
    a.first_purchase_timestamp as first_purchase_timestamp,
    a.first_purchase_date as first_purchase_date,
    a.first_inapp_purchase_date as first_inapp_purchase_date,
    a.first_subscription_purchase_date as first_subscription_purchase_date,
    now() as version
from aggregated as a
inner join users_for_join as u
    on a.app_name = u.application
   and a.client_id = u.client_id
   and a.user_lifetime_id = u.user_lifetime_id
