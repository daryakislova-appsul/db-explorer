{{ config(
    materialized = "table",
    alias = "ua_cohort_long",
    engine = "MergeTree()",
    order_by = [
        "event_date",
        "cohort_date",
        "platform",
        "application",
        "country",
        "media_source",
        "promo_campaign",
        "promo_campaign_id",
        "adset",
        "ad",
        "day_idx"
    ],
    partition_by = "toYYYYMM(event_date)",
    settings = {"allow_nullable_key": 1}
) }}

{% set cohort_start_date = var('gold_ua_cohort_start_date', '2024-02-08') %}
{% set cohort_end_date = var('gold_ua_cohort_end_date', var('end_date', none)) %}

with cohort_users as (
    select *
    from {{ ref('gold__ua_cohort_users') }}
    where cohort_date >= toDate('{{ cohort_start_date }}')
      {% if cohort_end_date is not none %}
      and cohort_date <= toDate('{{ cohort_end_date }}')
      {% endif %}
),

installs_day0 as (
    select
        u.cohort_date as cohort_date,
        u.cohort_month as cohort_month,
        u.cohort_week as cohort_week,
        u.cohort_date as event_date,
        toUInt16(0) as day_idx,
        u.platform as platform,
        u.application as application,
        u.country as country,
        u.media_source as media_source,
        u.promo_campaign as promo_campaign,
        u.promo_campaign_id as promo_campaign_id,
        u.ad as ad,
        u.adset as adset,
        toUInt64(count()) as installs,
        toUInt64(0) as impressions,
        toUInt64(0) as clicks,
        toDecimal64(0, 8) as cost,
        toUInt64(0) as clients_sdk,
        toUInt64(0) as purchases,
        toUInt64(0) as refunds,
        toUInt64(0) as inapp_purchases,
        toUInt64(0) as inapp_refunds_num,
        toUInt64(0) as subscription_purchases,
        toUInt64(0) as subscription_refunds_num,
        toUInt64(0) as trial_purchases,
        toUInt64(0) as trial_converted_purchases,
        toUInt64(0) as renewal_purchases,
        toUInt64(0) as cancelations,
        toDecimal64(0, 8) as inapp_revenue_gross,
        toDecimal64(0, 8) as subs_revenue_gross,
        toDecimal64(0, 8) as inapp_refunds_revenue_net,
        toDecimal64(0, 8) as subs_refunds_revenue_net,
        toDecimal64(0, 8) as inapp_revenue_net,
        toDecimal64(0, 8) as subs_revenue_net,
        toUInt64(0) as ad_impressions,
        toUInt64(0) as ad_impressions_rewarded,
        toUInt64(0) as ad_impressions_interstitial,
        toUInt64(0) as ad_impressions_banner,
        toDecimal64(0, 8) as ad_revenue_gross,
        toDecimal64(0, 8) as ad_revenue_rewarded_gross,
        toDecimal64(0, 8) as ad_revenue_interstitial_gross,
        toDecimal64(0, 8) as ad_revenue_banner_gross
    from cohort_users as u
    group by
        cohort_date,
        cohort_month,
        cohort_week,
        event_date,
        day_idx,
        platform,
        application,
        country,
        media_source,
        promo_campaign,
        promo_campaign_id,
        ad,
        adset
),

costs_day0 as (
    select
        toDate(c.day) as cohort_date,
        toStartOfMonth(toDate(c.day)) as cohort_month,
        toStartOfWeek(toDate(c.day), 5) as cohort_week,
        toDate(c.day) as event_date,
        toUInt16(0) as day_idx,
        multiIf(
            lowerUTF8(ifNull(c.platform, '')) like '%ios%', 'ios',
            lowerUTF8(ifNull(c.platform, '')) like '%and%', 'android',
            'unknown'
        ) as platform,
        coalesce(nullIf(c.application, ''), 'unknown') as application,
        coalesce(c.country, '') as country,
        coalesce(c.media_source, '') as media_source,
        coalesce(c.promo_campaign, '') as promo_campaign,
        coalesce(c.promo_campaign_id, '') as promo_campaign_id,
        coalesce(c.ad, '') as ad,
        coalesce(c.adset, '') as adset,
        toUInt64(0) as installs,
        toUInt64(sum(c.impressions)) as impressions,
        toUInt64(sum(c.clicks)) as clicks,
        toDecimal64(sum(toFloat64(c.cost)), 8) as cost,
        toUInt64(0) as clients_sdk,
        toUInt64(0) as purchases,
        toUInt64(0) as refunds,
        toUInt64(0) as inapp_purchases,
        toUInt64(0) as inapp_refunds_num,
        toUInt64(0) as subscription_purchases,
        toUInt64(0) as subscription_refunds_num,
        toUInt64(0) as trial_purchases,
        toUInt64(0) as trial_converted_purchases,
        toUInt64(0) as renewal_purchases,
        toUInt64(0) as cancelations,
        toDecimal64(0, 8) as inapp_revenue_gross,
        toDecimal64(0, 8) as subs_revenue_gross,
        toDecimal64(0, 8) as inapp_refunds_revenue_net,
        toDecimal64(0, 8) as subs_refunds_revenue_net,
        toDecimal64(0, 8) as inapp_revenue_net,
        toDecimal64(0, 8) as subs_revenue_net,
        toUInt64(0) as ad_impressions,
        toUInt64(0) as ad_impressions_rewarded,
        toUInt64(0) as ad_impressions_interstitial,
        toUInt64(0) as ad_impressions_banner,
        toDecimal64(0, 8) as ad_revenue_gross,
        toDecimal64(0, 8) as ad_revenue_rewarded_gross,
        toDecimal64(0, 8) as ad_revenue_interstitial_gross,
        toDecimal64(0, 8) as ad_revenue_banner_gross
    from {{ ref('silver__ua_costs') }} as c
    where toDate(c.day) >= toDate('{{ cohort_start_date }}')
    {% if cohort_end_date is not none %}
      and toDate(c.day) <= toDate('{{ cohort_end_date }}')
    {% endif %}
    group by
        cohort_date,
        cohort_month,
        cohort_week,
        event_date,
        day_idx,
        platform,
        application,
        country,
        media_source,
        promo_campaign,
        promo_campaign_id,
        ad,
        adset
),

user_lifetime_daily as (
    select *
    from {{ ref('gold__ua_user_lifetime_activity_daily') }}
    where cohort_date >= toDate('{{ cohort_start_date }}')
      and event_date >= toDate('{{ cohort_start_date }}')
      {% if cohort_end_date is not none %}
      and cohort_date <= toDate('{{ cohort_end_date }}')
      {% endif %}
),

activity_and_monetization as (
    select
        d.cohort_date as cohort_date,
        d.cohort_month as cohort_month,
        d.cohort_week as cohort_week,
        d.event_date as event_date,
        d.day_idx as day_idx,
        d.platform as platform,
        d.application as application,
        d.country as country,
        d.media_source as media_source,
        d.promo_campaign as promo_campaign,
        d.promo_campaign_id as promo_campaign_id,
        d.ad as ad,
        d.adset as adset,
        toUInt64(0) as installs,
        toUInt64(0) as impressions,
        toUInt64(0) as clicks,
        toDecimal64(0, 8) as cost,
        toUInt64(countIf(d.sessions_count > 0)) as clients_sdk,
        toUInt64(sum(d.purchases)) as purchases,
        toUInt64(sum(d.refunds)) as refunds,
        toUInt64(sum(d.inapp_purchases)) as inapp_purchases,
        toUInt64(sum(d.inapp_refunds_num)) as inapp_refunds_num,
        toUInt64(sum(d.subscription_purchases)) as subscription_purchases,
        toUInt64(sum(d.subscription_refunds_num)) as subscription_refunds_num,
        toUInt64(sum(d.trial_purchases)) as trial_purchases,
        toUInt64(sum(d.trial_converted_purchases)) as trial_converted_purchases,
        toUInt64(sum(d.renewal_purchases)) as renewal_purchases,
        toUInt64(sum(d.cancelations)) as cancelations,
        toDecimal64(sum(toFloat64(d.inapp_revenue_gross)), 8) as inapp_revenue_gross,
        toDecimal64(sum(toFloat64(d.subs_revenue_gross)), 8) as subs_revenue_gross,
        toDecimal64(sum(toFloat64(d.inapp_refunds_revenue_net)), 8) as inapp_refunds_revenue_net,
        toDecimal64(sum(toFloat64(d.subs_refunds_revenue_net)), 8) as subs_refunds_revenue_net,
        toDecimal64(sum(toFloat64(d.inapp_revenue_net)), 8) as inapp_revenue_net,
        toDecimal64(sum(toFloat64(d.subs_revenue_net)), 8) as subs_revenue_net,
        toUInt64(sum(d.ad_impressions)) as ad_impressions,
        toUInt64(sum(d.ad_impressions_rewarded)) as ad_impressions_rewarded,
        toUInt64(sum(d.ad_impressions_interstitial)) as ad_impressions_interstitial,
        toUInt64(sum(d.ad_impressions_banner)) as ad_impressions_banner,
        toDecimal64(sum(toFloat64(d.ad_revenue_gross)), 8) as ad_revenue_gross,
        toDecimal64(sum(toFloat64(d.ad_revenue_rewarded_gross)), 8) as ad_revenue_rewarded_gross,
        toDecimal64(sum(toFloat64(d.ad_revenue_interstitial_gross)), 8) as ad_revenue_interstitial_gross,
        toDecimal64(sum(toFloat64(d.ad_revenue_banner_gross)), 8) as ad_revenue_banner_gross
    from user_lifetime_daily as d
    group by
        cohort_date,
        cohort_month,
        cohort_week,
        event_date,
        day_idx,
        platform,
        application,
        country,
        media_source,
        promo_campaign,
        promo_campaign_id,
        ad,
        adset
),

unioned as (
    select * from installs_day0
    union all
    select * from costs_day0
    union all
    select * from activity_and_monetization
),

aggregated as (
    select
        cohort_date,
        cohort_month,
        cohort_week,
        event_date,
        day_idx,
        platform,
        application,
        country,
        media_source,
        promo_campaign,
        promo_campaign_id,
        ad,
        adset,
        toUInt64(sum(installs)) as installs,
        toUInt64(sum(impressions)) as impressions,
        toUInt64(sum(clicks)) as clicks,
        toDecimal64(sum(toFloat64(cost)), 8) as cost,
        toUInt64(sum(clients_sdk)) as clients_sdk,
        toUInt64(sum(purchases)) as purchases,
        toUInt64(sum(refunds)) as refunds,
        toUInt64(sum(inapp_purchases)) as inapp_purchases,
        toUInt64(sum(inapp_refunds_num)) as inapp_refunds_num,
        toUInt64(sum(subscription_purchases)) as subscription_purchases,
        toUInt64(sum(subscription_refunds_num)) as subscription_refunds_num,
        toUInt64(sum(trial_purchases)) as trial_purchases,
        toUInt64(sum(trial_converted_purchases)) as trial_converted_purchases,
        toUInt64(sum(renewal_purchases)) as renewal_purchases,
        toUInt64(sum(cancelations)) as cancelations,
        toDecimal64(sum(toFloat64(inapp_revenue_gross)), 8) as inapp_revenue_gross,
        toDecimal64(sum(toFloat64(subs_revenue_gross)), 8) as subs_revenue_gross,
        toDecimal64(sum(toFloat64(inapp_refunds_revenue_net)), 8) as inapp_refunds_revenue_net,
        toDecimal64(sum(toFloat64(subs_refunds_revenue_net)), 8) as subs_refunds_revenue_net,
        toDecimal64(sum(toFloat64(inapp_revenue_net)), 8) as inapp_revenue_net,
        toDecimal64(sum(toFloat64(subs_revenue_net)), 8) as subs_revenue_net,
        toUInt64(sum(ad_impressions)) as ad_impressions,
        toUInt64(sum(ad_impressions_rewarded)) as ad_impressions_rewarded,
        toUInt64(sum(ad_impressions_interstitial)) as ad_impressions_interstitial,
        toUInt64(sum(ad_impressions_banner)) as ad_impressions_banner,
        toDecimal64(sum(toFloat64(ad_revenue_gross)), 8) as ad_revenue_gross,
        toDecimal64(sum(toFloat64(ad_revenue_rewarded_gross)), 8) as ad_revenue_rewarded_gross,
        toDecimal64(sum(toFloat64(ad_revenue_interstitial_gross)), 8) as ad_revenue_interstitial_gross,
        toDecimal64(sum(toFloat64(ad_revenue_banner_gross)), 8) as ad_revenue_banner_gross
    from unioned
    group by
        cohort_date,
        cohort_month,
        cohort_week,
        event_date,
        day_idx,
        platform,
        application,
        country,
        media_source,
        promo_campaign,
        promo_campaign_id,
        ad,
        adset
)

select
    cohort_date,
    cohort_month,
    cohort_week,
    event_date,
    day_idx,
    platform,
    application,
    country,
    media_source,
    promo_campaign,
    promo_campaign_id,
    ad,
    adset,
    installs,
    impressions,
    clicks,
    cost,
    clients_sdk,
    purchases,
    refunds,
    inapp_purchases,
    inapp_refunds_num,
    subscription_purchases,
    subscription_refunds_num,
    trial_purchases,
    trial_converted_purchases,
    renewal_purchases,
    cancelations,
    inapp_revenue_gross,
    subs_revenue_gross,
    inapp_refunds_revenue_net,
    subs_refunds_revenue_net,
    inapp_revenue_net,
    subs_revenue_net,
    ad_impressions,
    ad_impressions_rewarded,
    ad_impressions_interstitial,
    ad_impressions_banner,
    ad_revenue_gross,
    ad_revenue_rewarded_gross,
    ad_revenue_interstitial_gross,
    ad_revenue_banner_gross,
    toDecimal64(
        toFloat64(inapp_revenue_gross)
        + toFloat64(subs_revenue_gross)
        + toFloat64(ad_revenue_gross),
        8
    ) as total_revenue_gross,
    toDecimal64(
        toFloat64(inapp_revenue_net)
        + toFloat64(subs_revenue_net)
        + toFloat64(ad_revenue_gross),
        8
    ) as total_revenue_net,
    now() as version
from aggregated
having day_idx >= 0
