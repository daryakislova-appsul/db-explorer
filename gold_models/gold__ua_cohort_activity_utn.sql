{{ config(
    materialized = "table",
    alias = "ua_cohort_activity_utn",
    engine = "MergeTree()",
    order_by = [
        "cohort_date",
        "platform",
        "application",
        "country",
        "media_source",
        "promo_campaign",
        "promo_campaign_id",
        "adset",
        "ad"
    ],
    partition_by = "toYYYYMM(cohort_date)",
    settings = {"allow_nullable_key": 1}
) }}

with cohort_users as (
    select *
    from {{ ref('gold__ua_user_lifetime_activity_daily') }}
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
    toUInt64(uniqExactIf(u.user_lifetime_id, u.sessions_count > 0)) as clients_sdk_utn,
    now() as version
from cohort_users as u
group by
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
    adset
