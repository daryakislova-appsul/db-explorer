{{ config(
    materialized = "table",
    alias = "ua_cohort_users",
    engine = "MergeTree()",
    order_by = ["cohort_date", "application", "client_id", "user_lifetime_id"],
    partition_by = "toYYYYMM(cohort_date)",
    settings = {"allow_nullable_key": 1}
) }}

{% set cohort_start_date = var('gold_ua_cohort_start_date', '2024-02-08') %}

select
    toDate(install_date) as cohort_date,
    toStartOfMonth(toDate(install_date)) as cohort_month,
    toStartOfWeek(toDate(install_date), 5) as cohort_week,
    multiIf(
        lowerUTF8(ifNull(platform, '')) like '%ios%', 'ios',
        lowerUTF8(ifNull(platform, '')) like '%and%', 'android',
        'unknown'
    ) as platform,
    coalesce(nullIf(app_name, ''), 'unknown') as application,
    coalesce(country_code, '') as country,
    coalesce(media_source, '') as media_source,
    coalesce(promo_campaign, '') as promo_campaign,
    coalesce(promo_campaign_id, '') as promo_campaign_id,
    coalesce(ad, '') as ad,
    coalesce(adset, '') as adset,
    coalesce(install_device_name, '') as install_device_name,
    coalesce(install_device_type, '') as install_device_type,
    client_id,
    user_lifetime_id,
    user_lifetime_number,
    toDate(install_time) as lifetime_start_date,
    if(
        isNull(lifetime_end_time) = 1,
        cast(null as Nullable(Date)),
        toDate(lifetime_end_time)
    ) as lifetime_end_date
from {{ ref('silver__users_scd2') }}
where toDate(install_date) >= toDate('{{ cohort_start_date }}')
  and isNull(client_id) = 0
  and client_id != ''
  and client_id != '00000000-0000-0000-0000-000000000000'
  and nullIf(app_name, '') is not null
