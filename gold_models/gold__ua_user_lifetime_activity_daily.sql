{% set gold_ua_user_lifetime_activity_start_date = var('gold_ua_user_lifetime_activity_start_date', var('start_date', none)) %}
{% set gold_ua_user_lifetime_activity_end_date = var('gold_ua_user_lifetime_activity_end_date', var('end_date', none)) %}
{% set gold_ua_user_lifetime_activity_incremental_lookback_days = var('gold_ua_user_lifetime_activity_incremental_lookback_days', 7) %}
{% set current_utc_date_expr %}toDate(toTimeZone(now(), 'UTC')){% endset %}
{% set use_explicit_window = gold_ua_user_lifetime_activity_start_date is not none or gold_ua_user_lifetime_activity_end_date is not none %}
{% set use_incremental_window = not use_explicit_window and not flags.FULL_REFRESH %}

{% if gold_ua_user_lifetime_activity_start_date is not none and gold_ua_user_lifetime_activity_end_date is not none %}
{% set activity_window_predicate %}
a.activity_date_utc >= toDate('{{ gold_ua_user_lifetime_activity_start_date }}')
and a.activity_date_utc <= toDate('{{ gold_ua_user_lifetime_activity_end_date }}')
and a.activity_date_utc < {{ current_utc_date_expr }}
{% endset %}
{% set inapp_window_predicate %}
toDate(i.event_time) >= toDate('{{ gold_ua_user_lifetime_activity_start_date }}')
and toDate(i.event_time) <= toDate('{{ gold_ua_user_lifetime_activity_end_date }}')
and toDate(i.event_time) < {{ current_utc_date_expr }}
{% endset %}
{% set subscription_window_predicate %}
toDate(s.event_time) >= toDate('{{ gold_ua_user_lifetime_activity_start_date }}')
and toDate(s.event_time) <= toDate('{{ gold_ua_user_lifetime_activity_end_date }}')
and toDate(s.event_time) < {{ current_utc_date_expr }}
{% endset %}
{% set ad_revenue_window_predicate %}
a.event_date >= toDate('{{ gold_ua_user_lifetime_activity_start_date }}')
and a.event_date <= toDate('{{ gold_ua_user_lifetime_activity_end_date }}')
and a.event_date < {{ current_utc_date_expr }}
{% endset %}
{% set delete_window_predicate %}
event_date >= toDate('{{ gold_ua_user_lifetime_activity_start_date }}')
and event_date <= toDate('{{ gold_ua_user_lifetime_activity_end_date }}')
and event_date < {{ current_utc_date_expr }}
{% endset %}
{% set cohort_users_overlap_predicate %}
toDate(install_time) <= toDate('{{ gold_ua_user_lifetime_activity_end_date }}')
and toDate(install_time) < {{ current_utc_date_expr }}
and (
    isNull(lifetime_end_time) = 1
    or toDate(lifetime_end_time) > toDate('{{ gold_ua_user_lifetime_activity_start_date }}')
)
{% endset %}
{% elif gold_ua_user_lifetime_activity_start_date is not none %}
{% set activity_window_predicate %}
a.activity_date_utc >= toDate('{{ gold_ua_user_lifetime_activity_start_date }}')
and a.activity_date_utc < {{ current_utc_date_expr }}
{% endset %}
{% set inapp_window_predicate %}
toDate(i.event_time) >= toDate('{{ gold_ua_user_lifetime_activity_start_date }}')
and toDate(i.event_time) < {{ current_utc_date_expr }}
{% endset %}
{% set subscription_window_predicate %}
toDate(s.event_time) >= toDate('{{ gold_ua_user_lifetime_activity_start_date }}')
and toDate(s.event_time) < {{ current_utc_date_expr }}
{% endset %}
{% set ad_revenue_window_predicate %}
a.event_date >= toDate('{{ gold_ua_user_lifetime_activity_start_date }}')
and a.event_date < {{ current_utc_date_expr }}
{% endset %}
{% set delete_window_predicate %}
event_date >= toDate('{{ gold_ua_user_lifetime_activity_start_date }}')
and event_date < {{ current_utc_date_expr }}
{% endset %}
{% set cohort_users_overlap_predicate %}
toDate(install_time) < {{ current_utc_date_expr }}
and (
    isNull(lifetime_end_time) = 1
    or toDate(lifetime_end_time) > toDate('{{ gold_ua_user_lifetime_activity_start_date }}')
)
{% endset %}
{% elif gold_ua_user_lifetime_activity_end_date is not none %}
{% set activity_window_predicate %}
a.activity_date_utc <= toDate('{{ gold_ua_user_lifetime_activity_end_date }}')
and a.activity_date_utc < {{ current_utc_date_expr }}
{% endset %}
{% set inapp_window_predicate %}
toDate(i.event_time) <= toDate('{{ gold_ua_user_lifetime_activity_end_date }}')
and toDate(i.event_time) < {{ current_utc_date_expr }}
{% endset %}
{% set subscription_window_predicate %}
toDate(s.event_time) <= toDate('{{ gold_ua_user_lifetime_activity_end_date }}')
and toDate(s.event_time) < {{ current_utc_date_expr }}
{% endset %}
{% set ad_revenue_window_predicate %}
a.event_date <= toDate('{{ gold_ua_user_lifetime_activity_end_date }}')
and a.event_date < {{ current_utc_date_expr }}
{% endset %}
{% set delete_window_predicate %}
event_date <= toDate('{{ gold_ua_user_lifetime_activity_end_date }}')
and event_date < {{ current_utc_date_expr }}
{% endset %}
{% set cohort_users_overlap_predicate %}
toDate(install_time) <= toDate('{{ gold_ua_user_lifetime_activity_end_date }}')
and toDate(install_time) < {{ current_utc_date_expr }}
{% endset %}
{% elif use_incremental_window %}
{% set activity_window_predicate %}
a.activity_date_utc >= addDays({{ current_utc_date_expr }}, -{{ gold_ua_user_lifetime_activity_incremental_lookback_days }})
and a.activity_date_utc < {{ current_utc_date_expr }}
{% endset %}
{% set inapp_window_predicate %}
toDate(i.event_time) >= addDays({{ current_utc_date_expr }}, -{{ gold_ua_user_lifetime_activity_incremental_lookback_days }})
and toDate(i.event_time) < {{ current_utc_date_expr }}
{% endset %}
{% set subscription_window_predicate %}
toDate(s.event_time) >= addDays({{ current_utc_date_expr }}, -{{ gold_ua_user_lifetime_activity_incremental_lookback_days }})
and toDate(s.event_time) < {{ current_utc_date_expr }}
{% endset %}
{% set ad_revenue_window_predicate %}
a.event_date >= addDays({{ current_utc_date_expr }}, -{{ gold_ua_user_lifetime_activity_incremental_lookback_days }})
and a.event_date < {{ current_utc_date_expr }}
{% endset %}
{% set delete_window_predicate %}
event_date >= addDays({{ current_utc_date_expr }}, -{{ gold_ua_user_lifetime_activity_incremental_lookback_days }})
and event_date < {{ current_utc_date_expr }}
{% endset %}
{% set cohort_users_overlap_predicate %}
toDate(install_time) < {{ current_utc_date_expr }}
and (
    isNull(lifetime_end_time) = 1
    or toDate(lifetime_end_time) > addDays({{ current_utc_date_expr }}, -{{ gold_ua_user_lifetime_activity_incremental_lookback_days }})
)
{% endset %}
{% else %}
{% set activity_window_predicate %}
a.activity_date_utc < {{ current_utc_date_expr }}
{% endset %}
{% set inapp_window_predicate %}
toDate(i.event_time) < {{ current_utc_date_expr }}
{% endset %}
{% set subscription_window_predicate %}
toDate(s.event_time) < {{ current_utc_date_expr }}
{% endset %}
{% set ad_revenue_window_predicate %}
a.event_date < {{ current_utc_date_expr }}
{% endset %}
{% set delete_window_predicate %}
event_date < {{ current_utc_date_expr }}
{% endset %}
{% set cohort_users_overlap_predicate %}
toDate(install_time) < {{ current_utc_date_expr }}
{% endset %}
{% endif %}

{% if use_explicit_window or use_incremental_window %}
{% set pre_hook_sql %}ALTER TABLE {{ this }} DELETE WHERE {{ delete_window_predicate }} SETTINGS mutations_sync = 1{% endset %}
{% else %}
{% set pre_hook_sql %}SELECT 1{% endset %}
{% endif %}

{{ config(
    materialized = "incremental",
    alias = "ua_user_lifetime_activity_daily",
    engine = "MergeTree()",
    order_by = ["event_date", "application", "client_id", "user_lifetime_id"],
    partition_by = "toYYYYMM(event_date)",
    incremental_strategy = "append",
    pre_hook = pre_hook_sql,
    settings = {"allow_nullable_key": 1}
) }}

{% set cohort_start_date = var('gold_ua_cohort_start_date', '2024-02-08') %}
{% set cohort_end_date = var('gold_ua_cohort_end_date', var('end_date', none)) %}

with activity_source as (
    select
        a.app_name as application,
        a.client_id as client_id,
        a.activity_date_utc as event_date,
        a.last_session_end_utc as event_time,
        toUInt64(a.sessions_count) as sessions_count,
        toUInt64(a.session_duration_seconds_total) as session_duration_seconds_total,
        toUInt64(a.session_event_count_total) as session_event_count_total,
        a.app_version_last as app_version_last
    from {{ ref('silver__user_activity_daily') }} as a
    where {{ activity_window_predicate }}
      and a.activity_date_utc >= toDate('{{ cohort_start_date }}')
      and isNull(a.client_id) = 0
      and a.client_id != ''
),

inapp_source as (
    select
        i.app_name as application,
        i.client_id as client_id,
        i.event_time as event_time,
        toDate(i.event_time) as event_date,
        i.event_name as event_name,
        i.price as price,
        i.price_gross as price_gross
    from {{ ref('silver__inapp_table') }} as i
    where {{ inapp_window_predicate }}
      and toDate(i.event_time) >= toDate('{{ cohort_start_date }}')
      and isNull(i.client_id) = 0
      and i.client_id != ''
),

subscription_source as (
    select
        s.app_name as application,
        s.client_id as client_id,
        s.event_time as event_time,
        toDate(s.event_time) as event_date,
        s.event_name as event_name,
        s.period_type as period_type,
        s.is_trial_conversion as is_trial_conversion,
        s.type as type,
        s.cancel_ind as cancel_ind,
        s.price as price,
        s.price_gross as price_gross
    from {{ ref('silver__subscription_table') }} as s
    where {{ subscription_window_predicate }}
      and toDate(s.event_time) >= toDate('{{ cohort_start_date }}')
      and isNull(s.client_id) = 0
      and s.client_id != ''
),

ad_revenue_source as (
    select
        a.app_name as application,
        a.client_id as client_id,
        a.event_date as event_date,
        a.impressions_count as impressions_count,
        a.impressions_rewarded as impressions_rewarded,
        a.impressions_interstitial as impressions_interstitial,
        a.impressions_banner as impressions_banner,
        a.ad_revenue_total as ad_revenue_total,
        a.revenue_rewarded as revenue_rewarded,
        a.revenue_interstitial as revenue_interstitial,
        a.revenue_banner as revenue_banner
    from {{ ref('silver__applovin_ad_user_daily') }} as a
    where {{ ad_revenue_window_predicate }}
      and a.event_date >= toDate('{{ cohort_start_date }}')
      and isNull(a.client_id) = 0
      and a.client_id != ''
),

window_clients as (
    select
        application,
        client_id
    from (
        select application, client_id from activity_source
        union all
        select application, client_id from inapp_source
        union all
        select application, client_id from subscription_source
        union all
        select application, client_id from ad_revenue_source
    )
    group by
        application,
        client_id
),

user_lifetimes as (
    select
        toDate(u.install_time) as cohort_date,
        toStartOfMonth(toDate(u.install_time)) as cohort_month,
        toStartOfWeek(toDate(u.install_time), 5) as cohort_week,
        multiIf(
            lowerUTF8(ifNull(u.platform, '')) like '%ios%', 'ios',
            lowerUTF8(ifNull(u.platform, '')) like '%and%', 'android',
            'unknown'
        ) as platform,
        coalesce(nullIf(u.app_name, ''), 'unknown') as application,
        coalesce(u.country_code, '') as country,
        coalesce(u.media_source, '') as media_source,
        coalesce(u.promo_campaign, '') as promo_campaign,
        coalesce(u.promo_campaign_id, '') as promo_campaign_id,
        coalesce(u.ad, '') as ad,
        coalesce(u.adset, '') as adset,
        coalesce(u.install_device_name, '') as install_device_name,
        coalesce(u.install_device_type, '') as install_device_type,
        u.client_id as client_id,
        u.user_lifetime_id as user_lifetime_id,
        u.user_lifetime_number as user_lifetime_number,
        u.install_time as install_time,
        toDate(u.install_time) as lifetime_start_date,
        u.lifetime_end_time as lifetime_end_time,
        if(
            isNull(u.lifetime_end_time) = 1,
            cast(null as Nullable(Date)),
            toDate(u.lifetime_end_time)
        ) as lifetime_end_date
    from {{ ref('silver__users_scd2') }} as u
    inner join window_clients as w
        on coalesce(nullIf(u.app_name, ''), 'unknown') = w.application
       and u.client_id = w.client_id
    where toDate(u.install_time) >= toDate('{{ cohort_start_date }}')
      and toDate(u.install_time) < {{ current_utc_date_expr }}
      {% if cohort_end_date is not none %}
      and toDate(u.install_time) <= toDate('{{ cohort_end_date }}')
      {% endif %}
      and isNull(u.client_id) = 0
      and u.client_id != ''
      and u.client_id != '00000000-0000-0000-0000-000000000000'
      and nullIf(u.app_name, '') is not null
      and {{ cohort_users_overlap_predicate }}
),

user_lifetime_lookup_ts as (
    select
        application,
        client_id,
        user_lifetime_id,
        install_time,
        lifetime_end_time
    from user_lifetimes
    order by
        application,
        client_id,
        install_time
),

user_lifetime_lookup_date as (
    select
        application,
        client_id,
        user_lifetime_id,
        lifetime_start_date,
        lifetime_end_date
    from user_lifetimes
),

activity_enriched as (
    select
        a.event_date as event_date,
        a.application as application,
        a.client_id as client_id,
        if(
            isNull(u.user_lifetime_id) = 0
            and (isNull(u.lifetime_end_time) = 1 or a.event_time < u.lifetime_end_time),
            u.user_lifetime_id,
            cast(null as Nullable(String))
        ) as user_lifetime_id,
        a.sessions_count as sessions_count,
        a.session_duration_seconds_total as session_duration_seconds_total,
        a.session_event_count_total as session_event_count_total,
        a.app_version_last as app_version_last
    from activity_source as a
    left asof join user_lifetime_lookup_ts as u
        on u.application = a.application
       and u.client_id = a.client_id
       and u.install_time <= a.event_time
),

activity_daily as (
    select
        event_date,
        application,
        client_id,
        user_lifetime_id,
        sessions_count,
        session_duration_seconds_total,
        session_event_count_total,
        app_version_last,
        toUInt8(1) as app_version_priority,
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
        toUInt64(0) as ad_impressions,
        toUInt64(0) as ad_impressions_rewarded,
        toUInt64(0) as ad_impressions_interstitial,
        toUInt64(0) as ad_impressions_banner,
        toDecimal64(0, 8) as inapp_revenue_gross,
        toDecimal64(0, 8) as subs_revenue_gross,
        toDecimal64(0, 8) as ad_revenue_gross,
        toDecimal64(0, 8) as inapp_refunds_revenue_net,
        toDecimal64(0, 8) as subs_refunds_revenue_net,
        toDecimal64(0, 8) as inapp_revenue_net,
        toDecimal64(0, 8) as subs_revenue_net,
        toDecimal64(0, 8) as ad_revenue_rewarded_gross,
        toDecimal64(0, 8) as ad_revenue_interstitial_gross,
        toDecimal64(0, 8) as ad_revenue_banner_gross
    from activity_enriched
    where isNull(user_lifetime_id) = 0
),

inapp_enriched as (
    select
        i.event_date as event_date,
        i.application as application,
        i.client_id as client_id,
        i.event_name as event_name,
        i.price as price,
        i.price_gross as price_gross,
        if(
            isNull(u.user_lifetime_id) = 0
            and (isNull(u.lifetime_end_time) = 1 or i.event_time < u.lifetime_end_time),
            u.user_lifetime_id,
            cast(null as Nullable(String))
        ) as user_lifetime_id
    from inapp_source as i
    left asof join user_lifetime_lookup_ts as u
        on u.application = i.application
       and u.client_id = i.client_id
       and u.install_time <= i.event_time
),

inapp_daily as (
    select
        event_date,
        application,
        client_id,
        user_lifetime_id,
        toUInt64(0) as sessions_count,
        toUInt64(0) as session_duration_seconds_total,
        toUInt64(0) as session_event_count_total,
        cast(null as Nullable(String)) as app_version_last,
        toUInt8(0) as app_version_priority,
        toUInt64(countIf(event_name = 'purchase')) as purchases,
        toUInt64(countIf(event_name = 'refund')) as refunds,
        toUInt64(countIf(event_name = 'purchase')) as inapp_purchases,
        toUInt64(countIf(event_name = 'refund')) as inapp_refunds_num,
        toUInt64(0) as subscription_purchases,
        toUInt64(0) as subscription_refunds_num,
        toUInt64(0) as trial_purchases,
        toUInt64(0) as trial_converted_purchases,
        toUInt64(0) as renewal_purchases,
        toUInt64(0) as cancelations,
        toUInt64(0) as ad_impressions,
        toUInt64(0) as ad_impressions_rewarded,
        toUInt64(0) as ad_impressions_interstitial,
        toUInt64(0) as ad_impressions_banner,
        toDecimal64(
            sumIf(
                abs(toFloat64(ifNull(price_gross, toDecimal64(0, 8)))),
                event_name = 'purchase'
            ),
            8
        ) as inapp_revenue_gross,
        toDecimal64(0, 8) as subs_revenue_gross,
        toDecimal64(0, 8) as ad_revenue_gross,
        toDecimal64(
            sumIf(
                abs(toFloat64(ifNull(price, toDecimal64(0, 8)))),
                event_name = 'refund'
            ),
            8
        ) as inapp_refunds_revenue_net,
        toDecimal64(0, 8) as subs_refunds_revenue_net,
        toDecimal64(
            sumIf(
                if(event_name = 'purchase', 1.0, -1.0) * abs(toFloat64(ifNull(price, toDecimal64(0, 8)))),
                event_name in ('purchase', 'refund')
            ),
            8
        ) as inapp_revenue_net,
        toDecimal64(0, 8) as subs_revenue_net,
        toDecimal64(0, 8) as ad_revenue_rewarded_gross,
        toDecimal64(0, 8) as ad_revenue_interstitial_gross,
        toDecimal64(0, 8) as ad_revenue_banner_gross
    from inapp_enriched
    where isNull(user_lifetime_id) = 0
    group by
        event_date,
        application,
        client_id,
        user_lifetime_id
),

subscription_enriched as (
    select
        s.event_date as event_date,
        s.application as application,
        s.client_id as client_id,
        s.event_name as event_name,
        s.period_type as period_type,
        s.is_trial_conversion as is_trial_conversion,
        s.type as type,
        s.cancel_ind as cancel_ind,
        s.price as price,
        s.price_gross as price_gross,
        if(
            isNull(u.user_lifetime_id) = 0
            and (isNull(u.lifetime_end_time) = 1 or s.event_time < u.lifetime_end_time),
            u.user_lifetime_id,
            cast(null as Nullable(String))
        ) as user_lifetime_id
    from subscription_source as s
    left asof join user_lifetime_lookup_ts as u
        on u.application = s.application
       and u.client_id = s.client_id
       and u.install_time <= s.event_time
),

subscription_daily as (
    select
        event_date,
        application,
        client_id,
        user_lifetime_id,
        toUInt64(0) as sessions_count,
        toUInt64(0) as session_duration_seconds_total,
        toUInt64(0) as session_event_count_total,
        cast(null as Nullable(String)) as app_version_last,
        toUInt8(0) as app_version_priority,
        toUInt64(
            countIf(
                event_name = 'purchase'
                and lowerUTF8(ifNull(period_type, '')) = 'normal'
            )
        ) as purchases,
        toUInt64(countIf(event_name = 'refund')) as refunds,
        toUInt64(0) as inapp_purchases,
        toUInt64(0) as inapp_refunds_num,
        toUInt64(
            countIf(
                event_name = 'purchase'
                and lowerUTF8(ifNull(period_type, '')) = 'normal'
            )
        ) as subscription_purchases,
        toUInt64(countIf(event_name = 'refund')) as subscription_refunds_num,
        toUInt64(
            countIf(
                event_name = 'purchase'
                and lowerUTF8(ifNull(period_type, '')) like '%trial%'
            )
        ) as trial_purchases,
        toUInt64(
            countIf(
                event_name = 'purchase'
                and ifNull(is_trial_conversion, 0) = 1
            )
        ) as trial_converted_purchases,
        toUInt64(
            countIf(
                event_name = 'purchase'
                and upperUTF8(ifNull(type, '')) = 'RENEWAL'
            )
        ) as renewal_purchases,
        toUInt64(countIf(ifNull(cancel_ind, 0) = 1)) as cancelations,
        toUInt64(0) as ad_impressions,
        toUInt64(0) as ad_impressions_rewarded,
        toUInt64(0) as ad_impressions_interstitial,
        toUInt64(0) as ad_impressions_banner,
        toDecimal64(0, 8) as inapp_revenue_gross,
        toDecimal64(
            sumIf(
                abs(toFloat64(ifNull(price_gross, toDecimal64(0, 8)))),
                event_name = 'purchase'
                and lowerUTF8(ifNull(period_type, '')) = 'normal'
            ),
            8
        ) as subs_revenue_gross,
        toDecimal64(0, 8) as ad_revenue_gross,
        toDecimal64(0, 8) as inapp_refunds_revenue_net,
        toDecimal64(
            sumIf(
                abs(toFloat64(ifNull(price, toDecimal64(0, 8)))),
                event_name = 'refund'
            ),
            8
        ) as subs_refunds_revenue_net,
        toDecimal64(0, 8) as inapp_revenue_net,
        toDecimal64(
            sumIf(
                if(event_name = 'purchase', 1.0, -1.0) * abs(toFloat64(ifNull(price, toDecimal64(0, 8)))),
                (
                    event_name = 'purchase'
                    and lowerUTF8(ifNull(period_type, '')) = 'normal'
                )
                or event_name = 'refund'
            ),
            8
        ) as subs_revenue_net,
        toDecimal64(0, 8) as ad_revenue_rewarded_gross,
        toDecimal64(0, 8) as ad_revenue_interstitial_gross,
        toDecimal64(0, 8) as ad_revenue_banner_gross
    from subscription_enriched
    where isNull(user_lifetime_id) = 0
    group by
        event_date,
        application,
        client_id,
        user_lifetime_id
),

ad_revenue_daily as (
    select
        a.event_date as event_date,
        a.application as application,
        a.client_id as client_id,
        u.user_lifetime_id as user_lifetime_id,
        toUInt64(0) as sessions_count,
        toUInt64(0) as session_duration_seconds_total,
        toUInt64(0) as session_event_count_total,
        cast(null as Nullable(String)) as app_version_last,
        toUInt8(0) as app_version_priority,
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
        toUInt64(sum(a.impressions_count)) as ad_impressions,
        toUInt64(sum(a.impressions_rewarded)) as ad_impressions_rewarded,
        toUInt64(sum(a.impressions_interstitial)) as ad_impressions_interstitial,
        toUInt64(sum(a.impressions_banner)) as ad_impressions_banner,
        toDecimal64(0, 8) as inapp_revenue_gross,
        toDecimal64(0, 8) as subs_revenue_gross,
        toDecimal64(sum(toFloat64(a.ad_revenue_total)), 8) as ad_revenue_gross,
        toDecimal64(0, 8) as inapp_refunds_revenue_net,
        toDecimal64(0, 8) as subs_refunds_revenue_net,
        toDecimal64(0, 8) as inapp_revenue_net,
        toDecimal64(0, 8) as subs_revenue_net,
        toDecimal64(sum(toFloat64(a.revenue_rewarded)), 8) as ad_revenue_rewarded_gross,
        toDecimal64(sum(toFloat64(a.revenue_interstitial)), 8) as ad_revenue_interstitial_gross,
        toDecimal64(sum(toFloat64(a.revenue_banner)), 8) as ad_revenue_banner_gross
    from ad_revenue_source as a
    inner join user_lifetime_lookup_date as u
        on u.application = a.application
       and u.client_id = a.client_id
       and a.event_date >= u.lifetime_start_date
       and (
            isNull(u.lifetime_end_date) = 1
            or a.event_date < u.lifetime_end_date
       )
    group by
        event_date,
        application,
        client_id,
        user_lifetime_id
),

unioned as (
    select * from activity_daily
    union all
    select * from inapp_daily
    union all
    select * from subscription_daily
    union all
    select * from ad_revenue_daily
),

aggregated as (
    select
        event_date,
        application,
        client_id,
        user_lifetime_id,
        toUInt64(sum(sessions_count)) as sessions_count,
        toUInt64(sum(session_duration_seconds_total)) as session_duration_seconds_total,
        toUInt64(sum(session_event_count_total)) as session_event_count_total,
        argMax(app_version_last, app_version_priority) as app_version_last,
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
        toUInt64(sum(ad_impressions)) as ad_impressions,
        toUInt64(sum(ad_impressions_rewarded)) as ad_impressions_rewarded,
        toUInt64(sum(ad_impressions_interstitial)) as ad_impressions_interstitial,
        toUInt64(sum(ad_impressions_banner)) as ad_impressions_banner,
        toDecimal64(sum(toFloat64(inapp_revenue_gross)), 8) as inapp_revenue_gross,
        toDecimal64(sum(toFloat64(subs_revenue_gross)), 8) as subs_revenue_gross,
        toDecimal64(sum(toFloat64(ad_revenue_gross)), 8) as ad_revenue_gross,
        toDecimal64(sum(toFloat64(inapp_refunds_revenue_net)), 8) as inapp_refunds_revenue_net,
        toDecimal64(sum(toFloat64(subs_refunds_revenue_net)), 8) as subs_refunds_revenue_net,
        toDecimal64(sum(toFloat64(inapp_revenue_net)), 8) as inapp_revenue_net,
        toDecimal64(sum(toFloat64(subs_revenue_net)), 8) as subs_revenue_net,
        toDecimal64(sum(toFloat64(ad_revenue_rewarded_gross)), 8) as ad_revenue_rewarded_gross,
        toDecimal64(sum(toFloat64(ad_revenue_interstitial_gross)), 8) as ad_revenue_interstitial_gross,
        toDecimal64(sum(toFloat64(ad_revenue_banner_gross)), 8) as ad_revenue_banner_gross
    from unioned
    group by
        event_date,
        application,
        client_id,
        user_lifetime_id
)

select
    u.cohort_date as cohort_date,
    u.cohort_month as cohort_month,
    u.cohort_week as cohort_week,
    a.event_date as event_date,
    toUInt16(dateDiff('day', u.cohort_date, a.event_date)) as day_idx,
    u.platform as platform,
    u.application as application,
    u.country as country,
    u.media_source as media_source,
    u.promo_campaign as promo_campaign,
    u.promo_campaign_id as promo_campaign_id,
    u.ad as ad,
    u.adset as adset,
    u.install_device_name as install_device_name,
    u.install_device_type as install_device_type,
    u.client_id as client_id,
    u.user_lifetime_id as user_lifetime_id,
    u.user_lifetime_number as user_lifetime_number,
    u.install_time as install_time,
    u.lifetime_start_date as lifetime_start_date,
    u.lifetime_end_date as lifetime_end_date,
    a.sessions_count as sessions_count,
    a.session_duration_seconds_total as session_duration_seconds_total,
    a.session_event_count_total as session_event_count_total,
    a.app_version_last as app_version_last,
    a.purchases as purchases,
    a.refunds as refunds,
    a.inapp_purchases as inapp_purchases,
    a.inapp_refunds_num as inapp_refunds_num,
    a.subscription_purchases as subscription_purchases,
    a.subscription_refunds_num as subscription_refunds_num,
    a.trial_purchases as trial_purchases,
    a.trial_converted_purchases as trial_converted_purchases,
    a.renewal_purchases as renewal_purchases,
    a.cancelations as cancelations,
    a.ad_impressions as ad_impressions,
    a.ad_impressions_rewarded as ad_impressions_rewarded,
    a.ad_impressions_interstitial as ad_impressions_interstitial,
    a.ad_impressions_banner as ad_impressions_banner,
    a.inapp_revenue_gross as inapp_revenue_gross,
    a.subs_revenue_gross as subs_revenue_gross,
    a.ad_revenue_gross as ad_revenue_gross,
    a.inapp_refunds_revenue_net as inapp_refunds_revenue_net,
    a.subs_refunds_revenue_net as subs_refunds_revenue_net,
    a.inapp_revenue_net as inapp_revenue_net,
    a.subs_revenue_net as subs_revenue_net,
    a.ad_revenue_rewarded_gross as ad_revenue_rewarded_gross,
    a.ad_revenue_interstitial_gross as ad_revenue_interstitial_gross,
    a.ad_revenue_banner_gross as ad_revenue_banner_gross,
    toDecimal64(
        toFloat64(a.inapp_revenue_gross)
        + toFloat64(a.subs_revenue_gross)
        + toFloat64(a.ad_revenue_gross),
        8
    ) as total_revenue_gross,
    toDecimal64(
        toFloat64(a.inapp_revenue_net)
        + toFloat64(a.subs_revenue_net)
        + toFloat64(a.ad_revenue_gross),
        8
    ) as total_revenue_net,
    now() as version
from aggregated as a
inner join user_lifetimes as u
    on a.application = u.application
   and a.client_id = u.client_id
   and a.user_lifetime_id = u.user_lifetime_id
where a.event_date >= u.cohort_date
