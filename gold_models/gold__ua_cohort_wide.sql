{{ config(
    materialized = "incremental",
    alias = "ua_cohort_wide",
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
    unique_key = [
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
    incremental_strategy = "delete+insert",
    settings = {"allow_nullable_key": 1}
) }}

{% set cohort_start_date = var('gold_ua_cohort_start_date', '2024-02-08') %}
{% set cohort_end_date = var('gold_ua_cohort_end_date', var('end_date', none)) %}
{% set day_buckets = [0, 1, 2, 3, 7, 14, 30, 180, 360] %}
{% set allowed_applications = [
    '3 Tiles',
    '3 Tiles GP',
    'Live wallpapers',
    'Tamadog',
    'match me gp',
    'match me ios',
    'mycat',
    'mycat gp',
    'mydragon gp',
    'mydragon ios',
    'myshark gp',
    'myshark ios',
    'tamadog gp',
    'farm jam gp',
    'farm jam ios'
] %}
{% set count_bucket_metrics = [
    'ad_impressions',
    'purchases',
    'refunds',
    'inapp_purchases',
    'inapp_refunds_num',
    'subscription_purchases',
    'subscription_refunds_num',
    'trial_purchases',
    'trial_converted_purchases',
    'renewal_purchases',
    'cancelations',
    'ad_impressions_rewarded',
    'ad_impressions_interstitial',
    'ad_impressions_banner'
] %}
{% set exact_day_count_metrics = ['active_users'] %}
{% set decimal_bucket_metrics = [
    'inapp_revenue_gross',
    'subs_revenue_gross',
    'ad_revenue_gross',
    'total_revenue_net',
    'total_revenue_gross',
    'inapp_refunds_revenue_net',
    'subs_refunds_revenue_net',
    'inapp_revenue_net',
    'subs_revenue_net',
    'ad_revenue_rewarded_gross',
    'ad_revenue_interstitial_gross',
    'ad_revenue_banner_gross'
] %}
{% set utn_count_metrics = [
    'ad_impressions',
    'purchases',
    'refunds',
    'inapp_purchases',
    'inapp_refunds_num',
    'subscription_purchases',
    'subscription_refunds_num',
    'trial_purchases',
    'trial_converted_purchases',
    'renewal_purchases',
    'cancelations',
    'ad_impressions_rewarded',
    'ad_impressions_interstitial',
    'ad_impressions_banner'
] %}
{% set utn_decimal_metrics = [
    'inapp_revenue_gross',
    'subs_revenue_gross',
    'ad_revenue_gross',
    'total_revenue_net',
    'total_revenue_gross',
    'inapp_refunds_revenue_net',
    'subs_refunds_revenue_net',
    'inapp_revenue_net',
    'subs_revenue_net',
    'ad_revenue_rewarded_gross',
    'ad_revenue_interstitial_gross',
    'ad_revenue_banner_gross'
] %}

with long_metrics as (
    select *
    from {{ ref('gold__ua_cohort_long') }}
    where cohort_date >= toDate('{{ cohort_start_date }}')
      {% if cohort_end_date is not none %}
      and cohort_date <= toDate('{{ cohort_end_date }}')
      {% endif %}
      and application in (
        {% for app_name in allowed_applications %}
        '{{ app_name }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
    )
),

wide_monetization as (
    select
        l.cohort_date as cohort_date,
        any(l.cohort_month) as cohort_month,
        any(l.cohort_week) as cohort_week,
        l.platform as platform,
        l.application as application,
        l.country as country,
        l.media_source as media_source,
        l.promo_campaign as promo_campaign,
        l.promo_campaign_id as promo_campaign_id,
        l.ad as ad,
        l.adset as adset,
        toUInt64(sumIf(l.installs, l.day_idx = 0)) as installs_0,
        toUInt64(sumIf(l.impressions, l.day_idx = 0)) as impressions_0,
        toUInt64(sumIf(l.clicks, l.day_idx = 0)) as clicks_0,
        toDecimal64(sumIf(toFloat64(l.cost), l.day_idx = 0), 8) as cost_0
        {% for bucket in day_buckets %}
            {% for metric in count_bucket_metrics %}
                {% if bucket == 0 %}
        , toUInt64(sumIf(l.{{ metric }}, l.day_idx = 0)) as {{ metric }}_{{ bucket }}
                {% else %}
        , toUInt64(
            sumIf(
                l.{{ metric }},
                l.day_idx <= {{ bucket }}
                and dateDiff('day', l.cohort_date, addDays({{ monitor_run_date_expr() }}, -1)) >= {{ bucket }}
            )
        ) as {{ metric }}_{{ bucket }}
                {% endif %}
            {% endfor %}
            {% for metric in decimal_bucket_metrics %}
                {% if bucket == 0 %}
        , toDecimal64(sumIf(toFloat64(l.{{ metric }}), l.day_idx = 0), 8) as {{ metric }}_{{ bucket }}
                {% else %}
        , toDecimal64(
            sumIf(
                toFloat64(l.{{ metric }}),
                l.day_idx <= {{ bucket }}
                and dateDiff('day', l.cohort_date, addDays({{ monitor_run_date_expr() }}, -1)) >= {{ bucket }}
            ),
            8
        ) as {{ metric }}_{{ bucket }}
                {% endif %}
            {% endfor %}
        {% endfor %}
        {% for metric in utn_count_metrics %}
        , toUInt64(sum(l.{{ metric }})) as {{ metric }}_utn
        {% endfor %}
        {% for metric in utn_decimal_metrics %}
        , toDecimal64(sum(toFloat64(l.{{ metric }})), 8) as {{ metric }}_utn
        {% endfor %}
    from long_metrics as l
    group by
        l.cohort_date,
        l.platform,
        l.application,
        l.country,
        l.media_source,
        l.promo_campaign,
        l.promo_campaign_id,
        l.ad,
        l.adset
),

payer_users as (
    select
        p.cohort_date as cohort_date,
        p.platform as platform,
        p.application as application,
        p.country as country,
        p.media_source as media_source,
        p.promo_campaign as promo_campaign,
        p.promo_campaign_id as promo_campaign_id,
        p.ad as ad,
        p.adset as adset,
        dateDiff('day', p.cohort_date, p.first_purchase_date) as first_purchase_day_idx,
        if(
            isNull(p.first_inapp_purchase_date) = 1,
            cast(null as Nullable(Int32)),
            dateDiff('day', p.cohort_date, p.first_inapp_purchase_date)
        ) as first_inapp_purchase_day_idx,
        if(
            isNull(p.first_subscription_purchase_date) = 1,
            cast(null as Nullable(Int32)),
            dateDiff('day', p.cohort_date, p.first_subscription_purchase_date)
        ) as first_subscription_purchase_day_idx
    from {{ ref('gold__ua_payers') }} as p
    where p.cohort_date >= toDate('{{ cohort_start_date }}')
      {% if cohort_end_date is not none %}
      and p.cohort_date <= toDate('{{ cohort_end_date }}')
      {% endif %}
      and p.first_purchase_date >= p.cohort_date
      and p.application in (
        {% for app_name in allowed_applications %}
        '{{ app_name }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
      )
),

payer_metrics as (
    select
        p.cohort_date as cohort_date,
        p.platform as platform,
        p.application as application,
        p.country as country,
        p.media_source as media_source,
        p.promo_campaign as promo_campaign,
        p.promo_campaign_id as promo_campaign_id,
        p.ad as ad,
        p.adset as adset
        {% for bucket in day_buckets %}
        {% if bucket == 0 %}
        , toUInt64(countIf(p.first_purchase_day_idx = 0)) as paying_users_{{ bucket }}
        , toUInt64(countIf(p.first_inapp_purchase_day_idx = 0)) as inapp_payers_{{ bucket }}
        , toUInt64(countIf(p.first_subscription_purchase_day_idx = 0)) as subs_payers_{{ bucket }}
        , toUInt64(countIf(p.first_purchase_day_idx = 0)) as new_payers_{{ bucket }}
        {% else %}
        , toUInt64(
            countIf(
                p.first_purchase_day_idx >= 0
                and p.first_purchase_day_idx <= {{ bucket }}
                and dateDiff('day', p.cohort_date, addDays({{ monitor_run_date_expr() }}, -1)) >= {{ bucket }}
            )
        ) as paying_users_{{ bucket }}
        , toUInt64(
            countIf(
                p.first_inapp_purchase_day_idx >= 0
                and p.first_inapp_purchase_day_idx <= {{ bucket }}
                and dateDiff('day', p.cohort_date, addDays({{ monitor_run_date_expr() }}, -1)) >= {{ bucket }}
            )
        ) as inapp_payers_{{ bucket }}
        , toUInt64(
            countIf(
                p.first_subscription_purchase_day_idx >= 0
                and p.first_subscription_purchase_day_idx <= {{ bucket }}
                and dateDiff('day', p.cohort_date, addDays({{ monitor_run_date_expr() }}, -1)) >= {{ bucket }}
            )
        ) as subs_payers_{{ bucket }}
        , toUInt64(
            countIf(
                p.first_purchase_day_idx = {{ bucket }}
                and dateDiff('day', p.cohort_date, addDays({{ monitor_run_date_expr() }}, -1)) >= {{ bucket }}
            )
        ) as new_payers_{{ bucket }}
        {% endif %}
        {% endfor %}
        , toUInt64(countIf(p.first_purchase_day_idx >= 0)) as paying_users_utn
        , toUInt64(countIf(p.first_inapp_purchase_day_idx >= 0)) as inapp_payers_utn
        , toUInt64(countIf(p.first_subscription_purchase_day_idx >= 0)) as subs_payers_utn
        , toUInt64(countIf(p.first_purchase_day_idx >= 0)) as new_payers_utn
    from payer_users as p
    group by
        p.cohort_date,
        p.platform,
        p.application,
        p.country,
        p.media_source,
        p.promo_campaign,
        p.promo_campaign_id,
        p.ad,
        p.adset
)

,

activity_metrics as (
    select
        l.cohort_date as cohort_date,
        l.platform as platform,
        l.application as application,
        l.country as country,
        l.media_source as media_source,
        l.promo_campaign as promo_campaign,
        l.promo_campaign_id as promo_campaign_id,
        l.ad as ad,
        l.adset as adset
        {% for bucket in day_buckets %}
        {% if bucket == 0 %}
        , toUInt64(sumIf(l.clients_sdk, l.day_idx = 0)) as active_users_{{ bucket }}
        {% else %}
        , toUInt64(
            sumIf(
                l.clients_sdk,
                l.day_idx = {{ bucket }}
                and dateDiff('day', l.cohort_date, addDays({{ monitor_run_date_expr() }}, -1)) >= {{ bucket }}
            )
        ) as active_users_{{ bucket }}
        {% endif %}
        {% endfor %}
    from long_metrics as l
    group by
        l.cohort_date,
        l.platform,
        l.application,
        l.country,
        l.media_source,
        l.promo_campaign,
        l.promo_campaign_id,
        l.ad,
        l.adset
),

activity_utn as (
    select
        a.cohort_date as cohort_date,
        a.platform as platform,
        a.application as application,
        a.country as country,
        a.media_source as media_source,
        a.promo_campaign as promo_campaign,
        a.promo_campaign_id as promo_campaign_id,
        a.ad as ad,
        a.adset as adset,
        a.clients_sdk_utn as active_users_utn
    from {{ ref('gold__ua_cohort_activity_utn') }} as a
    where a.cohort_date >= toDate('{{ cohort_start_date }}')
      {% if cohort_end_date is not none %}
      and a.cohort_date <= toDate('{{ cohort_end_date }}')
      {% endif %}
      and a.application in (
        {% for app_name in allowed_applications %}
        '{{ app_name }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
    )
)

select
    m.cohort_date as cohort_date,
    m.cohort_month as cohort_month,
    m.cohort_week as cohort_week,
    m.platform as platform,
    m.application as application,
    m.country as country,
    m.media_source as media_source,
    m.promo_campaign as promo_campaign,
    m.promo_campaign_id as promo_campaign_id,
    m.ad as ad,
    m.adset as adset,
    m.installs_0 as installs_0,
    m.impressions_0 as impressions_0,
    m.clicks_0 as clicks_0,
    m.cost_0 as cost_0
    {% for bucket in day_buckets %}
        {% for metric in exact_day_count_metrics %}
    , ifNull(a.{{ metric }}_{{ bucket }}, toUInt64(0)) as {{ metric }}_{{ bucket }}
        {% endfor %}
        {% for metric in count_bucket_metrics %}
    , m.{{ metric }}_{{ bucket }} as {{ metric }}_{{ bucket }}
        {% endfor %}
        {% for metric in decimal_bucket_metrics %}
    , m.{{ metric }}_{{ bucket }} as {{ metric }}_{{ bucket }}
        {% endfor %}
    , toDecimal64(
        toFloat64(m.inapp_refunds_revenue_net_{{ bucket }})
        + toFloat64(m.subs_refunds_revenue_net_{{ bucket }}),
        8
    ) as total_refunds_revenue_{{ bucket }}
    , ifNull(p.paying_users_{{ bucket }}, toUInt64(0)) as paying_users_{{ bucket }}
    , ifNull(p.inapp_payers_{{ bucket }}, toUInt64(0)) as inapp_payers_{{ bucket }}
    , ifNull(p.subs_payers_{{ bucket }}, toUInt64(0)) as subs_payers_{{ bucket }}
    , ifNull(p.new_payers_{{ bucket }}, toUInt64(0)) as new_payers_{{ bucket }}
    {% endfor %}
    {% for metric in utn_count_metrics %}
    , m.{{ metric }}_utn as {{ metric }}_utn
    {% endfor %}
    , ifNull(a_utn.active_users_utn, toUInt64(0)) as active_users_utn
    {% for metric in utn_decimal_metrics %}
    , m.{{ metric }}_utn as {{ metric }}_utn
    {% endfor %}
    , toDecimal64(
        toFloat64(m.inapp_refunds_revenue_net_utn)
        + toFloat64(m.subs_refunds_revenue_net_utn),
        8
    ) as total_refunds_revenue_utn
    , ifNull(p.paying_users_utn, toUInt64(0)) as paying_users_utn
    , ifNull(p.inapp_payers_utn, toUInt64(0)) as inapp_payers_utn
    , ifNull(p.subs_payers_utn, toUInt64(0)) as subs_payers_utn
    , ifNull(p.new_payers_utn, toUInt64(0)) as new_payers_utn
    , {{ monitor_run_ts_expr() }} as version
from wide_monetization as m
left join payer_metrics as p
    on m.cohort_date = p.cohort_date
   and m.platform = p.platform
   and m.application = p.application
   and m.country = p.country
   and m.media_source = p.media_source
   and m.promo_campaign = p.promo_campaign
   and m.promo_campaign_id = p.promo_campaign_id
   and m.ad = p.ad
   and m.adset = p.adset
left join activity_metrics as a
    on m.cohort_date = a.cohort_date
   and m.platform = a.platform
   and m.application = a.application
   and m.country = a.country
   and m.media_source = a.media_source
   and m.promo_campaign = a.promo_campaign
   and m.promo_campaign_id = a.promo_campaign_id
   and m.ad = a.ad
   and m.adset = a.adset
left join activity_utn as a_utn
    on m.cohort_date = a_utn.cohort_date
   and m.platform = a_utn.platform
   and m.application = a_utn.application
   and m.country = a_utn.country
   and m.media_source = a_utn.media_source
   and m.promo_campaign = a_utn.promo_campaign
   and m.promo_campaign_id = a_utn.promo_campaign_id
   and m.ad = a_utn.ad
   and m.adset = a_utn.adset
