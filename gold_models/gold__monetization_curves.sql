{{ config(
    materialized = "table",
    alias = "monetization_curves",
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
        "ad",
        "cohort_day"
    ],
    partition_by = "toYYYYMM(cohort_date)",
    settings = {"allow_nullable_key": 1}
) }}

{% set max_cohort_day = var('gold_ua_monetization_curves_max_day', 400) %}
{% set test_start_date = var('gold_ua_monetization_curves_test_start_date', '2026-01-01') %}
{% set test_end_date = var('gold_ua_monetization_curves_test_end_date', '2026-02-01') %}
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

with long_metrics as (
    select
        cohort_date,
        cohort_month,
        cohort_week,
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
        cost,
        ad_revenue_gross,
        total_revenue_net
    from {{ ref('gold__ua_cohort_long') }}
    where application in (
        {% for app_name in allowed_applications %}
        '{{ app_name }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
    )
      and cohort_date >= toDate('{{ test_start_date }}')
      and cohort_date < toDate('{{ test_end_date }}')
      and day_idx >= 0
      and day_idx <= {{ max_cohort_day }}
),

grain_metrics as (
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
        least(
            toUInt16(greatest(dateDiff('day', l.cohort_date, addDays(today(), -1)), 0)),
            toUInt16({{ max_cohort_day }})
        ) as mature_cohort_day_max,
        toUInt64(sumIf(l.installs, l.day_idx = 0)) as installs,
        toDecimal64(sumIf(toFloat64(l.cost), l.day_idx = 0), 8) as spend,
        sumMap([toUInt16(l.day_idx)], [toFloat64(l.ad_revenue_gross)]) as ad_metric_map,
        sumMap([toUInt16(l.day_idx)], [toFloat64(l.total_revenue_net)]) as total_metric_map
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
        l.adset,
        mature_cohort_day_max
),

prepared as (
    select
        g.cohort_date as cohort_date,
        g.cohort_month as cohort_month,
        g.cohort_week as cohort_week,
        g.platform as platform,
        g.application as application,
        g.country as country,
        g.media_source as media_source,
        g.promo_campaign as promo_campaign,
        g.promo_campaign_id as promo_campaign_id,
        g.ad as ad,
        g.adset as adset,
        g.mature_cohort_day_max as mature_cohort_day_max,
        g.installs as installs,
        g.spend as spend,
        g.ad_metric_map.1 as ad_days,
        g.ad_metric_map.2 as ad_values,
        arrayCumSum(g.ad_metric_map.2) as ad_cumulative_values,
        g.total_metric_map.1 as total_days,
        g.total_metric_map.2 as total_values,
        arrayCumSum(g.total_metric_map.2) as total_cumulative_values
    from grain_metrics as g
),

expanded as (
    select
        p.cohort_date as cohort_date,
        p.cohort_month as cohort_month,
        p.cohort_week as cohort_week,
        toUInt16(arrayJoin(range(toUInt32(p.mature_cohort_day_max) + 1))) as cohort_day,
        p.platform as platform,
        p.application as application,
        p.country as country,
        p.media_source as media_source,
        p.promo_campaign as promo_campaign,
        p.promo_campaign_id as promo_campaign_id,
        p.ad as ad,
        p.adset as adset,
        p.installs as installs,
        p.spend as spend,
        p.ad_days as ad_days,
        p.ad_values as ad_values,
        p.ad_cumulative_values as ad_cumulative_values,
        p.total_days as total_days,
        p.total_values as total_values,
        p.total_cumulative_values as total_cumulative_values
    from prepared as p
)

select
    e.cohort_date as cohort_date,
    e.cohort_month as cohort_month,
    e.cohort_week as cohort_week,
    e.cohort_day as cohort_day,
    e.platform as platform,
    e.application as application,
    e.country as country,
    e.media_source as media_source,
    e.promo_campaign as promo_campaign,
    e.promo_campaign_id as promo_campaign_id,
    e.ad as ad,
    e.adset as adset,
    e.installs as installs,
    e.spend as spend,
    toDecimal64(
        if(indexOf(e.ad_days, e.cohort_day) = 0, 0, e.ad_values[indexOf(e.ad_days, e.cohort_day)]),
        8
    ) as ad_revenue_gross,
    toDecimal64(
        if(
            arrayLastIndex(x -> x <= e.cohort_day, e.ad_days) = 0,
            0,
            e.ad_cumulative_values[arrayLastIndex(x -> x <= e.cohort_day, e.ad_days)]
        ),
        8
    ) as ad_revenue_gross_cumulative,
    toDecimal64(
        if(
            arrayLastIndex(x -> x < e.cohort_day, e.ad_days) = 0,
            0,
            e.ad_cumulative_values[arrayLastIndex(x -> x < e.cohort_day, e.ad_days)]
        ),
        8
    ) as ad_revenue_gross_prev_day,
    toDecimal64(
        if(indexOf(e.ad_days, toUInt16(0)) = 0, 0, e.ad_values[indexOf(e.ad_days, toUInt16(0))]),
        8
    ) as ad_revenue_gross_day0,
    toDecimal64(
        if(indexOf(e.total_days, e.cohort_day) = 0, 0, e.total_values[indexOf(e.total_days, e.cohort_day)]),
        8
    ) as total_revenue_net,
    toDecimal64(
        if(
            arrayLastIndex(x -> x <= e.cohort_day, e.total_days) = 0,
            0,
            e.total_cumulative_values[arrayLastIndex(x -> x <= e.cohort_day, e.total_days)]
        ),
        8
    ) as total_revenue_net_cumulative,
    toDecimal64(
        if(
            arrayLastIndex(x -> x < e.cohort_day, e.total_days) = 0,
            0,
            e.total_cumulative_values[arrayLastIndex(x -> x < e.cohort_day, e.total_days)]
        ),
        8
    ) as total_revenue_net_prev_day,
    toDecimal64(
        if(indexOf(e.total_days, toUInt16(0)) = 0, 0, e.total_values[indexOf(e.total_days, toUInt16(0))]),
        8
    ) as total_revenue_net_day0,
    now() as version
from expanded as e
