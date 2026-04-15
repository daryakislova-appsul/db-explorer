{{ config(
    materialized = "table",
    alias = "app_revenue_by_source_daily",
    engine = "MergeTree()",
    order_by = ["event_date", "app_name", "platform", "source_tag", "source_model"],
    partition_by = "toYYYYMM(event_date)",
    settings = {"allow_nullable_key": 1}
) }}

{% set start_date = var('gold_app_revenue_start_date', '2026-01-01') %}
{% set gold_timezone = 'America/Los_Angeles' %}

with
appsyoulove_subscription as (
    select
        toDate(toTimeZone(event_time, '{{ gold_timezone }}')) as event_date,
        coalesce(nullIf(app_name, ''), 'unknown') as app_name,
        multiIf(
            lowerUTF8(ifNull(platform, '')) like '%ios%', 'ios',
            lowerUTF8(ifNull(platform, '')) like '%and%', 'android',
            'unknown'
        ) as platform,
        'appsyoulove' as source_tag,
        'silver__subscription_table' as source_model,
        toDecimal64(
            sum(
                multiIf(
                    event_name = 'refund',
                    -abs(
                        toFloat64(ifNull(local_price, toDecimal64(0, 8)))
                        / multiIf(
                            isNull(currency_rates_denominator) = 1, 1.0,
                            toFloat64(currency_rates_denominator) <= 0.0, 1.0,
                            toFloat64(currency_rates_denominator)
                        )
                    ),
                    abs(
                        toFloat64(ifNull(local_price, toDecimal64(0, 8)))
                        / multiIf(
                            isNull(currency_rates_denominator) = 1, 1.0,
                            toFloat64(currency_rates_denominator) <= 0.0, 1.0,
                            toFloat64(currency_rates_denominator)
                        )
                    )
                )
            ),
            8
        ) as subs_revenue_gross_usd,
        toDecimal64(0, 8) as inapp_revenue_gross_usd,
        toDecimal64(0, 8) as ad_revenue_gross_usd,
        toUInt64(countIf(event_name = 'purchase')) as purchase_events_count,
        toUInt64(countIf(event_name = 'refund')) as refund_events_count,
        toUInt64(0) as impressions_count,
        toUInt64(count()) as rows_count
    from {{ ref('silver__subscription_table') }}
    where toDate(toTimeZone(event_time, '{{ gold_timezone }}')) >= toDate('{{ start_date }}')
    group by
        event_date,
        app_name,
        platform,
        source_tag,
        source_model
),

appsyoulove_inapp as (
    select
        toDate(toTimeZone(event_time, '{{ gold_timezone }}')) as event_date,
        coalesce(nullIf(app_name, ''), 'unknown') as app_name,
        multiIf(
            lowerUTF8(ifNull(platform, '')) like '%ios%', 'ios',
            lowerUTF8(ifNull(platform, '')) like '%and%', 'android',
            'unknown'
        ) as platform,
        'appsyoulove' as source_tag,
        'silver__inapp_table' as source_model,
        toDecimal64(0, 8) as subs_revenue_gross_usd,
        toDecimal64(
            sum(
                multiIf(
                    event_name = 'refund',
                    -abs(
                        toFloat64(ifNull(local_price, toDecimal64(0, 8)))
                        / multiIf(
                            isNull(currency_rates_denominator) = 1, 1.0,
                            toFloat64(currency_rates_denominator) <= 0.0, 1.0,
                            toFloat64(currency_rates_denominator)
                        )
                    ),
                    abs(
                        toFloat64(ifNull(local_price, toDecimal64(0, 8)))
                        / multiIf(
                            isNull(currency_rates_denominator) = 1, 1.0,
                            toFloat64(currency_rates_denominator) <= 0.0, 1.0,
                            toFloat64(currency_rates_denominator)
                        )
                    )
                )
            ),
            8
        ) as inapp_revenue_gross_usd,
        toDecimal64(0, 8) as ad_revenue_gross_usd,
        toUInt64(countIf(event_name = 'purchase')) as purchase_events_count,
        toUInt64(countIf(event_name = 'refund')) as refund_events_count,
        toUInt64(0) as impressions_count,
        toUInt64(count()) as rows_count
    from {{ ref('silver__inapp_table') }}
    where toDate(toTimeZone(event_time, '{{ gold_timezone }}')) >= toDate('{{ start_date }}')
    group by
        event_date,
        app_name,
        platform,
        source_tag,
        source_model
),

magify_payments_prepared as (
    select
        toDate(toTimeZone(purchased_at_ms, '{{ gold_timezone }}')) as event_date,
        coalesce(nullIf(application, ''), 'unknown') as app_name,
        multiIf(
            lowerUTF8(ifNull(store_name, '')) like '%app_store%', 'ios',
            lowerUTF8(ifNull(store_name, '')) like '%ios%', 'ios',
            lowerUTF8(ifNull(store_name, '')) like '%play_store%', 'android',
            lowerUTF8(ifNull(store_name, '')) like '%google%', 'android',
            lowerUTF8(ifNull(store_name, '')) like '%android%', 'android',
            'unknown'
        ) as platform,
        type,
        lowerUTF8(ifNull(product_id_type, '')) as product_id_type,
        toDecimal64(
            toFloat64(ifNull(price, toDecimal64(0, 8)))
            / multiIf(
                isNull(currencyRatesDenominator) = 1, 1.0,
                toFloat64(currencyRatesDenominator) <= 0.0, 1.0,
                toFloat64(currencyRatesDenominator)
            ),
            8
        ) as amount_gross_usd
    from {{ ref('magify_payments_events') }}
    where toDate(toTimeZone(purchased_at_ms, '{{ gold_timezone }}')) >= toDate('{{ start_date }}')
),

magify_payments_classified as (
    select
        event_date,
        app_name,
        platform,
        type,
        multiIf(
            product_id_type like '%sub%', 'subscription',
            product_id_type like '%consumable%', 'inapp',
            product_id_type like '%non_consumable%', 'inapp',
            product_id_type like '%inapp%', 'inapp',
            type = 'NON_RENEWING_PURCHASE', 'inapp',
            'subscription'
        ) as revenue_type,
        multiIf(
            type = 'CANCELLATION', -abs(toFloat64(ifNull(amount_gross_usd, toDecimal64(0, 8)))),
            abs(toFloat64(ifNull(amount_gross_usd, toDecimal64(0, 8))))
        ) as signed_amount_gross_usd
    from magify_payments_prepared
),

magify_payments as (
    select
        event_date,
        app_name,
        platform,
        'magify' as source_tag,
        'magify_payments_events' as source_model,
        toDecimal64(sumIf(signed_amount_gross_usd, revenue_type = 'subscription'), 8) as subs_revenue_gross_usd,
        toDecimal64(sumIf(signed_amount_gross_usd, revenue_type = 'inapp'), 8) as inapp_revenue_gross_usd,
        toDecimal64(0, 8) as ad_revenue_gross_usd,
        toUInt64(countIf(type != 'CANCELLATION')) as purchase_events_count,
        toUInt64(countIf(type = 'CANCELLATION')) as refund_events_count,
        toUInt64(0) as impressions_count,
        toUInt64(count()) as rows_count
    from magify_payments_classified
    group by
        event_date,
        app_name,
        platform,
        source_tag,
        source_model
),

magify_applovin_user as (
    select
        toDate(toTimeZone(toDateTime(event_date, 'UTC'), '{{ gold_timezone }}')) as event_date,
        coalesce(nullIf(app_name, ''), 'unknown') as app_name,
        multiIf(
            lowerUTF8(ifNull(platform, '')) like '%ios%', 'ios',
            lowerUTF8(ifNull(platform, '')) like '%and%', 'android',
            'unknown'
        ) as platform,
        'magify' as source_tag,
        'silver__applovin_user_revenue_table' as source_model,
        toDecimal64(0, 8) as subs_revenue_gross_usd,
        toDecimal64(0, 8) as inapp_revenue_gross_usd,
        toDecimal64(sum(toFloat64(revenue_total)), 8) as ad_revenue_gross_usd,
        toUInt64(0) as purchase_events_count,
        toUInt64(0) as refund_events_count,
        toUInt64(sum(impressions_count)) as impressions_count,
        toUInt64(count()) as rows_count
    from {{ ref('silver__applovin_user_revenue_table') }}
    where toDate(toTimeZone(toDateTime(event_date, 'UTC'), '{{ gold_timezone }}')) >= toDate('{{ start_date }}')
    group by
        event_date,
        app_name,
        platform,
        source_tag,
        source_model
),

apple as (
    select
        ar.event_date as event_date,
        coalesce(nullIf(ar.app_name, ''), 'unknown') as app_name,
        multiIf(
            lowerUTF8(ifNull(ar.platform, '')) like '%ios%', 'ios',
            lowerUTF8(ifNull(ar.platform, '')) like '%and%', 'android',
            'unknown'
        ) as platform,
        'apple' as source_tag,
        'silver__apple_revenue_table' as source_model,
        toDecimal64(sum(toFloat64(ar.revenue_gross_usd_subscription)), 8) as subs_revenue_gross_usd,
        toDecimal64(sum(toFloat64(ar.revenue_gross_usd_inapp)), 8) as inapp_revenue_gross_usd,
        toDecimal64(0, 8) as ad_revenue_gross_usd,
        toUInt64(0) as purchase_events_count,
        toUInt64(0) as refund_events_count,
        toUInt64(0) as impressions_count,
        toUInt64(sum(ar.rows_count)) as rows_count
    from {{ ref('silver__apple_revenue_table') }} as ar
    where ar.event_date >= toDate('{{ start_date }}')
    group by
        event_date,
        app_name,
        platform,
        source_tag,
        source_model
),

adjust as (
    select
        -- Adjust daily export date is already aligned to Pacific Time.
        toDate(event_date) as event_date,
        coalesce(nullIf(app_name, ''), 'unknown') as app_name,
        multiIf(
            lowerUTF8(ifNull(platform, '')) like '%ios%', 'ios',
            lowerUTF8(ifNull(platform, '')) like '%and%', 'android',
            'unknown'
        ) as platform,
        'adjust' as source_tag,
        'silver__adjust_revenue_table' as source_model,
        toDecimal64(sum(toFloat64(revenue_subscription)), 8) as subs_revenue_gross_usd,
        toDecimal64(sum(toFloat64(revenue_inapp)), 8) as inapp_revenue_gross_usd,
        toDecimal64(sum(toFloat64(revenue_ad)), 8) as ad_revenue_gross_usd,
        toUInt64(0) as purchase_events_count,
        toUInt64(0) as refund_events_count,
        toUInt64(sum(count_impression)) as impressions_count,
        toUInt64(sum(count_total)) as rows_count
    from {{ ref('silver__adjust_revenue_table') }}
    where toDate(event_date) >= toDate('{{ start_date }}')
    group by
        event_date,
        app_name,
        platform,
        source_tag,
        source_model
),

applovin_impression as (
    select
        toDate(toTimeZone(toDateTime(event_date, 'UTC'), '{{ gold_timezone }}')) as event_date,
        coalesce(nullIf(app_name, ''), coalesce(nullIf(project, ''), 'unknown')) as app_name,
        multiIf(
            lowerUTF8(ifNull(platform, '')) like '%ios%', 'ios',
            lowerUTF8(ifNull(platform, '')) like '%and%', 'android',
            'unknown'
        ) as platform,
        'applovin' as source_tag,
        'silver__applovin_impression_revenue_table' as source_model,
        toDecimal64(0, 8) as subs_revenue_gross_usd,
        toDecimal64(0, 8) as inapp_revenue_gross_usd,
        toDecimal64(sum(toFloat64(revenue_total)), 8) as ad_revenue_gross_usd,
        toUInt64(0) as purchase_events_count,
        toUInt64(0) as refund_events_count,
        toUInt64(sum(impressions_count)) as impressions_count,
        toUInt64(count()) as rows_count
    from {{ ref('silver__applovin_impression_revenue_table') }}
    where toDate(toTimeZone(toDateTime(event_date, 'UTC'), '{{ gold_timezone }}')) >= toDate('{{ start_date }}')
    group by
        event_date,
        app_name,
        platform,
        source_tag,
        source_model
),

unioned as (
    select * from appsyoulove_subscription
    union all
    select * from appsyoulove_inapp
    union all
    select * from magify_payments
    union all
    select * from magify_applovin_user
    union all
    select * from apple
    union all
    select * from adjust
    union all
    select * from applovin_impression
)

select
    event_date,
    app_name,
    platform,
    source_tag,
    source_model,
    toFloat64(ad_revenue_gross_usd) as ad_revenue_gross,
    toFloat64(inapp_revenue_gross_usd) as inapp_revenue_gross,
    toFloat64(subs_revenue_gross_usd) as subs_revenue_gross,
    purchase_events_count,
    refund_events_count,
    impressions_count,
    rows_count
from unioned
where
    lowerUTF8(app_name) in (
        '3 tiles',
        '3 tiles gp',
        'live wallpapers',
        'numberzilla',
        'numberzilla-android',
        'tamadog',
        'unicorn',
        'unicorn 3d',
        'blockjam ios',
        'blockjam gp',
        'farm jam ios',
        'farm jam gp',
        'match me ios',
        'match me gp',
        'multido ios',
        'multido gp',
        'mycat',
        'mycat gp',
        'mydragon ios',
        'mydragon gp',
        'myshark ios',
        'myshark gp',
        'sweet puzzle ios',
        'sweet puzzle gp',
        'tile springs ios',
        'tile springs gp',
        'tilesofhope ios',
        'tilesofhope gp',
        'twinspuzzle ios',
        'twinspuzzle android'
    )
