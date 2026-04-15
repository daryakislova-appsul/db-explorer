{% macro calc_price_usd(local_price_expr, currency_rates_denominator_expr, vat_income_multiplier_expr) %}
    (
        toDecimal128(
            (
                toFloat64(ifNull({{ local_price_expr }}, toDecimal64(0, 8)))
                / multiIf(
                    isNull({{ currency_rates_denominator_expr }}) = 1, 1.0,
                    toFloat64({{ currency_rates_denominator_expr }}) <= 0.0, 1.0,
                    toFloat64({{ currency_rates_denominator_expr }})
                )
            )
            * multiIf(
                isNull({{ vat_income_multiplier_expr }}) = 1, 1.0,
                toFloat64({{ vat_income_multiplier_expr }}) <= 0.0, 1.0,
                toFloat64({{ vat_income_multiplier_expr }})
            ),
            8
        )
    )
{% endmacro %}

{% macro calc_price_gross_usd(local_price_expr, currency_rates_denominator_expr) %}
    (
        toDecimal128(
            (
                toFloat64(ifNull({{ local_price_expr }}, toDecimal64(0, 8)))
                / multiIf(
                    isNull({{ currency_rates_denominator_expr }}) = 1, 1.0,
                    toFloat64({{ currency_rates_denominator_expr }}) <= 0.0, 1.0,
                    toFloat64({{ currency_rates_denominator_expr }})
                )
            ),
            8
        )
    )
{% endmacro %}
