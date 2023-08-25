{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['date', 'account_id']},
    ]
)}}

WITH

ts_holdings AS (
    SELECT
        account_id,
        date,
        COUNT(subscription_id) AS num_subscriptions,
        ARRAY_AGG(product_name) AS product_names,
        ARRAY_AGG(product_family) AS product_families,
        AVG(product_price) AS avg_product_price
    FROM {{ ref('int_product_holdings_exploded_to_timeseries') }}
    GROUP BY account_id, date
),

ts_interactions AS (
    SELECT
        account_id,
        date,
        COUNT(*) AS num_interactions,
        AVG(time_in_queue) AS avg_time_in_queue,
        AVG(handling_time_s) AS avg_handling_time_s,
        AVG(customer_satisfaction_after_call) AS avg_customer_satisfaction_after_call,
        ARRAY_AGG(call_reason) AS call_reasons
    FROM {{ ref('int_interactions_exploded_to_timeseries') }}
    GROUP BY account_id, date
),

ts_combined AS (
    SELECT
        h.account_id,
        h.date,
        h.num_subscriptions,
        h.product_names,
        h.product_families,
        h.avg_product_price,
        i.num_interactions,
        i.avg_time_in_queue,
        i.avg_handling_time_s,
        i.avg_customer_satisfaction_after_call,
        i.call_reasons,
        CASE WHEN h.num_subscriptions > 0 
            THEN 0
        ELSE 1 END AS is_churned

    FROM ts_holdings h
    INNER JOIN ts_interactions i
    ON h.account_id = i.account_id AND h.date = i.date
)

SELECT * FROM ts_combined