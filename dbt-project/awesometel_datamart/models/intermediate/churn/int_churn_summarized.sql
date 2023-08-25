{{ config(
    materialized = 'table',
)}}

WITH

churn_lag AS (
    SELECT 
        account_id,
        date,
        is_churned AS current_day_churn,
        LAG(is_churned) OVER (PARTITION BY account_id ORDER BY date) AS previous_day_churn
    FROM 
        {{ ref('int_timeseries_combined') }}
),

churn AS (
    SELECT 
        account_id,
        date AS churn_date
    FROM 
        churn_lag
    WHERE 
        current_day_churn = 1 AND 
        (previous_day_churn IS NOT NULL AND previous_day_churn <> 1)
),

summary AS (
    SELECT 
        ai.id AS account_id,
        ARRAY_AGG(churn_date) AS churn_dates
    FROM {{ ref('stg_awesometel__accounts') }} as ai
    LEFT JOIN churn c
    ON ai.id = c.account_id
    WHERE ai.id IN (SELECT DISTINCT account_id FROM {{ ref('int_timeseries_combined') }})
    GROUP BY ai.id
),

clean_summary AS (
    SELECT 
        account_id,
        ARRAY_REMOVE(churn_dates, NULL) AS churn_dates,
        CASE 
            WHEN CARDINALITY(ARRAY_REMOVE(churn_dates, NULL)) > 0 THEN 1
            ELSE 0
        END AS is_churned
    FROM summary
)

SELECT * FROM clean_summary




