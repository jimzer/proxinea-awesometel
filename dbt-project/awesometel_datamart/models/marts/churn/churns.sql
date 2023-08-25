WITH

-- Get users with 1 churn
churned_users AS (
    SELECT
        account_id,
        (churn_dates[1] - {{ var('lead_time') }}) AS end_date,
        TRUE AS is_churned
    FROM {{ ref('int_churn_summarized') }}
    WHERE CARDINALITY(churn_dates) = 1
),

block_dates AS (
    WITH numbered_rows AS (
        SELECT 
            account_id,
            date AS block_end_date,
            ROW_NUMBER() OVER(PARTITION BY account_id ORDER BY date) AS row_num
        FROM {{ ref('int_timeseries_combined') }}
        WHERE account_id NOT IN (SELECT account_id FROM churned_users)
    )

    SELECT 
        account_id,
        block_end_date,
        FALSE AS is_churned
    FROM numbered_rows
    WHERE mod(row_num, {{ var('span') }}) = 0
),

consolidated_end_dates AS (
    SELECT account_id, end_date, is_churned FROM churned_users
    UNION ALL
    SELECT account_id, block_end_date AS end_date, is_churned FROM block_dates
),

tenure AS (
    SELECT 
        account_id,
        MIN(valid_from) AS client_since
    FROM {{ ref('stg_awesometel__product_holdings') }}
    GROUP BY account_id
),

aggregated_features AS (
    SELECT
        c.account_id,
        c.end_date,
        c.is_churned,

        (c.end_date - GREATEST(c.end_date - 1 - {{ var('span') }}, t.client_since)) AS window_size_days,

        -- product related
        AVG(i.avg_product_price) AS avg_product_price,

        -- interaction related
        COUNT(i.avg_handling_time_s) AS interaction_count,

        -- subscription related
        AVG(i.num_subscriptions) AS avg_num_subscriptions,

        -- handling time related
        AVG(i.avg_handling_time_s) AS avg_handling_time,

        -- satisfaction related
        AVG(i.avg_customer_satisfaction_after_call) AS avg_satisfaction
        
    FROM consolidated_end_dates c
    LEFT JOIN {{ ref('int_timeseries_combined') }} i 
        ON c.account_id = i.account_id 
        AND i.date BETWEEN (c.end_date - 1 - {{ var('span') }}) AND c.end_date
    LEFT JOIN tenure t
        ON c.account_id = t.account_id
    WHERE i.date >= t.client_since
    GROUP BY c.account_id, c.end_date, c.is_churned, window_size_days
)

SELECT 

    f.account_id,
    f.end_date,
    f.is_churned,
    t.client_since,

    -- Features
    f.avg_product_price,
    f.interaction_count,
    f.avg_handling_time,
    f.avg_satisfaction,
    f.avg_num_subscriptions,
    (f.end_date - t.client_since) AS tenure,

    -- Account details
    a.language,
    a.gender,
    -- a.birthday,
    a.zip_code,
    a.payment_method,
    a.age_years,
    a.age_bin
FROM aggregated_features f
INNER JOIN {{ ref('stg_awesometel__accounts') }} a
ON f.account_id = a.id
INNER JOIN tenure t
ON t.account_id = a.id
WHERE (f.end_date - t.client_since) > 0

