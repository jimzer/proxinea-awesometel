WITH

churned AS (
    SELECT account_id
    FROM {{ ref('churns' )}}
    WHERE is_churned = TRUE
),

non_churned AS (
    SELECT account_id
    FROM {{ ref('churns' )}}
    WHERE is_churned = FALSE
)

SELECT account_id FROM non_churned WHERE account_id IN (SELECT account_id FROM churned)