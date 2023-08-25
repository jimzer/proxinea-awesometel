SELECT 
    account_id
FROM {{ ref('churns') }}
WHERE tenure < 0