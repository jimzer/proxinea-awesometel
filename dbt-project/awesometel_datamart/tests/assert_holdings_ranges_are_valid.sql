SELECT 
    subscription_id 
FROM {{ ref('stg_awesometel__product_holdings') }}
WHERE valid_from > valid_to
