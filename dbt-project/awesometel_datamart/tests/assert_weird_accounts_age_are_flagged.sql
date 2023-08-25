SELECT 
    id
FROM {{ ref('stg_awesometel__accounts') }}
WHERE age_years > 110 AND is_age_invalid = FALSE