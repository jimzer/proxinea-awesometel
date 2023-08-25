WITH

renamed AS (

    SELECT
        -- ids
        account_id as id,

        -- attributes
        CASE
            WHEN LOWER(language) = 'nan' THEN NULL
            ELSE LOWER(language) 
        END language,
        gender,
        birthday,
        zip_code,
        payment_method,

        -- calculated attributes
        EXTRACT(YEAR FROM age(birthday))::INT AS age_years,
        width_bucket(EXTRACT(YEAR FROM age(birthday))::INT, 0, 100, 10) AS age_bucket,
        (width_bucket(EXTRACT(YEAR FROM age(birthday))::INT, 0, 100, 10) - 1) * 10 || '-' || width_bucket(EXTRACT(YEAR FROM age(birthday))::INT, 0, 100, 10) * 10 AS age_bin,
        (EXTRACT(YEAR FROM age(birthday))::INT > 110) AS is_age_invalid

    FROM {{ ref('inp_awesometel__accounts') }}

)

SELECT * FROM renamed