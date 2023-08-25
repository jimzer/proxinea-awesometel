WITH

source AS (

    SELECT * FROM {{ source('raw','interactions') }}

),

renamed AS (

    SELECT
        -- ids
        account_id,

        -- attributes
        date,
        time_in_queue::SMALLINT,
        handling_time_s::SMALLINT,
        call_reason,
        customer_satisfaction_after_call::SMALLINT

    FROM {{ ref('inp_awesometel__interactions') }}

)

SELECT * FROM renamed