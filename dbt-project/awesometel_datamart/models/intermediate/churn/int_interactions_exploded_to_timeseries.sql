{% set start_result = run_query(get_start_date()) %}

{% if execute %}
{% set start_date = start_result.columns[0].values()[0] %}
{% endif %}


WITH

master AS (
    SELECT DISTINCT 
        account_id, 
        generate_series(DATE '{{ start_date }}', DATE '{{ var("end") }}', interval '1 day')::DATE as date
    FROM input.account_info
),

master_with_interactions AS (
    SELECT
        m.account_id,
        m.date,
        i.time_in_queue,
        i.handling_time_s,
        i.call_reason,
        i.customer_satisfaction_after_call
    FROM 
        master m
    LEFT JOIN 
        {{ ref('stg_awesometel__interactions') }} i
    ON 
        m.account_id = i.account_id AND m.date = i.date
)



SELECT * FROM master_with_interactions