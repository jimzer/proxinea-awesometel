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

master_with_products AS (
    SELECT
        m.account_id,
        m.date,
        ph.subscription_id,
        ph.product_family,
        ph.product_name,
        ph.product_price
    FROM 
        master m
    LEFT JOIN 
        {{ ref('stg_awesometel__product_holdings') }} ph 
    ON 
        m.account_id = ph.account_id
    AND 
        m.date BETWEEN ph.valid_from AND ph.valid_to
)



SELECT * FROM master_with_products