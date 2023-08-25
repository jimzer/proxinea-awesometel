WITH

source AS (

    SELECT * FROM {{ source('raw','product_holdings') }}

),

renamed AS (

    SELECT
        -- ids
        account_id::BIGINT,
        subscription_id::BIGINT,

        -- attributes
        product_family,
        product_name,
        product_price::NUMERIC,
        valid_from::DATE,
        valid_to::DATE

    FROM {{ ref('inp_awesometel__product_holdings') }}

    -- avoid invalid ranges
    WHERE valid_from <= valid_to

)

-- remove duplicates
{{ dbt_utils.deduplicate(
    relation="renamed",
    partition_by='account_id, subscription_id, product_family, product_name, product_price, valid_from, valid_to',
    order_by="subscription_id",
   )
}}