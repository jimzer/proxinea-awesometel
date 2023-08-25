{% macro get_start_date() %}
  SELECT MIN(valid_from)
  FROM {{ ref('stg_awesometel__product_holdings') }}
{% endmacro %}