{{
  config(
    materialized='view'
  )
}}

SELECT
  *
FROM
  {{ source('project_db', 'fact_order_details') }}