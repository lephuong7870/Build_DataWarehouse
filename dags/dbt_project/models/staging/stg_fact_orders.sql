{{
  config(
    materialized='view'
  )
}}

SELECT
  *
FROM
  {{ source('project_db', 'fact_orders') }}