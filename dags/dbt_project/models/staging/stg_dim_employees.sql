{{
  config(
    materialized='view'
  )
}}

SELECT
  *
FROM
  {{ source('project_db', 'dim_employees') }}