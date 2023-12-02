{{ config (materialized = 'view')}}

select * from {{ source('staging', 'dezoomcamp.green')}} 
limit 100