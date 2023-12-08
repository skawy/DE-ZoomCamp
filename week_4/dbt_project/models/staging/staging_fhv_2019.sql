{{ config (materialized = 'view')}}

select
    -- identifiers
    cast(int64_field_0 as integer) as vendorid,
    dispatching_base_num,
    cast(PUlocationID as integer) as  pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropOff_datetime,
    
    -- trip info
    SR_Flag,
    Affiliated_base_number,

    



FROM {{ source("resolute-choir-403411",'fhv_2019')}}

-- dbt build --m <model.sql> --var 'is_test_run: false'
-- {% if var('is_test_run', default=true) %}

--   limit 100

-- {% endif %}
