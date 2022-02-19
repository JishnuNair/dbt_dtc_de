{{ config(materialized='view') }}

select
--identifiers
{{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
cast(PULocationID as integer) as pickup_locationid,
cast(DOLocationID as integer) as dropoff_locationid,
dispatching_base_num,

--timestamps
cast(pickup_datetime as timestamp) as pickup_datetime,
cast(dropoff_datetime as timestamp) as dropoff_datetime,

SR_Flag as shared_ride_flag

from
{{ source('staging','fhv_tripdata') }}

{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}