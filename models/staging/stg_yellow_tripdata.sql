

select * from {{
    source('staging', 'yellow_tripdata')
}}
limit 100
