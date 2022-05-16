{{
  config(
    materialized='view'
  )
}}

with requests_by_borough as (

    select

        count(*) as num_requests,
        borough

    from {{ source('nyc_open_data', '311_service_requests') }}

    group by borough

)

select * from requests_by_borough