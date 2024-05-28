{{
    config(
        materialized='view'
    )
}}

/*
with all_trips as (
  select *,
    -- set row number per partition. partitions are set by similar vendor and lpep_pickup_datetime
    -- explanation here https://www.youtube.com/watch?v=ueVy2N54lyc&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&t=33m25s
    row_number() over(partition by "VendorID", tpep_pickup_datetime) as rn
  from {{source('staging', 'taxi_trips')}}
  where "VendorID" is not null 
)
  select
      -- identifiers
      {{ dbt_utils.generate_surrogate_key(['VendorID', 'tpep_pickup_datetime']) }} as tripid,
      {{ dbt.safe_cast("VendorID", api.Column.translate_type("integer")) }} as vendorid,
      {{ dbt.safe_cast("RatecodeID", api.Column.translate_type("integer")) }} as ratecodeid,
      {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationID,
      {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationID,
      
      -- timestamps
      cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
      cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
      
      -- trip info
      store_and_fwd_flag,
      {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} as passenger_count,
      cast(trip_distance as numeric) as trip_distance,
      -- {{ dbt.safe_cast("trip_type", api.Column.translate_type("integer")) }} as trip_type,

      -- payment info
      cast(fare_amount as numeric) as fare_amount,
      cast(extra as numeric) as extra,
      cast(mta_tax as numeric) as mta_tax,
      cast(tip_amount as numeric) as tip_amount,
      cast(tolls_amount as numeric) as tolls_amount,
      -- cast(ehail_fee as numeric) as ehail_fee,
      cast(improvement_surcharge as numeric) as improvement_surcharge,
      cast(total_amount as numeric) as total_amount,
      coalesce({{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }},0) as payment_type,
      {{ get_payment_type_description("payment_type") }} as payment_type_description
  from all_trips 
  where rn = 1
*/

-- had to comment out lines above. dbt removing the double quotes on column names. Column names in sql automatically lower cased when no double qoutes 
-- lesson learned when loading from csv data, turn column names into lower case

with all_trips as (
  select *,
    -- set row number per partition. partitions are set by similar vendor and lpep_pickup_datetime
    -- explanation here https://www.youtube.com/watch?v=ueVy2N54lyc&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&t=33m25s
    row_number() over(partition by "VendorID", tpep_pickup_datetime) as rn
  from "taxi"."public"."taxi_trips"
  where "VendorID" is not null 
)
  select
      -- identifiers
      md5(cast(coalesce(cast("VendorID" as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(tpep_pickup_datetime as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as tripid,
      
    
    cast("VendorID" as integer)
 as vendorid,
      
    
    cast("RatecodeID" as integer)
 as ratecodeid,
      
    
    cast("PULocationID" as integer)
 as pickup_locationID,
      
    
    cast("DOLocationID" as integer)
 as dropoff_locationID,
      
      -- timestamps
      cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
      cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
      
      -- trip info
      store_and_fwd_flag,
      
    
    cast(passenger_count as integer)
 as passenger_count,
      cast(trip_distance as numeric) as trip_distance,
      -- 

      -- payment info
      cast(fare_amount as numeric) as fare_amount,
      cast(extra as numeric) as extra,
      cast(mta_tax as numeric) as mta_tax,
      cast(tip_amount as numeric) as tip_amount,
      cast(tolls_amount as numeric) as tolls_amount,
      -- cast(ehail_fee as numeric) as ehail_fee,
      cast(improvement_surcharge as numeric) as improvement_surcharge,
      cast(total_amount as numeric) as total_amount,
      coalesce(
    
    cast(payment_type as integer)
,0) as payment_type,
      case 
    
    cast(payment_type as integer)
  
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
        else 'EMPTY'
    end as payment_type_description
  from all_trips 
  where rn = 1