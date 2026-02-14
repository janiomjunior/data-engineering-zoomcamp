with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        -- identifiers
        cast(dispatching_base_num as string) as dispatching_base_num,

        -- location ids (rename to project convention)
        {{ safe_cast('pulocationid', 'integer') }} as pickup_location_id,
        {{ safe_cast('dolocationid', 'integer') }} as dropoff_location_id,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,

        -- additional fhv fields
        cast(sr_flag as string) as sr_flag,
        cast(affiliated_base_number as string) as affiliated_base_number

    from source

    -- requirement: filter out null dispatching_base_num
    where dispatching_base_num is not null
)

select * from renamed
