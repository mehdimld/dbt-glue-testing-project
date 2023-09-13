with source as (

    select * from {{ source('data_source', 'raw_orders') }}

),

renamed as (

    select 
        id as order_id,
        user_id as customer_id,
        order_date,
        status

    from source

    group by 1, 2, 3, 4
)

select * from renamed
