with source as (

    select * from {{ source('data_source', 'raw_customers') }}

),

renamed as (

    select 
        id as customer_id,
        first_name,
        last_name
    
    from source

    group by 1, 2, 3

)

select * from renamed
