{{ config(
    materialized='table',
    lf_tags_config={
          'enabled': true,
          'drop_existing' : True,
          'tags_database': {
            'sensitive' : 'no'
          },
          'tags_table': {
            'sensitive': 'no'         }, 
          'tags_columns': {
            'sensitive': {
              'yes': ['customer_id', 'customer_lifetime_value', 'dt']}}}, 
    lf_grants={
        'data_cell_filters': {
            'enabled': True,
            'drop_existing' : True,
            'filters': {
                'test_dbt_glue': {
                    'row_filter': 'customer_lifetime_value>15',
                    'principals': ['arn:aws:iam::<ANY_PRINCIPAL>'], 
                    'excluded_column_names': ['first_name']
                }}}}
) }}

with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('stg_payments') }}

),

customer_orders as (

        select
        customer_id,
        min(order_date) as first_order,
        max(order_date) as most_recent_order,
        count(order_id) as number_of_orders
    from orders

    group by 1

),

customer_payments as (

    select
        orders.customer_id,
        sum(amount) as total_amount

    from payments

    left join orders using (order_id)

    group by 1

),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order,
        customer_orders.most_recent_order,
        customer_orders.number_of_orders,
        customer_payments.total_amount as customer_lifetime_value,
        current_date() as dt
        
    from customers

    left join customer_orders using (customer_id)

    left join customer_payments using (customer_id)

)

select * from final order by customer_id asc