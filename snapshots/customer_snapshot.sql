{% snapshot demosnapshot %}

{{
    config(
        strategy='timestamp',
        target_schema='jaffle_shop_schema',
        unique_key='customer_id',
        updated_at='dt',
        file_format='iceberg'
    )
}}

select * from {{ ref('customers') }}

{% endsnapshot %}
