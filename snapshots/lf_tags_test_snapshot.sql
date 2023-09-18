{% snapshot lftagssnap %}

{{
    config(
        strategy='timestamp',
        target_schema='jaffle_shop_schema',
        unique_key='customer_id',
        updated_at='dt',
        file_format='iceberg',
        lf_tags_config={
          'enabled': true,
          'tags': {
            'sensitive': 'no'          }
          }
    )
}}

select * from {{ ref('customers') }}

{% endsnapshot %}