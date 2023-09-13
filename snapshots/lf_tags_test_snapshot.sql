{% snapshot lftagssnap %}

{{
    config(
        strategy='timestamp',
        target_schema='dbt_output_new',
        unique_key='customer_id',
        updated_at='dt',
        file_format='iceberg',
        lf_tags_config={
          'enabled': true,
          'tags': {
            'group': 'developer'          }
          }
    )
}}

select * from {{ ref('customers') }}

{% endsnapshot %}


{% snapshot demo_snap %}
{{
  config(
    target_schema='demo_db',
    file_format='iceberg',
    unique_key= " firstname || '-' || lastname",
    strategy='check',
    check_cols=['age'
                ,'DOB'
    ],
  )
}}
SELECT
  {{ cleanse_text_field('student', 'firstname ') }} AS firstname ,
  {{ cleanse_text_field('student', 'lastname') }} AS lastname,
 {{ cleanse_text_field('student', 'age') }} AS age,
  TO_DATE({{ ref('student') }}.DOB , 'mm/dd/yyyy') AS DOB
FROM {{ ref('student') }}


{% endsnapshot %}