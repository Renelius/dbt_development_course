{% set fields_string %}
    custom_id int,
    mt_insert_dt timestamp(0) without time zone,
    mt_update_dt timestamp(0) without time zone
{% endset %}

{% set raw_partition %}
    PARTITION BY RANGE (mt_update_dt)
    (
        START ('2021-01-01'::timestamp) INCLUSIVE
        END ('2023-01-01'::timestamp) EXCLUSIVE
        EVERY (INTERVAL '1 month'),
        DEFAULT PARTITION extra
    );
{% endset %}

{{ config(
    fields_string=fields_string,
    raw_partition=raw_partition,
    distributed_by='custom_id',
    materialized='custom_incremental',
    incremental_strategy='append_if_not_exists',
    unique_key=['custom_id'],
    table_owner='test'
    ) 
}}

with source_data as (
    {% for i in range(5) %}
    select {{ i }} as custom_id
    {% if not loop.last%}union{% endif %}
    {% endfor %}

)

select 
    custom_id,
    current_timestamp as mt_insert_dt,
    current_timestamp as mt_update_dt
from source_data

