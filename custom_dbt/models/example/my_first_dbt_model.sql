

{{ config(
    materialized='custom_table',
    table_owner='test'
    ) 
}}

with source_data as (

    select 1 as id
    union all
    select null as id

)

select *
from source_data

