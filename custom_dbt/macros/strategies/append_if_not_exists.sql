{% macro greenplum__get_append_if_not_exists_strategy_sql(strategy_arg_dict) %}

    {%- set dest_cols_csv = strategy_arg_dict["dest_columns"] -%}
    {%- set dest_cols_csv =  dest_cols_csv | map(attribute="name") | join(' ,') -%}

    insert into {{ strategy_arg_dict["target_relation"] }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ strategy_arg_dict["temp_relation"] }} src
    
    where 
        not exists (
        select 1 
        from {{ strategy_arg_dict["target_relation"] }} as tgt
        where 
        {% for key in strategy_arg_dict["unique_key"] %}
        tgt.{{ key }} = src.{{ key }} {% if not loop.last %}and {% endif %}
        {% endfor %}
        )
    )
{% endmacro %}