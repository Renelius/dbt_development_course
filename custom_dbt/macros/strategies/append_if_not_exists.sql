{% macro greenplum__get_append_if_not_exists_strategy_sql(strategy_arg_dict) %}

    {% set dest_col_str = strategy_arg_dict["dest_colums"] | map(attribute="name") | join(", ") %}  
    {{ log("LOG dest_col_str " ~ dest_col_str, info=True) }}
    insert into {{ strategy_arg_dict["target_relation"] }} ({{ dest_col_str }})
    (
        select {{ dest_cols_str }}
        from {{ strategy_arg_dict["temp_relation"] }} as src
        where
            not exists(
                select 1
                from {{ strategy_arg_dict["target_relation"] }} as tgt
                where
                    {%- for key in strategy_arg_dict["unique_key"] %}
                        tgt.{{ key }} = src.{{ key }} {{ " and " if not loop.last }}
                    {%- endfor %}
            )
    )

{% endmacro %}