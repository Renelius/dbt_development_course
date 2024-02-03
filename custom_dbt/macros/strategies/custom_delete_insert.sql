{% macro greenplum__get_custom_delete_insert_strategy_sql(strategy_arg_dict) %}

    {% set target_relation = strategy_arg_dict['target_relation'] %}
    {% set temp_relation = strategy_arg_dict['temp_relation'] %}
    {% set unique_key = strategy_arg_dict['unique_key'] %}
    {% set dest_columns = strategy_arg_dict['dest_columns'] %}

    {% set columns_with_old_version_values = ['mt_insert_dt'] %}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set dest_cols_list = dest_columns | map(attribute="name") | list -%}

    create temporary table some_table as (
       select 
         {% for col in dest_cols_list %}
            {% if col in columns_with_old_version_values %}
            COALESCE(tgt.{{ col }}, src.{{ col }}) as {{ col }}
            {% else %}
            src.{{ col }} as {{ col }}
            {% endif %}
            {%- if not loop.last-%}, {%- endif -%}
         {% endfor %}
    
    from {{ temp_relation }} as src
    left join {{ target_relation }} as tgt on
                    {%- for key in unique_key %}
                        tgt.{{ key }} = src.{{ key }} {{ " and " if not loop.last }}
                    {%- endfor %}
    );

        delete from {{ target_relation }}
        using {{ temp_relation }}
        where (
                {% for key in unique_key %}
                    {{ temp_relation }}.{{ key }} = {{ target_relation }}.{{ key }}
                    {{ "and " if not loop.last }}
                {% endfor %}
        );


    insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from some_table
    )

{% endmacro %}