{% macro get_strategy(strategy, strategy_arg_dict) %}
  {% if strategy == 'append_if_not_exists' %}
    {% do return(greenplum__get_append_if_not_exists_strategy_sql(strategy_arg_dict)) %}
  {% else %}
    {% do exceptions.raise_compiler_error('invalid strategy: ' ~ strategy) %}
  {% endif %}
{% endmacro %}



{% materialization custom_incremental, default -%}

  -- relations
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set temp_relation = make_temp_relation(target_relation)-%}

  -- configs
  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (should_full_refresh()  or existing_relation.is_view) -%}



  {% if existing_relation is none %}
      {% set build_sql = get_create_table_as_sql(False, target_relation, sql) %}
  {% elif full_refresh_mode %}
       {% do adapter.drop_relation(target_relation) %}
      {% set build_sql = get_create_table_as_sql(False, target_relation, sql) %}
  {% else %}
    {% do run_query(get_create_table_as_sql(True, temp_relation, sql)) %}

    {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% set incremental_strategy = config.get('incremental_strategy') or 'default' %}

    {% set strategy_arg_dict = ({'target_relation': target_relation, 'temp_relation': temp_relation, 'unique_key': unique_key, 'dest_columns': dest_columns }) %}
    {% set build_sql = get_strategy(incremental_strategy, strategy_arg_dict) %}

  {% endif %}

  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}


  -- `COMMIT` happens here
  {% do adapter.commit() %}


  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
