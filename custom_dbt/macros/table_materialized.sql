{% materialization custom_table, default %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') %}

  {% set table_owner = config.get('table_owner') %}

  {% if existing_relation is not none %}
     {% do drop_relation(target_relation) %}
  {% endif %}

  -- build model
  {% call statement('main') -%}
    {{ get_create_table_as_sql(False, target_relation, sql) }}
  {%- endcall %}


  {% do apply_custom_grants(target_relation, table_owner) %}

  {{ adapter.commit() }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}