{% macro greenplum__generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}  
    {%- if custom_schema_name is none -%}

        {{ node.name.split("_dbt_") | first }}

    {%- else -%}

        {{ custom_schema_name | trim }} 

    {%- endif -%}

{%- endmacro %}


{% macro greenplum__generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- if custom_alias_name is none -%}

        {{ node.name.split('_dbt_')[1] }}

    {%- else -%}

        {{ custom_alias_name | trim }}

    {%- endif -%}

{%- endmacro %}


{% macro greenplum__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}

{% macro apply_custom_grants(relation, table_owner) -%}
  {% call statement('set_table_owner', auto_begin=False) -%}
    alter table {{ relation }} owner to {{ table_owner }}
  {%- endcall %}
{% endmacro %}


{% macro greenplum__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create schema if not exists {{ relation.schema }};
    alter schema {{ relation.schema }} owner to {{ var("schema_owner") }};
  {%- endcall -%}
{% endmacro %}