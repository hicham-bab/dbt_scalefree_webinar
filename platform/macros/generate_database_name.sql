{% macro generate_database_name(custom_database_name, node) -%}
    {%- set default_database = target.database -%}
    {%- if custom_database_name is not none -%}
        {{ custom_database_name | trim }}
    {%- else -%}
        {{ env_var('DBT_TARGET_DB', default_database) }}
    {%- endif -%}
{%- endmacro %}
