{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- set env_name = env_var('DBT_ENV_NAME', 'DEV') -%}
    {%- if custom_schema_name is not none and env_name in ['PRD', 'STG'] -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}
    {%- endif -%}
{%- endmacro %}
