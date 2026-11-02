{#
  Use the custom schema name as-is (core, staging, finance...) instead of dbt's
  default <target_schema>_<custom_schema>. Each lab attendee gets their own DEV
  database, so plain schema names do not collide, and they read better in
  Snowflake, in the Context Layer and in agent answers.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
