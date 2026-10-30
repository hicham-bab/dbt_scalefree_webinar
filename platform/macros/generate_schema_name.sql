{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if node and ('-on-run-start-' in node.name or '-on-run-end-' in node.name) -%}
        {# Ignore the 'on-run-[start | end]' hooks, they can't execute queries against a specific target. #}
        {{ return(target.schema) }}
    {%- endif -%}

    {%- if custom_schema_name is none -%}
        {%- if node['resource_type'] == 'test' and not execute -%}
            {#- dbt schema tests are run as models which require a schema name, however the 'custom_schema_name'     -#}
            {#- from the model configuration is not passed to this macro.  In this case, return the default_schema   -#}
            {#- (which will be the same for different models that are targeted at different schemas).  This doesn't  -#}
            {#- seem to cause anything to break                                                                      -#}
            {{ return(target.schema) }}
        {%- endif -%}
        {{ exceptions.raise_compiler_error("The model '" ~ node['name'] ~ "' does not have a custom schema name set '" ~ node['test_metadata'] ~ "'" ) }}
    {%- endif -%}

    {%- if env_var('DBT_DATABASE_PREFIX') == 'PROD' -%}
    {# for UAT and PROD Jobs #}

        {{ custom_schema_name | trim }}

    {%- else -%}
    {# for CICD pipeline or developers #}

        {{ default_schema }}__{{ custom_schema_name | trim }}

    {%-endif -%}

{%- endmacro %}