{% macro generate_database_name(custom_database_name=none, node=none) -%}

    {%- set default_database = target.database -%}

    {%- if node and ('-on-run-start-' in node.name or '-on-run-end-' in node.name) -%}
        {# Ignore the 'on-run-[start | end]' hooks, they can't execute queries against a specific target. #}
        {{ return(target.database) }}
    {%- endif -%}

    {%- if custom_database_name is none -%}
        {%- if node['resource_type'] == 'test' and not execute -%}
            {# dbt schema tests are run as models which require a schema name, however the 'custom_database_name'    #}
            {# from the model configuration is not passed to this macro.  In this case, return the target database   #}
            {# (which will be the same for different models that are targeted at different databases).  This doesn't #}
            {# seem to cause anything to break.                                                                      #}
            {{ return(target.database) }}
        {%- endif -%}
        {{ exceptions.raise_compiler_error("The model '" ~ node['name'] ~ "' does not have a custom database name set '" ~ node['test_metadata'] ~ "'" ) }}
    {%- endif -%}

    {{ env_var('DBT_DATABASE_PREFIX') }}__{{ custom_database_name | trim }}

{%- endmacro %}