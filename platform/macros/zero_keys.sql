{%- macro unknown_key() -%}

{%- set hash = datavault4dbt.hash_method() -%}
{%- set hash_dtype = var('datavault4dbt.hash_datatype', 'STRING') -%}

{%- set hash_default_values = fromjson(datavault4dbt.hash_default_values(hash_function=hash,hash_datatype=hash_dtype)) -%}
{%- set unknown_key = hash_default_values['unknown_key'] -%}


{%- set unknown_key = datavault4dbt.as_constant(column_str=unknown_key) -%}

{{ return(unknown_key) }}

{%- endmacro -%}


{%- macro error_key() -%}

{%- set hash = datavault4dbt.hash_method() -%}
{%- set hash_dtype = var('datavault4dbt.hash_datatype', 'STRING') -%}

{%- set hash_default_values = fromjson(datavault4dbt.hash_default_values(hash_function=hash,hash_datatype=hash_dtype)) -%}
{%- set error_key = hash_default_values['error_key'] -%}


{%- set error_key = datavault4dbt.as_constant(column_str=error_key) -%}

{{ return(error_key) }}

{%- endmacro -%}