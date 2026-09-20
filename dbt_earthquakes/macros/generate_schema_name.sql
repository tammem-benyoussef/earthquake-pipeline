{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if target.name == 'dev' -%}
        dev_{{ custom_schema_name if custom_schema_name else default_schema }}
    {%- else -%}
        {{ custom_schema_name if custom_schema_name else default_schema }}
    {%- endif -%}
{%- endmacro %}