{#
    Overrides dbt's default schema naming (<target_schema>_<custom_schema>,
    e.g. "public_stg") to use the custom schema name exactly as given in
    dbt_project.yml (stg/int/marts) — this repo's Postgres already has those
    three schemas provisioned, distinct from `public` where bronze_* lives.
    Standard dbt override, not a project-specific trick — see dbt's own docs
    on customizing schema generation.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
