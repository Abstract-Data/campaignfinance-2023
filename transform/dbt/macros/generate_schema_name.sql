{#
  Force every model into the target schema (gold) verbatim — no `gold_<custom>`
  prefixing. Gold models are published into public.unified_* by publish_to_unified.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ target.schema }}
{%- endmacro %}
