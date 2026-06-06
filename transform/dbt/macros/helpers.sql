{#
  Shared SQL helpers that mirror the Python ingest layer 1:1 so a dbt dedup and a
  builder dedup collapse to the same grain.
#}

{# Trimmed, lower-cased, empty-as-NULL — the case-insensitive dedup primitive. #}
{% macro cf_lower(col) -%}
    nullif(lower(trim({{ col }})), '')
{%- endmacro %}

{# Trimmed, empty-as-NULL passthrough (no case fold) for stored display values. #}
{% macro cf_clean(col) -%}
    nullif(trim({{ col }}), '')
{%- endmacro %}

{#
  Reproduces app/core/value_objects.py::normalize_entity_name exactly:
  lower(strip) -> collapse every [^a-z0-9]+ run to a single space -> trim.
  Used for unified_entities.normalized_name and the entity dedup grain.
#}
{% macro cf_normalize_name(col) -%}
    nullif(trim(regexp_replace(lower(trim(coalesce({{ col }}, ''))), '[^a-z0-9]+', ' ', 'g')), '')
{%- endmacro %}

{#
  Surrogate-id registry body. Assigns a stable INTEGER id per natural-key hash
  (`nk`). New nks get max(existing id) + dense order; existing nks keep their id,
  so ids are append-stable across incremental runs. Pair with:
    {{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='nk') }}
#}
{# Safe numeric cast: returns NULL on non-numeric text (raw silver is all-TEXT). #}
{% macro cf_safe_numeric(col) -%}
    case when nullif(trim({{ col }}), '') ~ '^-?[0-9]+(\.[0-9]+)?$' then trim({{ col }})::numeric end
{%- endmacro %}

{# Safe date cast: handles raw TEC 'YYYYMMDD' and ISO 'YYYY-MM-DD'; NULL otherwise. #}
{% macro cf_safe_date(col) -%}
    case
        when trim({{ col }}) ~ '^[0-9]{8}$' then to_date(trim({{ col }}), 'YYYYMMDD')
        when trim({{ col }}) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' then substr(trim({{ col }}), 1, 10)::date
    end
{%- endmacro %}

{#
  Shared party-key SELECT fragment. Given a row with the canonical party columns
  (organization, first_name, middle_name, last_name, suffix, street_1, city, state,
  zip_code, state_id, record_type, source_transaction_id), emit person_type +
  transaction_nk + person_nk + address_nk computed IDENTICALLY everywhere, so every
  record type (and guarantors) dedupes into the same shared dimensions. The person key
  branches on organization presence to match the DB partial unique indexes exactly.
#}
{% macro cf_party_key_columns() %}
    case
        when organization is not null then 'ORGANIZATION'
        when first_name is not null and last_name is not null then 'INDIVIDUAL'
        else 'UNKNOWN'
    end as person_type,
    {{ dbt_utils.generate_surrogate_key(['record_type', 'source_transaction_id']) }} as transaction_nk,
    {{ dbt_utils.generate_surrogate_key([
        "case when organization is not null then 'org' else 'ind' end",
        "nullif(lower(trim(organization)), '')",
        "case when organization is null then nullif(lower(trim(first_name)), '') end",
        "case when organization is null then nullif(lower(trim(last_name)), '') end",
        "state_id"
    ]) }} as person_nk,
    case
        when ((street_1 is not null)::int + (city is not null)::int
              + (state is not null)::int + (zip_code is not null)::int) >= 2
        then {{ dbt_utils.generate_surrogate_key([
            "(street_1 is not null)",
            "nullif(lower(trim(street_1)), '')",
            "nullif(lower(trim(city)), '')",
            "nullif(lower(trim(state)), '')",
            "zip_code"
        ]) }}
    end as address_nk
{% endmacro %}


{% macro cf_surrogate_ids(dim_relation) %}
with source_nks as (
    select distinct nk from {{ dim_relation }} where nk is not null
)
{% if is_incremental() %}
, existing as (
    select nk, id from {{ this }}
)
, new_nks as (
    select s.nk
    from source_nks s
    left join existing e using (nk)
    where e.nk is null
)
, base as (
    select coalesce(max(id), 0) as max_id from {{ this }}
)
select nk, id from existing
union all
select nk, (select max_id from base) + row_number() over (order by nk) as id
from new_nks
{% else %}
select nk, row_number() over (order by nk) as id from source_nks
{% endif %}
{% endmacro %}
