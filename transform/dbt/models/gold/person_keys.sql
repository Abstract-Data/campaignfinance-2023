{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='nk') }}
{{ cf_surrogate_ids(ref('dim_persons')) }}
