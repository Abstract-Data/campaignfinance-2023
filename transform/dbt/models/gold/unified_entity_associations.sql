{#
  Entity-to-entity relationships derived set-based from the unified layer — the
  linking layer that was previously empty. Deduped to one row per
  (source, target, association_type):
    DONOR_TO     contributor -> recipient committee   (from contributions)
    VENDOR_FOR   payee       -> payer committee        (from expenditures)
    EMPLOYED_BY  person      -> employer org           (employer text normalized to an entity)
    TREASURER_OF treasurer   -> committee              (from FILER officers)
#}
with donor as (
    select contributor_entity_id as source_entity_id, recipient_entity_id as target_entity_id,
           'DONOR_TO' as association_type
    from {{ ref('unified_contributions') }}
),

vendor as (
    select payee_entity_id as source_entity_id, payer_entity_id as target_entity_id,
           'VENDOR_FOR' as association_type
    from {{ ref('unified_expenditures') }}
),

employed as (
    select
        src_ek.id as source_entity_id,
        emp_ek.id as target_entity_id,
        'EMPLOYED_BY' as association_type
    from {{ ref('int_parties') }} p
    join {{ ref('dim_persons') }} src_dp on src_dp.nk = p.person_nk
    join {{ ref('entity_keys') }} src_ek on src_ek.nk = src_dp.entity_nk
    join {{ ref('dim_persons') }} emp_dp
        on emp_dp.nk = {{ dbt_utils.generate_surrogate_key([
            "'org'", "nullif(lower(trim(p.employer)), '')",
            "cast(null as text)", "cast(null as text)", "p.state_id"]) }}
    join {{ ref('entity_keys') }} emp_ek on emp_ek.nk = emp_dp.entity_nk
    where p.employer is not null and p.occurrence_type in ('party', 'guarantor')
),

treasurer as (
    select
        off_ek.id as source_entity_id,
        c_ek.id as target_entity_id,
        'TREASURER_OF' as association_type
    from {{ ref('int_parties') }} p
    join {{ ref('dim_persons') }} off_dp on off_dp.nk = p.person_nk
    join {{ ref('entity_keys') }} off_ek on off_ek.nk = off_dp.entity_nk
    join {{ ref('int_committees') }} c on c.filer_id = p.committee_filer_id
    join {{ ref('entity_keys') }} c_ek on c_ek.nk = c.entity_nk
    where p.occurrence_type = 'officer'
),

deduped as (
    select distinct source_entity_id, target_entity_id, association_type
    from (
        select * from donor
        union all select * from vendor
        union all select * from employed
        union all select * from treasurer
    ) a
    where source_entity_id is not null and target_entity_id is not null
      and source_entity_id <> target_entity_id
)

select
    row_number() over (order by source_entity_id, target_entity_id, association_type) as id,
    source_entity_id,
    target_entity_id,
    association_type
from deduped
