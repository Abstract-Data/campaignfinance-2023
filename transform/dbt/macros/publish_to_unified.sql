{#
  The seam between dbt-owned `gold` and the ORM-owned `public.unified_*` tables that
  app/resolve reads. Resolve imports SQLModel classes bound to public.unified_* (with
  native PG enum types + the string committee_id FK), so it cannot read a `gold`
  schema unchanged — this load step is the adapter.

  Performance: the public.unified_* tables carry the Fix-7 partial unique indexes, so
  a row-by-row index maintenance during a multi-million-row insert dominates the cost.
  We DROP those indexes, bulk-insert, then RECREATE them — the recreate is a single
  set-based index build AND re-validates the dedup invariants (a duplicate would fail
  the unique index build, surfacing it loudly).

  Enum text uses the UPPERCASE member names (e.g. 'CONTRIBUTOR'::personrole). uuid /
  created_at / updated_at / amended / is_anonymous are supplied (SQLModel sets them
  Python-side). committee_types is seeded from the FILER committee types so the
  unified_committees.committee_type FK resolves.

  Run after `dbt build`:  uv run dbt run-operation publish_to_unified
#}
{% macro publish_to_unified() %}
    {% set gold = target.schema %}

    {% set delete_order = [
        'loan_guarantors',
        'unified_contributions', 'unified_expenditures', 'unified_loans', 'unified_debts',
        'unified_credits', 'unified_pledges', 'unified_travel', 'unified_assets',
        'unified_transaction_persons', 'unified_transactions', 'unified_entities',
        'unified_committees', 'unified_persons', 'unified_addresses'
    ] %}

    {% set drop_indexes = [
        "DROP INDEX IF EXISTS public.uix_persons_name_state",
        "DROP INDEX IF EXISTS public.uix_persons_org_state",
        "DROP INDEX IF EXISTS public.uix_addresses_city_state_zip_nostreet",
        "DROP INDEX IF EXISTS public.uix_addresses_full",
        "DROP INDEX IF EXISTS public.uix_txperson_txid_personid_role",
        "DROP INDEX IF EXISTS public.ix_transactions_source_id",
        "DROP INDEX IF EXISTS public.uix_entities_type_name_state"
    ] %}

    {% set create_indexes = [
        "CREATE UNIQUE INDEX IF NOT EXISTS uix_persons_name_state ON public.unified_persons (lower(first_name), lower(last_name), state_id) WHERE organization IS NULL AND first_name IS NOT NULL AND last_name IS NOT NULL",
        "CREATE UNIQUE INDEX IF NOT EXISTS uix_persons_org_state ON public.unified_persons (lower(organization), state_id) WHERE organization IS NOT NULL",
        "CREATE UNIQUE INDEX IF NOT EXISTS uix_addresses_city_state_zip_nostreet ON public.unified_addresses (lower(city), lower(state), zip_code) WHERE street_1 IS NULL",
        "CREATE UNIQUE INDEX IF NOT EXISTS uix_addresses_full ON public.unified_addresses (lower(street_1), lower(city), lower(state), zip_code) WHERE street_1 IS NOT NULL",
        "CREATE UNIQUE INDEX IF NOT EXISTS uix_txperson_txid_personid_role ON public.unified_transaction_persons (transaction_id, person_id, role)",
        "CREATE INDEX IF NOT EXISTS ix_transactions_source_id ON public.unified_transactions (transaction_id, committee_id) WHERE transaction_id IS NOT NULL",
        "CREATE UNIQUE INDEX IF NOT EXISTS uix_entities_type_name_state ON public.unified_entities (entity_type, normalized_name, state_id) WHERE state_id IS NOT NULL"
    ] %}

    {# committee_type is an FK to committee_types.code — seed any FILER types we have
       (placeholder title/description) so the committee insert's FK resolves. #}
    {% set seed_committee_types =
        "INSERT INTO public.committee_types (code, full_title, description)
         SELECT DISTINCT committee_type, committee_type, committee_type
         FROM " ~ gold ~ ".unified_committees
         WHERE committee_type IS NOT NULL
         ON CONFLICT (code) DO NOTHING" %}

    {% set inserts = [
        "INSERT INTO public.unified_addresses
            (id, uuid, street_1, street_2, city, state, zip_code, country, county, created_at, updated_at)
         SELECT id, gen_random_uuid()::text, street_1, street_2, city, state, zip_code, country, county, now(), now()
         FROM " ~ gold ~ ".unified_addresses",

        "INSERT INTO public.unified_persons
            (id, uuid, first_name, last_name, middle_name, suffix, organization, employer, occupation, job_title,
             person_type, address_id, state_id, created_at, updated_at)
         SELECT id, gen_random_uuid()::text, first_name, last_name, middle_name, suffix, organization, employer,
             occupation, job_title, person_type::persontype, address_id, state_id, now(), now()
         FROM " ~ gold ~ ".unified_persons",

        "INSERT INTO public.unified_committees
            (filer_id, uuid, name, committee_type, filer_status, address_id, state_id, created_at, updated_at)
         SELECT filer_id, gen_random_uuid()::text, name, committee_type, filer_status, address_id, state_id, now(), now()
         FROM " ~ gold ~ ".unified_committees",

        "INSERT INTO public.unified_entities
            (id, uuid, entity_type, name, normalized_name, person_id, committee_id, address_id, state_id, created_at, updated_at)
         SELECT id, gen_random_uuid()::text, entity_type::entitytype, name, normalized_name, person_id, committee_id,
             address_id, state_id, now(), now()
         FROM " ~ gold ~ ".unified_entities",

        "INSERT INTO public.unified_transactions
            (id, uuid, transaction_id, amount, transaction_date, description, transaction_type, committee_id,
             campaign_id, state_id, report_ident, amended, last_modified_at, created_at, updated_at)
         SELECT id, gen_random_uuid()::text, transaction_id, amount, transaction_date, description,
             transaction_type::transactiontype, committee_id, campaign_id, state_id, report_ident,
             false, now(), now(), now()
         FROM " ~ gold ~ ".unified_transactions",

        "INSERT INTO public.unified_transaction_persons
            (id, uuid, transaction_id, person_id, entity_id, state_id, role, amount, created_at, updated_at)
         SELECT id, gen_random_uuid()::text, transaction_id, person_id, entity_id, state_id,
             role::personrole, amount, now(), now()
         FROM " ~ gold ~ ".unified_transaction_persons",

        "INSERT INTO public.unified_contributions
            (id, uuid, transaction_id, contributor_entity_id, recipient_entity_id, state_id, amount, receipt_date,
             contribution_type, is_anonymous, description, created_at, updated_at)
         SELECT id, gen_random_uuid()::text, transaction_id, contributor_entity_id, recipient_entity_id, state_id,
             amount, receipt_date, contribution_type, false, description, now(), now()
         FROM " ~ gold ~ ".unified_contributions",

        "INSERT INTO public.unified_expenditures
            (id, uuid, transaction_id, payer_entity_id, payee_entity_id, state_id, amount, expenditure_date,
             expenditure_type, description, created_at, updated_at)
         SELECT id, gen_random_uuid()::text, transaction_id, payer_entity_id, payee_entity_id, state_id, amount,
             expenditure_date, expenditure_type, description, now(), now()
         FROM " ~ gold ~ ".unified_expenditures",

        "INSERT INTO public.unified_loans
            (id, uuid, transaction_id, lender_entity_id, borrower_entity_id, state_id, amount, loan_date,
             is_forgiven, collateral_flag, financial_institution, payment_made, created_at, updated_at)
         SELECT id, gen_random_uuid()::text, transaction_id, lender_entity_id, borrower_entity_id, state_id,
             amount, loan_date, false, false, false, false, now(), now()
         FROM " ~ gold ~ ".unified_loans",

        "INSERT INTO public.unified_debts
            (id, uuid, transaction_id, creditor_entity_id, debtor_entity_id, state_id, amount, debt_date,
             description, is_guaranteed, is_paid, created_at, updated_at)
         SELECT id, gen_random_uuid()::text, transaction_id, creditor_entity_id, debtor_entity_id, state_id,
             amount, debt_date, description, false, false, now(), now()
         FROM " ~ gold ~ ".unified_debts",

        "INSERT INTO public.unified_credits
            (id, uuid, transaction_id, payor_entity_id, recipient_entity_id, state_id, amount, credit_date,
             description, created_at, updated_at)
         SELECT id, gen_random_uuid()::text, transaction_id, payor_entity_id, recipient_entity_id, state_id,
             amount, credit_date, description, now(), now()
         FROM " ~ gold ~ ".unified_credits",

        "INSERT INTO public.unified_pledges
            (id, uuid, transaction_id, pledgor_entity_id, recipient_entity_id, state_id, amount, pledge_date,
             is_fulfilled, description, created_at, updated_at)
         SELECT id, gen_random_uuid()::text, transaction_id, pledgor_entity_id, recipient_entity_id, state_id,
             amount, pledge_date, false, description, now(), now()
         FROM " ~ gold ~ ".unified_pledges",

        "INSERT INTO public.unified_travel
            (id, uuid, transaction_id, traveler_person_id, state_id, parent_transaction_type,
             parent_transaction_id, parent_amount, amount, travel_date, travel_purpose, created_at, updated_at)
         SELECT id, gen_random_uuid()::text, transaction_id, traveler_person_id, state_id, parent_transaction_type,
             parent_transaction_id, parent_amount, amount, travel_date, travel_purpose, now(), now()
         FROM " ~ gold ~ ".unified_travel",

        "INSERT INTO public.unified_assets
            (id, uuid, transaction_id, committee_id, state_id, description, is_disposed, created_at, updated_at)
         SELECT id, gen_random_uuid()::text, transaction_id, committee_id, state_id, description, false, now(), now()
         FROM " ~ gold ~ ".unified_assets",

        "INSERT INTO public.loan_guarantors
            (id, uuid, loan_id, debt_id, position, entity_id, person_type, organization, last_name, first_name,
             suffix, prefix, city, state_code, county, country, postal_code, region, created_at)
         SELECT id, gen_random_uuid()::text, loan_id, debt_id, position, entity_id, person_type, organization,
             last_name, first_name, suffix, prefix, city, state_code, county, country, postal_code, region, now()
         FROM " ~ gold ~ ".unified_loan_guarantors"
    ] %}

    {% if execute %}
        {% for tbl in delete_order %}
            {% do run_query("DELETE FROM public." ~ tbl) %}
        {% endfor %}
        {% for stmt in drop_indexes %}
            {% do run_query(stmt) %}
        {% endfor %}
        {% do run_query(seed_committee_types) %}
        {{ log("publish_to_unified: cleared tables, dropped dedup indexes, seeded committee_types", info=True) }}
        {% for stmt in inserts %}
            {% do run_query(stmt) %}
        {% endfor %}
        {{ log("publish_to_unified: bulk-inserted gold -> public; rebuilding indexes (revalidates dedup)", info=True) }}
        {% for stmt in create_indexes %}
            {% do run_query(stmt) %}
        {% endfor %}
        {{ log("publish_to_unified: done (indexes rebuilt)", info=True) }}
    {% endif %}
{% endmacro %}
