-- db_bloat_triage.sql
-- Run against the FULL local database (the one that grows to ~70 GB), NOT a test slice.
--   psql "$DATABASE_URL" -f scripts/db_bloat_triage.sql
-- Purpose: confirm WHERE the bytes are (heap vs index vs dead tuples) and whether the
-- big fact tables hold duplicate rows from repeated/append loads (conflict_cols=None).

\echo '===== 1. Total database size ====='
SELECT pg_size_pretty(pg_database_size(current_database())) AS total_db_size;

\echo '===== 2. Per-table heap vs index vs TOAST, with live/dead tuples (top 25) ====='
SELECT
    c.relname                                                       AS table,
    pg_size_pretty(pg_total_relation_size(c.oid))                   AS total,
    pg_size_pretty(pg_table_size(c.oid))                            AS heap_plus_toast,
    pg_size_pretty(pg_indexes_size(c.oid))                          AS indexes,
    round(100.0 * pg_indexes_size(c.oid)
          / NULLIF(pg_total_relation_size(c.oid),0), 1)             AS pct_index,
    s.n_live_tup                                                    AS live_tup,
    s.n_dead_tup                                                    AS dead_tup,
    round(100.0 * s.n_dead_tup
          / NULLIF(s.n_live_tup + s.n_dead_tup,0), 1)               AS pct_dead
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
WHERE c.relkind = 'r' AND n.nspname = 'public'
ORDER BY pg_total_relation_size(c.oid) DESC
LIMIT 25;

\echo '===== 3. Index inventory on the big fact tables (spot redundant indexes) ====='
SELECT
    t.relname                               AS table,
    i.relname                               AS index,
    pg_size_pretty(pg_relation_size(i.oid)) AS size,
    s.idx_scan                              AS scans,
    pg_get_indexdef(i.oid)                  AS definition
FROM pg_stat_user_indexes s
JOIN pg_class i ON i.oid = s.indexrelid
JOIN pg_class t ON t.oid = s.relid
WHERE t.relname IN ('unified_transactions','unified_contributions','unified_expenditures',
                    'unified_transaction_persons','resolution_input','candidate_pairs',
                    'scored_pairs')
ORDER BY t.relname, pg_relation_size(i.oid) DESC;

\echo '===== 4. Never-scanned indexes (idx_scan = 0) — candidates to drop ====='
SELECT t.relname AS table, i.relname AS index,
       pg_size_pretty(pg_relation_size(i.oid)) AS size
FROM pg_stat_user_indexes s
JOIN pg_class i ON i.oid = s.indexrelid
JOIN pg_class t ON t.oid = s.relid
WHERE s.idx_scan = 0
ORDER BY pg_relation_size(i.oid) DESC
LIMIT 40;

\echo '===== 5. Duplicate-row check on transactions (append/no-conflict-key symptom) ====='
-- If total_rows >> distinct_keys, the loader appended the same data more than once.
SELECT
    count(*)                                                              AS total_rows,
    count(DISTINCT (state_id, transaction_type, transaction_id))         AS distinct_source_keys,
    count(*) - count(DISTINCT (state_id, transaction_type, transaction_id)) AS surplus_rows
FROM unified_transactions
WHERE transaction_id IS NOT NULL;

\echo '===== 6. Row counts across the fan-out tables (fact -> child -> link) ====='
SELECT 'unified_transactions'        AS tbl, count(*) FROM unified_transactions
UNION ALL SELECT 'unified_contributions',        count(*) FROM unified_contributions
UNION ALL SELECT 'unified_expenditures',         count(*) FROM unified_expenditures
UNION ALL SELECT 'unified_transaction_persons',  count(*) FROM unified_transaction_persons
UNION ALL SELECT 'unified_persons',              count(*) FROM unified_persons
UNION ALL SELECT 'unified_entities',             count(*) FROM unified_entities
UNION ALL SELECT 'unified_addresses',            count(*) FROM unified_addresses
UNION ALL SELECT 'resolution_input',             count(*) FROM resolution_input
UNION ALL SELECT 'candidate_pairs',              count(*) FROM candidate_pairs
UNION ALL SELECT 'scored_pairs',                 count(*) FROM scored_pairs
ORDER BY 2 DESC;
