// Vectorized-ingest implementation workflow (plan phases P1-P4).
// Spec: docs/design/vectorized-ingest-workflow-spec.md
// Plan: docs/design/vectorized-ingest-plan.md
//
// DRAFT / ready-to-launch. PREREQS before running:
//   1. P0 equivalence harness merged (app/core/ingest_equivalence.py + golden fixtures).
//   2. Scaffold app/core/ingest_vectorized/ with run_vectorized(engine, fixtures_dir).
// The gate for every family is: diff_snapshots(orm_snap, vec_snap) == [] on the
// golden fixtures (restricted to the family's tables). Verifiers run the diff
// themselves; implementer self-reports are not trusted.

export const meta = {
  name: 'vectorized-ingest',
  description: 'Implement the vectorized ingest engine family-by-family, each gated by the P0 equivalence harness (row-for-row diff vs the ORM loader)',
  phases: [
    { title: 'Implement', detail: 'one implementer per record-type family (Polars expressions, no map_elements)' },
    { title: 'Verify', detail: 'family-restricted equivalence diff + map_elements scan' },
    { title: 'Integrate', detail: 'full-slice equivalence + throughput benchmark + P5 go/no-go' },
  ],
}

const FAMILIES = [
  {
    key: 'refs',
    types: ['FILER', 'CVR1', 'FINL', 'SPAC', 'CVR2', 'CVR3', 'EXCAT'],
    tables: ['unified_committees', 'unified_reports', 'unified_committee_persons',
             'committee_purpose', 'expenditure_category', 'spac_link', 'unified_notice'],
  },
  {
    key: 'flat_txns',
    types: ['RCPT', 'EXPN'],
    tables: ['unified_transactions', 'unified_contributions', 'unified_expenditures',
             'unified_transaction_persons', 'unified_persons', 'unified_entities', 'unified_addresses'],
  },
  {
    key: 'detail_children',
    types: ['LOAN', 'DEBT', 'CRED', 'TRVL', 'ASSET', 'PLDG'],
    tables: ['unified_loans', 'unified_debts', 'unified_credits', 'unified_travel',
             'unified_assets', 'unified_pledges', 'loan_guarantors'],
  },
  {
    key: 'cand',
    types: ['CAND'],
    tables: ['unified_transaction_persons'],
  },
]

const IMPL_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    files_changed: { type: 'array', items: { type: 'string' } },
    approach: { type: 'string', description: 'how the builder semantics were vectorized' },
    map_elements_used: { type: 'boolean', description: 'true if any per-row UDF remains (should be false)' },
    self_check: { type: 'string', description: 'how the implementer verified locally' },
  },
  required: ['files_changed', 'approach', 'map_elements_used', 'self_check'],
}

const VERDICT_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    passed: { type: 'boolean' },
    diff_lines: { type: 'array', items: { type: 'string' }, description: 'diff_snapshots output (empty when equal)' },
    map_elements_found: { type: 'boolean' },
    notes: { type: 'string' },
  },
  required: ['passed', 'diff_lines', 'map_elements_found'],
}

const INTEGRATE_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    all_tables_equal: { type: 'boolean' },
    throughput_x: { type: 'number', description: 'vectorized rows/s vs ORM rows/s' },
    blockers: { type: 'array', items: { type: 'string' } },
    recommendation: { type: 'string', enum: ['ship', 'hold'] },
  },
  required: ['all_tables_equal', 'throughput_x', 'blockers', 'recommendation'],
}

function implPrompt(fam, priorDiff, attempt) {
  const feedback = priorDiff.length
    ? `\n\nThe previous attempt FAILED the equivalence gate. Fix exactly these diffs:\n- ${priorDiff.join('\n- ')}`
    : ''
  return `Vectorize ingest for TEC record types ${fam.types.join(', ')} (attempt ${attempt}).
Write the transform in app/core/ingest_vectorized/${fam.key}.py using PURE Polars
column expressions (scan_parquet -> with_columns(str/when/cast) -> unique/join ->
write). Reuse the field mappings in app/core/unified_field_library.py. Reproduce the
ORM builder semantics for these types EXACTLY — study app/core/builders.py and
app/core/processor.py (RECORD_TYPE_ROLE_MAP, DETAIL_BUILDERS, _build_guarantors) for
${fam.types.join(', ')}.
HARD RULE: no map_elements / apply (per-row Python UDF) anywhere in the hot path.
GOAL: make diff_snapshots(orm, vectorized) == [] for tables ${fam.tables.join(', ')}
on the golden fixtures (tests/fixtures/ingest_golden/). Add/extend unit tests.
Return files_changed, approach, map_elements_used (must be false), self_check.${feedback}`
}

function verifyPrompt(fam) {
  return `Adversarially verify the vectorized ingest for ${fam.types.join(', ')}. Do NOT
trust the implementer. Yourself:
1. Load the golden fixtures via the ORM loader and via app/core/ingest_vectorized
   (run_vectorized), snapshot both with app.core.ingest_equivalence.snapshot_unified.
2. Compute diff_snapshots(orm, vec) RESTRICTED to tables ${fam.tables.join(', ')}.
3. grep app/core/ingest_vectorized/${fam.key}.py for map_elements / .apply(.
Return {passed: diff is empty AND no map_elements, diff_lines, map_elements_found, notes}.`
}

// ── run ──────────────────────────────────────────────────────────────────────
const MAX_ATTEMPTS = 3

async function buildFamily(fam) {
  let priorDiff = []
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    const impl = await agent(implPrompt(fam, priorDiff, attempt), {
      label: `impl:${fam.key}#${attempt}`, phase: 'Implement', schema: IMPL_SCHEMA,
      isolation: 'worktree',
    })
    const verdict = await agent(verifyPrompt(fam), {
      label: `verify:${fam.key}#${attempt}`, phase: 'Verify', schema: VERDICT_SCHEMA,
    })
    if (verdict && verdict.passed && !verdict.map_elements_found) {
      log(`${fam.key}: PASSED equivalence gate on attempt ${attempt}`)
      return { family: fam.key, passed: true, attempts: attempt, impl }
    }
    priorDiff = (verdict && verdict.diff_lines) || ['(verifier returned no diff)']
    log(`${fam.key}: attempt ${attempt} failed (${priorDiff.length} diff lines)`)
  }
  return { family: fam.key, passed: false, attempts: MAX_ATTEMPTS, diff_lines: priorDiff }
}

phase('Implement')
const results = await parallel(FAMILIES.map((fam) => () => buildFamily(fam)))
const passed = results.filter((r) => r && r.passed).map((r) => r.family)
const failed = results.filter((r) => r && !r.passed).map((r) => r.family)
log(`families passed: ${passed.join(', ') || 'none'} | failed: ${failed.join(', ') || 'none'}`)

let integration = null
if (failed.length === 0) {
  phase('Integrate')
  integration = await agent(
    `All families pass the per-family equivalence gate. Now:
1. Run FULL-SLICE equivalence on the golden fixtures (ALL tables): ORM vs vectorized,
   diff_snapshots == [].
2. Benchmark vectorized vs ORM ingest throughput on a larger sample (rows/s ratio).
3. List any blockers to flipping the default engine (P5).
Return {all_tables_equal, throughput_x, blockers, recommendation}.`,
    { label: 'integrate', phase: 'Integrate', schema: INTEGRATE_SCHEMA },
  )
} else {
  log('skipping Integrate: some families did not reach the equivalence gate')
}

return { passed, failed, results, integration }
