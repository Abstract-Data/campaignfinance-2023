# RUNBOOK.md

Quick reference for common issues, fixes, and operational procedures for the campaign finance data processing system.

## Related Documentation

Before diving into fixes, check if these docs have the context you need:

| Need | Document |
|------|----------|
| Understanding a field or term | `GLOSSARY.md`, `DATA_DICTIONARY.md` |
| State-specific data quirks | `STATES.md` |
| How components work | `ARCHITECTURE.md` |
| Test failures | `TESTING.md` |
| Code patterns to follow | `../AGENTS.md` |

---

## Common Issues & Fixes

### Issue: High validation failure rate

**Symptoms:**
- Loader reports >5% failed records
- `ValidationError` entries in logs
- Incomplete data in database

**Diagnosis:**
```bash
# Check validation errors in loader log
grep "ValidationError" campaign_finance_loader.log | tail -50

# Check specific field errors
grep "missing" campaign_finance_loader.log | grep -oP "'[^']+'" | sort | uniq -c | sort -rn

# Run validation report on category
uv run python -c "
from app.states.texas import TexasCategory
contrib = TexasCategory('contributions')
contrib.read()
passed, failed = contrib.validate()
failed_list = list(failed)
print(f'Failed: {len(failed_list)}')
for f in failed_list[:5]:
    print(f['error'])
"
```

**Fix:**
1. Check if state portal changed field names → Update `unified_field_library.py`
2. Check for new data format → Add field validator in state validators
3. Check encoding issues → Verify file encoding (UTF-8 vs ISO-8859-1)
4. Re-download data if file corrupted → `uv run python -c "from app.states.texas import TexasDownloader; TexasDownloader().download()"`

---

### Issue: Database duplicate key violations

**Symptoms:**
- `IntegrityError: duplicate key value violates unique constraint`
- Loader slows down significantly
- Address/committee cache not working

**Diagnosis:**
```bash
# Check for duplicate addresses
uv run python -c "
from app.states.postgres_config import create_postgres_database_manager
from sqlalchemy import text
db = create_postgres_database_manager()
with db.get_session() as s:
    dupes = s.exec(text('''
        SELECT street_1, city, state, zip_code, COUNT(*) as cnt
        FROM unified_addresses
        GROUP BY street_1, city, state, zip_code
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC
        LIMIT 10
    ''')).all()
    for d in dupes:
        print(d)
"

# Check committee duplicates
uv run python -c "
from app.states.postgres_config import create_postgres_database_manager
from sqlalchemy import text
db = create_postgres_database_manager()
with db.get_session() as s:
    dupes = s.exec(text('''
        SELECT filer_id, COUNT(*) as cnt
        FROM unified_committees
        GROUP BY filer_id
        HAVING COUNT(*) > 1
    ''')).all()
    print(f'Duplicate committees: {len(dupes)}')
"
```

**Fix:**
1. Run deduplication maintenance:
```bash
uv run python maintenance/dedupe_addresses.py
```

2. Clear caches and reload:
```bash
uv run python -c "
from production_loader import ProductionLoader, LoaderConfig
loader = ProductionLoader(LoaderConfig())
with loader.db_manager.get_session() as session:
    loader.load_existing_data(session)
    loader._dedupe_addresses(session)
    loader._dedupe_persons_and_entities(session)
    session.commit()
"
```

3. If severe, truncate and reload:
```bash
uv run python recreate_tables.py
uv run python production_loader.py production texas_sample
```

---

### Issue: Selenium download fails

**Symptoms:**
- `TimeoutException` in downloader
- Empty or partial files in `tmp/{state}/`
- `WebDriverException: chrome not reachable`

**Diagnosis:**
```bash
# Check Chrome/ChromeDriver version
chromedriver --version
google-chrome --version

# Check if portal is accessible
curl -I https://www.ethics.state.tx.us/data/search/cf/

# Check download directory permissions
ls -la tmp/texas/

# Test with visible browser
uv run python -c "
from app.states.texas import TexasDownloader
dl = TexasDownloader()
dl.config.HEADLESS = False  # Show browser
dl.download()
"
```

**Fix:**
1. Update ChromeDriver to match Chrome version
2. Clear download directory and retry:
```bash
rm -rf tmp/texas/*.csv tmp/texas/*.parquet
uv run python -c "from app.states.texas import TexasDownloader; TexasDownloader().download()"
```
3. If portal changed, update selectors in `texas_downloader.py`
4. Increase timeouts in downloader config

---

### Issue: Memory exhaustion during load

**Symptoms:**
- `MemoryError` or process killed by OOM
- System becomes unresponsive
- Loader hangs at high record counts

**Diagnosis:**
```bash
# Check memory usage during load
watch -n 1 'ps aux | grep python | grep -v grep'

# Check batch size settings
grep -r "batch_size" loader_config.py production_loader.py
```

**Fix:**
1. Reduce batch size:
```python
# loader_config.py
config = LoaderConfig(
    batch_size=50,        # Reduce from 100
    commit_frequency=25,  # Commit more often
    max_records=10000,    # Limit for testing
)
```

2. Process files individually:
```bash
# Process one file at a time instead of all
for f in tmp/texas/contributions_*.parquet; do
    uv run python production_loader.py production "$f"
done
```

3. Increase system swap or use smaller dataset for testing

---

### Issue: Slow loader performance

**Symptoms:**
- Records/second < 100 (target: >500)
- Long pauses between batches
- Database connection timeouts

**Diagnosis:**
```bash
# Profile the loader
uv run python -c "
import cProfile
import pstats
from production_loader import ProductionLoader, LoaderConfig
from pathlib import Path

config = LoaderConfig(batch_size=100, max_records=1000)
loader = ProductionLoader(config)

cProfile.run(
    'loader.load_file(Path(\"tmp/oklahoma/2020_Expenditures.csv\"), state=\"oklahoma\")',
    'loader_profile.stats'
)
stats = pstats.Stats('loader_profile.stats')
stats.sort_stats('cumulative').print_stats(20)
"

# Check database connection pool
uv run python -c "
from app.states.postgres_config import create_postgres_database_manager
db = create_postgres_database_manager()
print(f'Pool size: {db.engine.pool.size()}')
print(f'Checked out: {db.engine.pool.checkedout()}')
"
```

**Fix:**
1. Increase batch size (if memory allows):
```python
config = LoaderConfig(batch_size=200, commit_frequency=100)
```

2. Pre-load caches before processing:
```python
with loader.db_manager.get_session() as session:
    loader.load_existing_data(session)  # Warm caches
```

3. Check PostgreSQL performance:
```sql
-- Check for missing indexes
EXPLAIN ANALYZE SELECT * FROM unified_addresses 
WHERE street_1 = '123 MAIN ST' AND city = 'AUSTIN';

-- Add index if needed
CREATE INDEX idx_addresses_lookup ON unified_addresses(street_1, city, state, zip_code);
```

---

### Issue: Field mapping not found

**Symptoms:**
- `KeyError` for state field name
- Records missing expected data
- "No mapping found for field X" warnings

**Diagnosis:**
```bash
# Check registered mappings for state
uv run python -c "
from app.states.unified_field_library import field_library
mappings = field_library.get_state_mappings('texas')
print(f'Texas mappings: {len(mappings)}')
for m in mappings[:10]:
    print(f'  {m.state_field} -> {m.unified_field}')
"

# Check what headers are in the file
uv run python -c "
import polars as pl
df = pl.scan_parquet('tmp/texas/contributions_01.parquet')
print(df.collect_schema().names()[:20])
"
```

**Fix:**
1. Add missing field mapping to `unified_field_library.py`:
```python
field_library.register_state_mapping(
    StateFieldMapping(
        state='texas',
        state_field='newFieldName',
        unified_field='new_unified_field',
    )
)
```

2. Rebuild schema:
```bash
uv run python -c "
from app.ingest import build_schema_for_states
schema = build_schema_for_states(['texas'])
print(f'Schema fields: {len(schema.fields)}')
"
```

---

### Issue: PaperTrail logging not working

**Symptoms:**
- No remote logs appearing
- `ConnectionRefusedError` in logger
- Only local logs present

**Diagnosis:**
```bash
# Test PaperTrail connection
nc -zv logs4.papertrailapp.com 33096

# Check logger configuration
grep -r "PAPERTRAIL" app/logger.py

# Test logger directly
uv run python -c "
from app.logger import Logger
log = Logger('test')
log.info('Test message from runbook')
print('Check PaperTrail for message')
"
```

**Fix:**
1. Verify PaperTrail credentials in environment
2. Check firewall allows outbound UDP to port 33096
3. Fall back to local-only logging if needed:
```python
# In logger.py, temporarily disable remote handler
# remote_handler = SysLogHandler(...)
# self.logger.addHandler(remote_handler)  # Comment out
```

---

## Debug Commands

### Data Inspection

```bash
# View first N records from a file
uv run python -c "
import polars as pl
df = pl.read_parquet('tmp/texas/contributions_01.parquet')
print(df.head(5))
"

# Check file schema
uv run python -c "
import polars as pl
df = pl.scan_parquet('tmp/texas/contributions_01.parquet')
for name, dtype in df.collect_schema().items():
    print(f'{name}: {dtype}')
"

# Count records by file
for f in tmp/texas/*.parquet; do
    echo -n "$f: "
    uv run python -c "import polars as pl; print(pl.scan_parquet('$f').select(pl.count()).collect().item())"
done

# Search for specific contributor
uv run python -c "
import polars as pl
df = pl.scan_parquet('tmp/texas/contributions_*.parquet')
results = df.filter(
    pl.col('contributorNameLast').str.contains('SMITH')
).collect()
print(f'Found {len(results)} records')
print(results.head(10))
"
```

### Database Queries

```bash
# Get table row counts
uv run python -c "
from app.states.postgres_config import create_postgres_database_manager
from sqlalchemy import text
db = create_postgres_database_manager()
tables = ['unified_transactions', 'unified_committees', 'unified_addresses', 
          'unified_persons', 'unified_entities', 'unified_contributions']
with db.get_session() as s:
    for t in tables:
        try:
            cnt = s.exec(text(f'SELECT COUNT(*) FROM {t}')).first()[0]
            print(f'{t}: {cnt:,}')
        except:
            print(f'{t}: (not found)')
"

# Check recent transactions
uv run python -c "
from app.states.postgres_config import create_postgres_database_manager
from sqlalchemy import text
db = create_postgres_database_manager()
with db.get_session() as s:
    recent = s.exec(text('''
        SELECT id, transaction_id, amount, transaction_date 
        FROM unified_transactions 
        ORDER BY id DESC LIMIT 10
    ''')).all()
    for r in recent:
        print(r)
"

# Find transactions by committee
uv run python -c "
from app.states.postgres_config import create_postgres_database_manager
from sqlalchemy import text
db = create_postgres_database_manager()
with db.get_session() as s:
    txs = s.exec(text('''
        SELECT t.id, t.amount, c.name 
        FROM unified_transactions t
        JOIN unified_committees c ON t.committee_id = c.filer_id
        WHERE c.name ILIKE '%republican%'
        LIMIT 10
    ''')).all()
    for t in txs:
        print(t)
"
```

### Loader Operations

```bash
# Run loader with debug output
uv run python production_loader.py development oklahoma_2020 2>&1 | tee loader_debug.log

# Test single file load
uv run python -c "
from production_loader import ProductionLoader, LoaderConfig
from pathlib import Path

config = LoaderConfig(batch_size=10, max_records=100, enable_logging=True)
loader = ProductionLoader(config)
stats = loader.load_file(
    Path('tmp/oklahoma/2020_Expenditures.csv'),
    state='oklahoma'
)
print(f'Success: {stats.successful_records}/{stats.total_records}')
print(f'Rate: {stats.records_per_second:.1f} rec/s')
"

# Run validation only (no database)
uv run python -c "
from app.states.texas import TexasCategory
contrib = TexasCategory('contributions')
contrib.read()
passed_count = 0
failed_count = 0
for status, record in contrib.validate():
    if status == 'passed':
        passed_count += 1
    else:
        failed_count += 1
    if passed_count + failed_count >= 1000:
        break
print(f'Passed: {passed_count}, Failed: {failed_count}')
print(f'Pass rate: {passed_count/(passed_count+failed_count)*100:.1f}%')
"

# Clear and reload specific state
uv run python -c "
from app.states.postgres_config import create_postgres_database_manager
from sqlalchemy import text
db = create_postgres_database_manager()
with db.get_session() as s:
    # Get state ID
    state = s.exec(text(\"SELECT id FROM states WHERE code = 'TX'\")).first()
    if state:
        state_id = state[0]
        # Delete state data (cascades)
        s.exec(text(f'DELETE FROM unified_transactions WHERE state_id = {state_id}'))
        s.commit()
        print(f'Cleared Texas data (state_id={state_id})')
"
```

### System Health

```bash
# Check disk space for temp files
du -sh tmp/*/

# Monitor loader process
watch -n 2 'ps aux | grep "production_loader" | grep -v grep'

# Check PostgreSQL connections
uv run python -c "
from app.states.postgres_config import create_postgres_database_manager
from sqlalchemy import text
db = create_postgres_database_manager()
with db.get_session() as s:
    conns = s.exec(text('''
        SELECT count(*) as connections,
               state,
               usename
        FROM pg_stat_activity
        WHERE datname = current_database()
        GROUP BY state, usename
    ''')).all()
    for c in conns:
        print(c)
"

# Test database connection
uv run python -c "
from app.states.postgres_config import create_postgres_database_manager
db = create_postgres_database_manager()
with db.get_session() as s:
    result = s.exec(text('SELECT 1')).first()
    print('Database connection: OK' if result else 'FAILED')
"
```

## Alert Thresholds

| Alert | Threshold | Action |
|-------|-----------|--------|
| Validation failure rate | > 5% per file | Review field mappings, check data format |
| Loader throughput | < 100 rec/s | Check batch size, database indexes |
| Memory usage | > 2GB | Reduce batch size, check for leaks |
| Download timeout | > 3 failures | Check portal, update selectors |
| Database connections | > 20 | Check for connection leaks |
| Duplicate rate | > 10% | Review deduplication caches |
| File encoding errors | > 1% | Add encoding fallback |
| Commit failures | Any | Investigate immediately, check constraints |

## Quick Reference

### Loader Presets

```bash
# Development - small batches, verbose
uv run python production_loader.py development oklahoma_2020

# Testing - medium batches, limited records
uv run python production_loader.py testing texas_sample

# Production - optimized for throughput
uv run python production_loader.py production oklahoma_2021

# High Performance - large batches, minimal logging
uv run python production_loader.py high_performance texas_full

# Safe - small batches, frequent commits
uv run python production_loader.py safe oklahoma_2020
```

### File Locations

```
tmp/texas/              # Texas downloaded data
tmp/oklahoma/           # Oklahoma downloaded data
tmp/fec/                # FEC federal data
app/logs/               # Application logs
campaign_finance.db     # SQLite development database
campaign_finance_loader.log  # Loader-specific log
.env                    # Environment variables (gitignored)
```

### Emergency Procedures

```bash
# Stop all running loaders
pkill -f "production_loader"

# Rollback database to backup
pg_restore -d campaign_finance backup_YYYYMMDD.dump

# Clear all caches and restart
rm -rf __pycache__ app/__pycache__ app/**/__pycache__
uv sync --reinstall

# Reset to clean state
uv run python recreate_tables.py
rm -rf tmp/texas/* tmp/oklahoma/*
```
