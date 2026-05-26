# Task 5d — Infrastructure: Dockerfile, Splink Pin, Scraper Drift, PII Audit

**Wave:** 5 — Quality & Infrastructure  
**Branch:** `remediation-r3/wave-5/task-5d-infra`  
**Effort:** ~4-5 hours  
**Parallel with:** 5a, 5b, 5c

---

## Findings Addressed

| ID | Finding | Severity |
|----|---------|----------|
| R5 | No containerization — production relies on host-level cron | P2 High |
| R6 | Splink version not pinned in CI matrix | P2 High |
| R1 | Selenium scraper fragility — drift detector not applied to all scrapers | P2 High |
| R7 | PII potentially exposed in logged `raw_data` JSON field | P2 Medium |

---

## Fix 1: Dockerfile + Docker Compose

### Context

`docs/DEPLOYMENTS.md` documents host-level cron. No Dockerfile is present. This means environment drift across dev/staging/prod is possible, and onboarding a second developer requires matching the host OS.

### `Dockerfile`

```dockerfile
FROM python:3.12-slim

# Install uv
RUN pip install --no-cache-dir uv

WORKDIR /app

# Install dependencies first (cache layer)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Copy application code
COPY app/ ./app/
COPY docs/ ./docs/

# Non-root user
RUN useradd --create-home appuser
USER appuser

# Default entrypoint — override in compose for specific commands
ENTRYPOINT ["uv", "run", "cf"]
CMD ["--help"]
```

### `docker-compose.yml`

```yaml
version: "3.9"

services:
  db:
    image: postgres:16
    environment:
      POSTGRES_USER: ${POSTGRES_USER:-campaignfinance}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB:-campaignfinance}
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER:-campaignfinance}"]
      interval: 5s
      timeout: 5s
      retries: 5

  app:
    build: .
    environment:
      - DATABASE_URL=postgresql://${POSTGRES_USER:-campaignfinance}:${POSTGRES_PASSWORD}@db:5432/${POSTGRES_DB:-campaignfinance}
      - LOG_LEVEL=${LOG_LEVEL:-INFO}
      - OP_SERVICE_ACCOUNT_TOKEN=${OP_SERVICE_ACCOUNT_TOKEN}
    depends_on:
      db:
        condition: service_healthy
    volumes:
      - ./data:/app/data:ro  # read-only input data mount

volumes:
  pgdata:
```

### `.dockerignore`

```
.git/
.github/
__pycache__/
*.pyc
*.pyo
*.pyd
.env
*.db
.mypy_cache/
.ruff_cache/
tests/
prompts/
docs/
*.egg-info/
dist/
```

### Update `docs/DEPLOYMENTS.md`

Add a "Container Deployment" section documenting:
```bash
# Build and start
docker compose up --build -d

# Run a one-off ingestion
docker compose run --rm app cf ingest TX --year 2024

# Initialize the database
docker compose run --rm app cf db-init
```

---

## Fix 2: Pin Splink Version in CI Matrix

### Context

`AGENTS.md` warns that Splink API shifts between minor versions. The current `pyproject.toml` likely has `splink>=4.x` without an upper bound. If Splink releases a breaking 4.x.y, the next CI run after `uv lock` update will fail.

### Changes

**`pyproject.toml`** — constrain the upper bound:
```toml
[project.dependencies]
...
splink = ">=4.0.0,<4.1.0"   # Pin to known-good minor; update intentionally
```

Adjust the exact version range to match the currently installed version:
```bash
uv pip show splink | grep Version
```

**`.github/workflows/ci.yml`** — document the Splink version in CI matrix comment:
```yaml
# Splink is pinned to 4.x.y in pyproject.toml — update intentionally, not automatically
```

**Add a Splink version check to CI:**
```yaml
- name: Verify Splink version
  run: python -c "import splink; print('Splink:', splink.__version__)"
```

---

## Fix 3: Expand Scraper Drift Detector

### Context

`app/scrapers/drift_detector.py` exists with `DriftDetector` using structural fingerprinting. The developer assessment flagged Selenium scraper fragility (R1) as the highest-risk item.

**Verify what scrapers exist:**
```bash
ls app/scrapers/
find app/states/ -name "*.py" | xargs grep -l "selenium\|webdriver" | head -10
```

**For each scraper that fetches from a state portal:**

1. Add a `DriftDetector` call after page load:
```python
from app.scrapers.drift_detector import DriftDetector, DriftDetectedError

detector = DriftDetector(state="texas", page_name="filer_search")

# After fetching page HTML:
try:
    detector.check(html_content)
except DriftDetectedError as e:
    logger.error("Scraper drift detected: %s — manual review required", e)
    raise
```

2. Save an HTML fixture of the current known-good page:
```bash
mkdir -p tests/fixtures/scrapers/
# Save a snapshot of the current page HTML as the fixture baseline
```

3. Add a test that runs `DriftDetector.check()` against the fixture and confirms no drift:
```python
def test_texas_scraper_no_drift():
    fixture = Path("tests/fixtures/scrapers/texas_filer_search.html").read_text()
    detector = DriftDetector(state="texas", page_name="filer_search")
    detector.check(fixture)  # should not raise
```

---

## Fix 4: PII Audit in `raw_data` Field

### Context

Developer assessment flagged that `UnifiedTransaction.raw_data` (a JSON column) stores the original CSV row. Campaign finance data includes names, addresses, employer/occupation — PII. If `raw_data` is included in logs at DEBUG level, PII leaks.

**Audit what goes into `raw_data`:**
```bash
grep -rn "raw_data" app/core/ | head -20
grep -rn "logger\." app/core/unified_state_loader.py | grep -i "raw\|record\|row" | head -10
```

**If raw records are logged at DEBUG:**
```python
# Before (potentially logs PII):
logger.debug("Processing record: %s", record)

# After (safe):
logger.debug("Processing record id=%s state=%s", record.get("id"), state)
```

**Identify PII fields in the raw CSV columns:**
```bash
grep -rn "FieldCategory\|PERSONAL\|PII" app/core/unified_field_library.py | head -20
```

**Document the policy in `docs/DATA_DICTIONARY.md` or `docs/adr/0002-data-classification-and-retention.md`:**

> The `raw_data` JSON column stores original source rows including PII (names, addresses, employer, occupation). This column must never be included in DEBUG logs or API responses. Access to raw_data is restricted to data engineering roles.

---

## Verification Checklist

```bash
# 1. Dockerfile exists and builds
docker build -t campaignfinance-test . && echo "PASS" || echo "FAIL"

# 2. docker-compose.yml exists
ls docker-compose.yml && echo "PASS" || echo "FAIL"

# 3. Splink version pinned
grep "splink" pyproject.toml | grep "<" && echo "PASS" || echo "FAIL"

# 4. DriftDetector used in at least one scraper
grep -rn "DriftDetector" app/states/ app/scrapers/ | wc -l  # ≥ 1

# 5. No raw record logging at DEBUG
grep -rn "logger.debug.*record\|logger.debug.*row" app/core/unified_state_loader.py \
  | grep -v "#" | head -5

# 6. Tests pass
uv run pytest tests/ -q
```

---

## Commit Message

```
feat/fix(infra): Dockerfile, Splink pin, scraper drift, PII log audit

- Add Dockerfile (python:3.12-slim + uv), docker-compose.yml with Postgres service,
  and .dockerignore; update DEPLOYMENTS.md with container deployment steps
- Pin Splink to known-good minor version in pyproject.toml; add version check to CI
- Expand DriftDetector to all Selenium scrapers; add HTML fixture-based drift tests
- Audit raw_data logging — remove PII from DEBUG log statements;
  document raw_data access policy in DATA_DICTIONARY.md

Fixes R1, R5, R6, R7
```
