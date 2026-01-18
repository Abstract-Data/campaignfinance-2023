# CHANGELOG.md

All notable changes to the campaign finance data processing system.

> **See also:** `AGENTS.md` for current patterns, `CONTRIBUTING.md` for how to document changes.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Comprehensive documentation suite (AGENTS.md, TESTING.md, ARCHITECTURE.md, RUNBOOK.md)
- GLOSSARY.md with campaign finance terminology
- DATA_DICTIONARY.md with field definitions and state mappings
- CONTRIBUTING.md with contribution guidelines
- STATES.md with state-specific configuration details

### Changed
- Enhanced AGENTS.md with runbook maintenance instructions

### Fixed
- (Add fixes here as they occur)

### Deprecated
- (Add deprecations here as they occur)

---

## [0.1.0] - Initial Release

### Added

#### Core Architecture
- Abstract Base Class (ABC) pattern for state-agnostic processing
  - `StateCategoryClass` - Core data processing pipeline
  - `StateFileValidation` - Record validation with Pydantic
  - `FileDownloaderABC` - Data acquisition abstraction
  - `DBLoaderClass` - Database operations
  - `StateConfig` - State configuration container

#### Unified Field Library
- Cross-state field mapping system
- `FieldDefinition` for unified field specifications
- `StateFieldMapping` for state-to-unified mappings
- Support for Texas and Oklahoma field mappings

#### File Ingestion System
- `GenericFileReader` - Schema-driven file parsing
- Support for CSV and Parquet file formats
- Automatic header normalization
- Type conversion and validation
- Encoding fallback (UTF-8 → ISO-8859-1)

#### Texas Implementation
- `TECDownloader` - Selenium-based data acquisition from TEC portal
- Validators for contributions, expenditures, filers, reports, travel, candidates, debts
- Field mappings for all TEC data categories
- Parquet file consolidation

#### Oklahoma Implementation
- `OklahomaCategory` - Category processing for Oklahoma data
- Validators for contributions, expenditures, lobby data
- Field mappings for Oklahoma data categories

#### Production Loader
- `ProductionLoader` - Batch database loading
- Address deduplication with caching
- Committee deduplication
- Entity and person deduplication
- Progress display with Rich
- Configurable batch sizes and commit frequency
- Error tracking and recovery

#### Unified Data Models
- `UnifiedTransaction` - Cross-state transaction model
- `UnifiedCommittee` - Committee model
- `UnifiedPerson` - Person model
- `UnifiedAddress` - Address model
- `UnifiedEntity` - Entity model
- `UnifiedCampaign` - Campaign model

#### Database Support
- PostgreSQL for production
- SQLite for development
- SQLModel ORM integration
- Session management with context managers

#### Logging & Monitoring
- `Logger` class with PaperTrail integration
- Local timed rotating file logs
- Console output

#### Testing
- Pytest test suite
- Hypothesis property-based testing
- Test fixtures for common setups

### Technical Details

#### Dependencies Added
- `sqlmodel>=0.0.22` - ORM
- `pydantic>=2.10.4` - Validation
- `polars>=1.19.0` - Data processing
- `pandas>=2.2.3` - Data analysis
- `selenium>=4.27.1` - Web scraping
- `rich>=13.9.4` - CLI output
- `hypothesis>=6.112.1` - Property testing
- `pytest>=8.3.3` - Testing

---

## Version History Template

Use this template for future releases:

```markdown
## [X.Y.Z] - YYYY-MM-DD

### Added
- New features

### Changed
- Changes to existing functionality

### Deprecated
- Features to be removed in future versions

### Removed
- Features removed in this version

### Fixed
- Bug fixes

### Security
- Security-related changes
```

---

## Migration Notes

### Upgrading to 0.1.0

This is the initial release. No migration needed.

### Future Migrations

Document database schema changes, breaking API changes, and required actions here.

---

## Contributing to the Changelog

When making changes:

1. Add your change to the `[Unreleased]` section
2. Use the appropriate category (Added, Changed, Fixed, etc.)
3. Write clear, user-focused descriptions
4. Reference issue numbers if applicable

Example:
```markdown
### Fixed
- Address deduplication now handles case variations (#123)
```
