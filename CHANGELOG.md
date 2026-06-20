# Changelog

All notable changes to this project are documented here.

## [Unreleased]

### Removed
- `app/resolve/staging.py` — atomic table swap helpers were tested but never wired
  to Stage 7 publish; survivorship uses delete-and-replace on live canonical tables.
