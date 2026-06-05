"""ELT unify spike (proof of concept).

A set-based, SQL-first (dbt-on-Postgres) reimplementation of the unify/load layer
for Texas contributions + expenditures, built BESIDE the imperative
``scripts/loaders/production_loader.py`` (which is untouched) so the two can be
benchmarked and reconciled. See ``docs/adr/0004-elt-unify-spike.md``.
"""
