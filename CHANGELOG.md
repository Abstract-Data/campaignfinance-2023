# Changelog

All notable changes to this project are documented here.

## 0.1.0 (2026-06-21)


### ⚠ BREAKING CHANGES

* **ingest:** tighten individual person dedup key to (name + address)
* **ingest:** one-to-one entity representative assignment (blocker #2)
* **ingest:** fix DEBT/TRVL data-loss + add bulk upsert primitive (wave 1)
* **core:** implement pipeline fix pack (fixes 1–7) with role-scoped field routing and dedup constraints

### Features

* **core:** extract loan/debt guarantors into loan_guarantors ([d4ce4a5](https://github.com/Abstract-Data/campaignfinance-2023/commit/d4ce4a5f62f7af51a8fa2ad974d506fdc8b60c97))
* **core:** field-coverage audit, load cache, FILER ingest, candidate role ([cf8c935](https://github.com/Abstract-Data/campaignfinance-2023/commit/cf8c93553547c46300be770bed02911ef8ece832))
* **core:** implement pipeline fix pack (fixes 1–7) with role-scoped field routing and dedup constraints ([b6ebc14](https://github.com/Abstract-Data/campaignfinance-2023/commit/b6ebc14b5eb33d33fb7151c68e3022cfe8f69e9c))
* db-bloat operational tooling (Wave 0 + Wave 3) ([e747b6d](https://github.com/Abstract-Data/campaignfinance-2023/commit/e747b6dbbfe43e6ea1d598065ff3610cdc601e17))
* **db:** Alembic 0002 — dedup legacy transactions, then apply unique index ([9834f96](https://github.com/Abstract-Data/campaignfinance-2023/commit/9834f9690bf44cca124d8d878d930fc73d1c3dde))
* **db:** Alembic 0002 — dedup legacy transactions, then apply unique index ([23fd067](https://github.com/Abstract-Data/campaignfinance-2023/commit/23fd067e311966372605d76ca3c8410584f0d171))
* **db:** cf migrate command + schema-parity test + MIGRATIONS.md ([d80949f](https://github.com/Abstract-Data/campaignfinance-2023/commit/d80949fd2e9447282aa03197554b048648395d3f))
* **db:** introduce Alembic with a verified baseline migration ([846c6f8](https://github.com/Abstract-Data/campaignfinance-2023/commit/846c6f880d79f075d5b3c092aee56e3e28c5c267))
* **db:** remove committed dev SQLite; make SQLite an interactive opt-in ([acad934](https://github.com/Abstract-Data/campaignfinance-2023/commit/acad93415208a8e4db66327704e19e518687b0d0))
* **db:** unique indexes + dedup for campaigns/committee_persons/campaign_entities ([ba4a61e](https://github.com/Abstract-Data/campaignfinance-2023/commit/ba4a61e994f47922b3667d08bb9f70869f16e23e))
* drop 16 zero-scan non-constraint indexes (Wave 5a) ([1fc41cf](https://github.com/Abstract-Data/campaignfinance-2023/commit/1fc41cfd2bc71a1f760f92885aec1d9a52e7ad32))
* drop unified_reports.raw_data (Wave 2) ([a8ac7cf](https://github.com/Abstract-Data/campaignfinance-2023/commit/a8ac7cff05052a22f9b882399ee5175fb773ae4a))
* drop unified_transactions.raw_data + campaign source cols (Wave 1) ([fae3c94](https://github.com/Abstract-Data/campaignfinance-2023/commit/fae3c9472b8e83c65c0ce30fd860aed46d34f1da))
* **infra:** Dockerfile, Splink pin, scraper drift, PII policy ([800ed94](https://github.com/Abstract-Data/campaignfinance-2023/commit/800ed94c3306f1a58f3bb6513e5954c476d9f2a0))
* **ingest-vec:** extend omit-null address match to EXPN + detail_children ([7e95b6d](https://github.com/Abstract-Data/campaignfinance-2023/commit/7e95b6d5afdee6592d8db812b4730ab352bacf46))
* **ingest-vec:** omit-null address match in flat_txns_detail + share helper ([b6488fe](https://github.com/Abstract-Data/campaignfinance-2023/commit/b6488fee716a7d4a8a274ce0d28c9a128bffa87f))
* **ingest-vec:** omit-null partial-address match helpers ([c5f1346](https://github.com/Abstract-Data/campaignfinance-2023/commit/c5f1346bc81060f5dd871c492cbeb276dd241f5a))
* **ingest-vec:** row-level error isolation in write_frame (ingest_errors) ([ca480c8](https://github.com/Abstract-Data/campaignfinance-2023/commit/ca480c8db061b2354fed80386c186708ca0f8045))
* **ingest-vec:** wire omit-null address match into flat_txns_dims (RCPT) ([1400904](https://github.com/Abstract-Data/campaignfinance-2023/commit/1400904bfd6858f6c3ad4efc18d38707c24ce7d7))
* **ingest:** finalize vectorized campaigns family on main (ORM ⊆ vec) ([07de119](https://github.com/Abstract-Data/campaignfinance-2023/commit/07de1192b8e4e53836d1e06afc09a0621c044780))
* **ingest:** fix DEBT/TRVL data-loss + add bulk upsert primitive (wave 1) ([03d5dc6](https://github.com/Abstract-Data/campaignfinance-2023/commit/03d5dc6a51e682b182fef2812905e6ca5d3bebe8))
* **ingest:** harness FK-&gt;natural-key resolution (linkage-infra, opt-in) ([f38ab5f](https://github.com/Abstract-Data/campaignfinance-2023/commit/f38ab5f3c38edf551032ff0c4d014263a810f90a))
* **ingest:** idempotent campaigns/committee_persons/campaign_entities/guarantors ([d1d3541](https://github.com/Abstract-Data/campaignfinance-2023/commit/d1d3541c04d55c1f6288f8afc9cef53c8927d35d))
* **ingest:** idempotent cand.py entity and txn_person writes ([fdfe197](https://github.com/Abstract-Data/campaignfinance-2023/commit/fdfe1976f9e1947b99328f360fe63d45fd1f1773))
* **ingest:** idempotent persons/addresses via anti-join ([0859f08](https://github.com/Abstract-Data/campaignfinance-2023/commit/0859f08bbc5b5f8c0ca95ea9c450fa5b36da4bd4))
* **ingest:** idempotent unified_entities writes (first-write-wins) ([c2e808c](https://github.com/Abstract-Data/campaignfinance-2023/commit/c2e808c23dee817774dbd3a43b7f283912481b9c))
* **ingest:** P0 equivalence harness for the vectorized ingest rewrite ([1808fdf](https://github.com/Abstract-Data/campaignfinance-2023/commit/1808fdf98fa2fc02c42e8a6cc6e407d33c9945a6))
* **ingest:** partial-index ON CONFLICT + filter_new_rows anti-join helper ([db3969b](https://github.com/Abstract-Data/campaignfinance-2023/commit/db3969b385430331bbfa2fae3eed91dc448d0f4f))
* **ingest:** partial-index ON CONFLICT + filter_new_rows anti-join helper ([cff804c](https://github.com/Abstract-Data/campaignfinance-2023/commit/cff804cfe5276ce60ecfdd59224f842f1be0e7bf))
* **ingest:** Postgres COPY write-path + throughput benchmark (39x vs ORM) ([97b7d18](https://github.com/Abstract-Data/campaignfinance-2023/commit/97b7d18390e125b352910cb05485342317f9f46c))
* **ingest:** vectorized cand enrichment family (CAND -&gt; candidate↔expenditure link) ([5cb151c](https://github.com/Abstract-Data/campaignfinance-2023/commit/5cb151c608925eafa5e7b814657aa6ad4afb6a3f))
* **ingest:** vectorized detail_children family (LOAN/DEBT/CRED/TRVL/ASSET/PLDG + loan_guarantors) ([89b9c73](https://github.com/Abstract-Data/campaignfinance-2023/commit/89b9c735387b17ef41521fdd3683c4410ec34b78))
* **ingest:** vectorized dim-normalization primitives (entity name, person full_name, state) ([e43acc2](https://github.com/Abstract-Data/campaignfinance-2023/commit/e43acc289136753fcbd85a1b9f89276ea48b2a14))
* **ingest:** vectorized FILER family + flat_txns_dims anti-join (on main+[#48](https://github.com/Abstract-Data/campaignfinance-2023/issues/48)) ([0b7227d](https://github.com/Abstract-Data/campaignfinance-2023/commit/0b7227d21e95a51cf847e49f78eeaa8589107a05))
* **ingest:** vectorized flat_txns detail/junction family (RCPT/EXPN) ([8e73fc5](https://github.com/Abstract-Data/campaignfinance-2023/commit/8e73fc59c336a2c362394767bbe604ea358a31e1))
* **ingest:** vectorized flat_txns dim layer (persons/entities/addresses/committees), gated ([8e2aee3](https://github.com/Abstract-Data/campaignfinance-2023/commit/8e2aee30b8dcde3a6db0dcd999ff3a0f3c19ead5))
* **ingest:** vectorized flat_txns family — unified_transactions (RCPT/EXPN), gated-equivalent ([1b85bd8](https://github.com/Abstract-Data/campaignfinance-2023/commit/1b85bd8cc931db0dda28b450a30ad53f9fe2740a))
* **ingest:** vectorized ingest foundation (P1) — package, parse-parity primitives, dispatcher ([e483db0](https://github.com/Abstract-Data/campaignfinance-2023/commit/e483db00fb432315fc4ff57c3779b31214911e28))
* **ingest:** vectorized reports family (CVR1/FINL -&gt; unified_reports), gated equivalent ([cc5340e](https://github.com/Abstract-Data/campaignfinance-2023/commit/cc5340eb30b8d3cca73adb8d43b8cddd9e3d3536))
* **ingest:** wire reports family into foundation (write_frame auto-cols, by-type dispatch, registry) ([1a5ede2](https://github.com/Abstract-Data/campaignfinance-2023/commit/1a5ede2c96b6ecdf033a6fb9ff8eb2f382bb554c))
* **loader:** flip default ingest engine to vectorized (cf load) ([d7a4612](https://github.com/Abstract-Data/campaignfinance-2023/commit/d7a4612bdb0dc9fe1479ee853c26cf6bef1ca250))
* **loaders:** add validate_schema pre-flight (drift audit over all files) ([8e53bb9](https://github.com/Abstract-Data/campaignfinance-2023/commit/8e53bb99cb8e7f3532f0cab6a9a733d8e3978fce))
* **loaders:** subset loader + production loader reject/dedup handling ([7ec9c28](https://github.com/Abstract-Data/campaignfinance-2023/commit/7ec9c28f4f1cf9f723e853cad25d46afb44db73d))
* **prompts:** add ELT unify/load refactor proof-of-concept brief ([dbf034f](https://github.com/Abstract-Data/campaignfinance-2023/commit/dbf034feb2d04ebb587d10470799bf72ece7fbf6))
* **resolve:** add postgres sql blocking backend for stage 2 ([b4edf57](https://github.com/Abstract-Data/campaignfinance-2023/commit/b4edf57c4bbce9fcbdb87942894b450a6c5c09de))
* **resolve:** backfill canonical_entity.canonical_address_id ([2440c9a](https://github.com/Abstract-Data/campaignfinance-2023/commit/2440c9a1260e176bf72410379926bd6a88025558))
* **resolve:** deterministic canonical campaign & address publishers ([81008ea](https://github.com/Abstract-Data/campaignfinance-2023/commit/81008ea37e1fd500e27c521dc38f4affc9a98ef8))
* **resolve:** employer signal + selective org cross-role blocking + employer survivorship (wave 3) ([fcd207c](https://github.com/Abstract-Data/campaignfinance-2023/commit/fcd207c737ee0afdf7be83c9152ad1192128bacf))
* **transform:** ELT medallion pipeline — dedup + FK linking across all record types ([1aa8935](https://github.com/Abstract-Data/campaignfinance-2023/commit/1aa893540129d4041be6670d65899d44a3cb554d))
* **transform:** ELT medallion pipeline — dedup + FK linking across all record types ([7fefc52](https://github.com/Abstract-Data/campaignfinance-2023/commit/7fefc52027eb68d26654ee9ff6f876fa5078de51))
* **transform:** entity-association + officer linking layer (Phase 2) ([d14492f](https://github.com/Abstract-Data/campaignfinance-2023/commit/d14492f4c65e1f8ed0cc8011bba0934c7fb2cbe8))
* **transform:** populate unified_entities.address_id for resolve occupancy (Phase 3) ([09b37c1](https://github.com/Abstract-Data/campaignfinance-2023/commit/09b37c1bb02b242ccaafb7bfca3065eee6852da6))
* **unified:** idempotent unified layer + at-filing report cols + null-year campaigns (wave 2) ([44a347c](https://github.com/Abstract-Data/campaignfinance-2023/commit/44a347c3bba2b903929698428732719d6f1852d5))


### Bug Fixes

* **analytics:** null transaction_type keys and cross-state top_contributors ([a3ed8c8](https://github.com/Abstract-Data/campaignfinance-2023/commit/a3ed8c8b60fc5a411a2d89d99b15e7dfb3419452))
* **ci,quality,perf,refactor:** wave 1-5 remediation tasks — dependency scan, asyncio, code smells, loader state, field mappings ([74a5c24](https://github.com/Abstract-Data/campaignfinance-2023/commit/74a5c2421eed1a312a0d5fdb422eb34eada918d2))
* **ci,quality:** wave 1 remediation — scan, dead code, utcnow, typing ([039a892](https://github.com/Abstract-Data/campaignfinance-2023/commit/039a892becb98b8b18102a50755565dfab16ec22))
* **ci:** clear pre-existing ruff debt + fix x86 op-import collection crash ([826ece7](https://github.com/Abstract-Data/campaignfinance-2023/commit/826ece771e769d99cb7ebcc963932b2d74e353ef))
* **ci:** clear pre-existing ruff debt + fix x86 op-import collection crash ([3ee3860](https://github.com/Abstract-Data/campaignfinance-2023/commit/3ee38603df4edbffa1b92f346371e2823fe104a3))
* **ci:** exclude agent tooling + transform spike from ruff; revert transform reformat ([4cfc26a](https://github.com/Abstract-Data/campaignfinance-2023/commit/4cfc26ab331de1397f1157d84b6c8f4923ad3b6b))
* **ci:** fetch LFS for Tests job + make resolve smoke tests hermetic ([f299cd8](https://github.com/Abstract-Data/campaignfinance-2023/commit/f299cd8b1e34b1761b5f08e1b43541fe1b3c4639))
* **config:** skip unreadable CSVs in StateConfig file counts ([783004c](https://github.com/Abstract-Data/campaignfinance-2023/commit/783004c57aaa177a72ac512974952ee941332caf))
* **csv_reader:** restore read_parquet dropped from stack ([6e758ea](https://github.com/Abstract-Data/campaignfinance-2023/commit/6e758ea5c9f8fb438e46ce25179726f6e58a34c9))
* **db:** restore wave-3 split after collapse regression ([5533334](https://github.com/Abstract-Data/campaignfinance-2023/commit/5533334b68784577856333aa263c0e2308f51177))
* **db:** widen alembic_version.version_num for long revision ids ([579228b](https://github.com/Abstract-Data/campaignfinance-2023/commit/579228b7a72846272bea57d355b3fe2b053b08f9))
* **db:** widen alembic_version.version_num for long revision ids ([d1f8dff](https://github.com/Abstract-Data/campaignfinance-2023/commit/d1f8dff7770e5471a31eb512a7218a01160c2919))
* **ingest+resolve:** address 25%-subset test findings (schema drift, CAND, occupancy, loader cadence) ([5236eaa](https://github.com/Abstract-Data/campaignfinance-2023/commit/5236eaa61776c2b1e8c596ce475058ae3f8e485b))
* **ingest:** one-to-one entity representative assignment (blocker [#2](https://github.com/Abstract-Data/campaignfinance-2023/issues/2)) ([bfbb8bd](https://github.com/Abstract-Data/campaignfinance-2023/commit/bfbb8bd17f1f8f01d6a3d7ba55cad7180c3f3f30))
* **ingest:** org-person dedup keys on lower(org) alone (PG partial-index parity) ([094d108](https://github.com/Abstract-Data/campaignfinance-2023/commit/094d108f35f10c5ba394ebd173f4610d3cf58bbf))
* **ingest:** tighten individual person dedup key to (name + address) ([f040f72](https://github.com/Abstract-Data/campaignfinance-2023/commit/f040f72f7feed4a042450e70c6332152b29cf9eb))
* **ok:** correct OnePasswordItem import path ([c3273ed](https://github.com/Abstract-Data/campaignfinance-2023/commit/c3273edac99db871fe5d34f20697df1ec2009970))
* **ok:** defer Snowflake import and repair CategoryConfig ([9175a1d](https://github.com/Abstract-Data/campaignfinance-2023/commit/9175a1de388fb60e4ae8fda33b332f65b4671f8c))
* **ok:** resolve ok_contribution merge — four-level split clean ([a13f385](https://github.com/Abstract-Data/campaignfinance-2023/commit/a13f38525f2640291c67d8e321968b3e2b738249))
* **ok:** ruff I001/W291 in Oklahoma validators ([891fdf6](https://github.com/Abstract-Data/campaignfinance-2023/commit/891fdf63c34ba82162070db49b89cc78fac6db55))
* **op:** replace asyncio.run() in __init__ with async factory pattern ([da54cde](https://github.com/Abstract-Data/campaignfinance-2023/commit/da54cdee314005ba0f5eefab3724f4f5fa9d8f5c))
* **repository:** annotate get_*_versions return types ([f8bdbc4](https://github.com/Abstract-Data/campaignfinance-2023/commit/f8bdbc4627c668ed3c64f59173f48cdb94d1b07b))
* **repository:** type _get_versions with TVersion TypeVar ([15779cb](https://github.com/Abstract-Data/campaignfinance-2023/commit/15779cb1c6d24eff4975487420969e851820466c))
* **resolve,coverage:** warn on implicit SQLite fallback; clarify coverage threshold scale ([a34680f](https://github.com/Abstract-Data/campaignfinance-2023/commit/a34680fb2477b84bb19cebd28402b6d0ce9a3c41))
* **resolve:** clear canonical_campaign before canonical_entity on rerun ([de4b0fb](https://github.com/Abstract-Data/campaignfinance-2023/commit/de4b0fbe576e1dc5a89383932de7eedc9562e1bc))
* **resolve:** deterministically merge a unified_entity with its source person/committee ([1f306ff](https://github.com/Abstract-Data/campaignfinance-2023/commit/1f306ff1846de8291cd084422a2745a6fdf1b12e))
* **resolve:** don't crash stage-1 on a dirty address (occupancy type without identifier) ([20db984](https://github.com/Abstract-Data/campaignfinance-2023/commit/20db984b60050c4c092d303e3a56619d49d54f2e))
* **resolve:** don't crash stage-1 on a dirty address (occupancy type without identifier) ([fcc8794](https://github.com/Abstract-Data/campaignfinance-2023/commit/fcc8794620a05482eab1c560c921d9c9c4384be6))
* **resolve:** expose employer to the Splink frame (wave 3 integration) ([67f8ffe](https://github.com/Abstract-Data/campaignfinance-2023/commit/67f8ffe6635146fcc2e2930defd89048bd3e7107))
* **resolve:** merge a unified_entity with its source person/committee (cross-source dedup) ([7e550e4](https://github.com/Abstract-Data/campaignfinance-2023/commit/7e550e47415388be694c2c027f218d3f5ad2fc9b))
* **resolve:** self-healing migration for resolution_input linked_* columns ([08c193b](https://github.com/Abstract-Data/campaignfinance-2023/commit/08c193b8fcd038665a39e0f6c36d420bae5050af))
* review remediation waves 1–5 (phase branch) ([#15](https://github.com/Abstract-Data/campaignfinance-2023/issues/15)) ([f53e8a4](https://github.com/Abstract-Data/campaignfinance-2023/commit/f53e8a42031ab74dc957e4c734a9e7f36fdc38dc))
* **validators:** Texas validator parsing + numeric phone coercion ([c2b94b4](https://github.com/Abstract-Data/campaignfinance-2023/commit/c2b94b420d610f1c661899802a688ad5f794602c))
* **wave-2:** processor singleton, OK contribution split, op asyncio factory ([3534949](https://github.com/Abstract-Data/campaignfinance-2023/commit/3534949ceec4fc19ab057d3c0f0e5e59976c24c0))


### Performance Improvements

* cache full_address_lookup on FamilyContext (Wave 4) ([323bce8](https://github.com/Abstract-Data/campaignfinance-2023/commit/323bce8d9da9767ffd4b46830a6b6df9e8f8232d))
* **resolve:** bulk-load blocking pairs via UNLOGGED staging + sorted DISTINCT-ON promote ([dbff780](https://github.com/Abstract-Data/campaignfinance-2023/commit/dbff78041ecce76e10f09e122babd5019d75e019))
* **resolve:** chunk + narrow the DuckDB-&gt;PG scored_pairs write (fix OOM, 37-&gt;27.5min) ([594e5a9](https://github.com/Abstract-Data/campaignfinance-2023/commit/594e5a9470c38a65296d6d92224339e8a447df67))
* **resolve:** drop pair unique constraint around just the promote insert ([faca9f3](https://github.com/Abstract-Data/campaignfinance-2023/commit/faca9f394d0edc6833db85221881433e2ca0e7b6))
* **resolve:** drop scored_pairs indexes before the delete-of-prior-rows too ([2cdde31](https://github.com/Abstract-Data/campaignfinance-2023/commit/2cdde31d15bc30e1c1b5b86aae902c6c72ca2324))
* **resolve:** exact-edge-list scoring + drop/rebuild indexes around COPY ([e2d15f0](https://github.com/Abstract-Data/campaignfinance-2023/commit/e2d15f0eb71734ddffe7e8b990c0380c5b0c10dd))
* **resolve:** leave scored_pairs UNLOGGED for WAL-free bulk load ([ad20774](https://github.com/Abstract-Data/campaignfinance-2023/commit/ad2077448cdccd7aaeec614eca4d443391ce2c72))
* **resolve:** make stage-2 blocking scale to millions (kill 100M-pair explosion) ([c87f3f5](https://github.com/Abstract-Data/campaignfinance-2023/commit/c87f3f5d2af6aab09644fcd33b3d74b2d80d0889))
* **resolve:** make stage-2 blocking scale to millions (kill 100M-pair explosion) ([2b6f1e4](https://github.com/Abstract-Data/campaignfinance-2023/commit/2b6f1e46153d0cbecb73fe3f69f59754b907cfd1))
* **resolve:** stage candidate pairs via DuckDB append, not executemany ([06d8b0a](https://github.com/Abstract-Data/campaignfinance-2023/commit/06d8b0a648e7adffa53b910576f3d5e3985010c1))
* **resolve:** stream score stage to scale to millions of pairs ([055b1fc](https://github.com/Abstract-Data/campaignfinance-2023/commit/055b1fca8188c8aa3e2f25a0c5e5b627f3b59e3d))
* **resolve:** write scored_pairs via DuckDB ATTACH-&gt;Postgres (no per-row Python) ([4ac4609](https://github.com/Abstract-Data/campaignfinance-2023/commit/4ac460944794008aed969ce9a738df36031a95c3))
* **resolve:** write scored_pairs via PostgreSQL COPY, not executemany ([1bf4f60](https://github.com/Abstract-Data/campaignfinance-2023/commit/1bf4f604140136faf3faecdf309c395f5883b693))
* **transform:** scalable publish (drop/rebuild all indexes, FK-off) + --fraction loader ([6013ff1](https://github.com/Abstract-Data/campaignfinance-2023/commit/6013ff194394fc07cd7e24e3aa7f53a8abd1a5a3))


### Documentation

* add implementation plans and database bloat baseline ([06b6e06](https://github.com/Abstract-Data/campaignfinance-2023/commit/06b6e06291a42f71b794d352439837caf33250bf))
* **agents, remediation, loaders:** document SDD workflow, hardening plan, and subset-load improvements ([6be106d](https://github.com/Abstract-Data/campaignfinance-2023/commit/6be106d473cc053611c4e1845f92ac7ea152344c))
* expand AGENTS.md with agent scope, model config, and operational guardrails; restructure project docs ([b2d0d86](https://github.com/Abstract-Data/campaignfinance-2023/commit/b2d0d86b544c053311665eb4aca5f89b93522d1b))
* **ingest:** P1-P4 vectorized-ingest workflow spec + runnable script ([f838ac3](https://github.com/Abstract-Data/campaignfinance-2023/commit/f838ac360b2608c704b3eab516d7b2a694888276))
* **remediation:** add Run 2 COMPLETION.md backlog audit ([6f1ccc6](https://github.com/Abstract-Data/campaignfinance-2023/commit/6f1ccc69021d77123415b1c5ff961651feb85795))
* **resolve:** record con.append staging result (37min, 25x total) ([7431ca5](https://github.com/Abstract-Data/campaignfinance-2023/commit/7431ca590df81941600343bbfc4a1a470efedf4f))
* **resolve:** record full-run results for score-stage optimizations ([350391c](https://github.com/Abstract-Data/campaignfinance-2023/commit/350391c463aed38e2c756ff544fa5848df9df9e2))
* restructure AGENTS.md with versioning, scope, and tool permissions; refactor CI/CD to modular workflows; expand README with unified model and docs index ([aab6d7e](https://github.com/Abstract-Data/campaignfinance-2023/commit/aab6d7e473133d9b347871941c95a2a47b0a4568))
* **task:** blocker [#1](https://github.com/Abstract-Data/campaignfinance-2023/issues/1) — org-person dedup parity ([155de0c](https://github.com/Abstract-Data/campaignfinance-2023/commit/155de0cd65b1b94c53cd7437e05567b1458a558b))
* **task:** record verified Alembic baseline + remaining integration step ([8661c5e](https://github.com/Abstract-Data/campaignfinance-2023/commit/8661c5ef058a090ba467f21215f203be5a312de7))

## [Unreleased]

### Removed
- `app/resolve/staging.py` — atomic table swap helpers were tested but never wired
  to Stage 7 publish; survivorship uses delete-and-replace on live canonical tables.
