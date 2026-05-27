# Changelog

## 0.1.0 (2026-05-27)


### Features

* **infra:** Dockerfile, Splink pin, scraper drift, PII policy ([800ed94](https://github.com/Abstract-Data/campaignfinance-2023/commit/800ed94c3306f1a58f3bb6513e5954c476d9f2a0))
* **resolve:** add postgres sql blocking backend for stage 2 ([b4edf57](https://github.com/Abstract-Data/campaignfinance-2023/commit/b4edf57c4bbce9fcbdb87942894b450a6c5c09de))


### Bug Fixes

* **analytics:** null transaction_type keys and cross-state top_contributors ([a3ed8c8](https://github.com/Abstract-Data/campaignfinance-2023/commit/a3ed8c8b60fc5a411a2d89d99b15e7dfb3419452))
* **ci,quality,perf,refactor:** wave 1-5 remediation tasks — dependency scan, asyncio, code smells, loader state, field mappings ([74a5c24](https://github.com/Abstract-Data/campaignfinance-2023/commit/74a5c2421eed1a312a0d5fdb422eb34eada918d2))
* **ci,quality:** wave 1 remediation — scan, dead code, utcnow, typing ([039a892](https://github.com/Abstract-Data/campaignfinance-2023/commit/039a892becb98b8b18102a50755565dfab16ec22))
* **config:** skip unreadable CSVs in StateConfig file counts ([783004c](https://github.com/Abstract-Data/campaignfinance-2023/commit/783004c57aaa177a72ac512974952ee941332caf))
* **csv_reader:** restore read_parquet dropped from stack ([6e758ea](https://github.com/Abstract-Data/campaignfinance-2023/commit/6e758ea5c9f8fb438e46ce25179726f6e58a34c9))
* **db:** restore wave-3 split after collapse regression ([5533334](https://github.com/Abstract-Data/campaignfinance-2023/commit/5533334b68784577856333aa263c0e2308f51177))
* **ok:** correct OnePasswordItem import path ([c3273ed](https://github.com/Abstract-Data/campaignfinance-2023/commit/c3273edac99db871fe5d34f20697df1ec2009970))
* **ok:** defer Snowflake import and repair CategoryConfig ([9175a1d](https://github.com/Abstract-Data/campaignfinance-2023/commit/9175a1de388fb60e4ae8fda33b332f65b4671f8c))
* **ok:** resolve ok_contribution merge — four-level split clean ([a13f385](https://github.com/Abstract-Data/campaignfinance-2023/commit/a13f38525f2640291c67d8e321968b3e2b738249))
* **ok:** ruff I001/W291 in Oklahoma validators ([891fdf6](https://github.com/Abstract-Data/campaignfinance-2023/commit/891fdf63c34ba82162070db49b89cc78fac6db55))
* **op:** replace asyncio.run() in __init__ with async factory pattern ([da54cde](https://github.com/Abstract-Data/campaignfinance-2023/commit/da54cdee314005ba0f5eefab3724f4f5fa9d8f5c))
* **repository:** annotate get_*_versions return types ([f8bdbc4](https://github.com/Abstract-Data/campaignfinance-2023/commit/f8bdbc4627c668ed3c64f59173f48cdb94d1b07b))
* **repository:** type _get_versions with TVersion TypeVar ([15779cb](https://github.com/Abstract-Data/campaignfinance-2023/commit/15779cb1c6d24eff4975487420969e851820466c))
* review remediation waves 1–5 (phase branch) ([#15](https://github.com/Abstract-Data/campaignfinance-2023/issues/15)) ([f53e8a4](https://github.com/Abstract-Data/campaignfinance-2023/commit/f53e8a42031ab74dc957e4c734a9e7f36fdc38dc))
* **wave-2:** processor singleton, OK contribution split, op asyncio factory ([3534949](https://github.com/Abstract-Data/campaignfinance-2023/commit/3534949ceec4fc19ab057d3c0f0e5e59976c24c0))


### Documentation

* expand AGENTS.md with agent scope, model config, and operational guardrails; restructure project docs ([b2d0d86](https://github.com/Abstract-Data/campaignfinance-2023/commit/b2d0d86b544c053311665eb4aca5f89b93522d1b))
* **remediation:** add Run 2 COMPLETION.md backlog audit ([6f1ccc6](https://github.com/Abstract-Data/campaignfinance-2023/commit/6f1ccc69021d77123415b1c5ff961651feb85795))
* restructure AGENTS.md with versioning, scope, and tool permissions; refactor CI/CD to modular workflows; expand README with unified model and docs index ([aab6d7e](https://github.com/Abstract-Data/campaignfinance-2023/commit/aab6d7e473133d9b347871941c95a2a47b0a4568))
