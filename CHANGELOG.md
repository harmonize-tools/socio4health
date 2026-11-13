# Changelog

All notable changes to this project will be documented in this file.

The format is based on "Keep a Changelog" (https://keepachangelog.com/en/1.0.0/)


## [Unreleased]
### Added
 - `s4h_export()`: the load step. Saves extracted or harmonized DataFrames as Parquet, CSV or tables in a DuckDB database, writing large data partition by partition. Accepts a DataFrame, a list, or a dict keyed by country.
 - New `duckdb` extra: `pip install "socio4health-core[duckdb]"` (included in `pip install socio4health`).
### Fixed
 - `s4h_vertical_merge` no longer modifies the DataFrames it receives: calling it twice on the same list gives the same result.
 - `s4h_vertical_merge` keeps the column order of the input tables (it used to change between runs).
 - `s4h_vertical_merge` rejects an invalid `method` right away instead of ignoring it when there is nothing to merge.

## [1.0.14] - 2026-11-10
### Added
 - `Extractor(cache_parquet=True)`: converts text files to Parquet once, so later reads are much faster.
 - `Extractor(file_pattern=...)`: only read the extracted files whose name matches a regex.
 - `s4h_set_log_level()`: choose how much socio4health prints (`"DEBUG"`, `"INFO"`, `"WARNING"`...).
### Changed
 - Cleaner logs: one line per step instead of one per file, and progress bars are no longer broken by log messages.
 - Info messages no longer appear in red in notebooks (only warnings and errors do).
 - `down_ext` is case-insensitive: `['.csv', '.zip']` also matches `.CSV` and `.ZIP` files.
 - Archives are much faster to extract: only files with the requested extensions are unpacked, and files already extracted are skipped.
### Fixed
 - Extraction no longer reads every file completely just to check that it is not empty.
 - Downloads no longer hang forever: they have a timeout and are retried 3 times. A failed download no longer crashes the extraction.
 - Files that were already fully downloaded are not downloaded again.
 - Brazil example: only reads household files (`Domicilios`), which match the dictionary.

## [1.0.13] - 2026-11-10
### Changed
 - `pip install socio4health` now installs everything (scraping, ml and geo).
 - New `socio4health-core` package for a basic install, with optional extras: `pip install "socio4health-core[geo]"`.
 - If upgrading from 1.0.12 or earlier, run `pip uninstall socio4health` first.

## [1.0.12] - 2026-09-10
### Fixed
 - Pandas, And Scrappy dependencies fix

## [1.0.11] - 2026-09-10
### Fixed
 - Translation Bug

## [1.0.10] - 2026-09-10
### Fixed
 - Scrappy depencency

## [1.0.5] - 2026-03-03
### Fixed
 - Fix normalization of columns

## [1.0.4] - 2026-03-02
### Fixed
 - Remove the similarity_threshold attribute and its property/setter,
 - Normalize column names (upper/strip) when comparing.
### Added
 - Replace with an overlap_threshold parameter using the Szymkiewicz–Simpson coefficient.
 - Method parameter on s4h_vertical_merge (supports 'union' and 'intersection').


## [1.0.3] - 2026-02-23
### Fixed
- Extractor now decompress deflated64.
- Sav files reader now supports extractors's encoding attributute as parameter.

## [1.0.2] - 2026-02-23
### Added
- Extractor now can read .sav files.
- Parameter to extractor to delete zip after extract.

## [1.0.1] - 2026-02-20
### Fixed
- Each extracted compressed file is now stored in its own independent folder.

## [1.0.0] - 2025-10-22
### Added
- Project now includes changelog linked from packaging metadata.
- Minor documentation updates.

### Fixed
- Packaging metadata clarified in `setup.py`.

## [0.1.7] - 2024-06-01
### Added
- Initial public release notes placeholder.



## [Unreleased]: https://github.com/harmonize-tools/socio4health/compare/v1.0.0...HEAD
## [1.0.0]: https://github.com/harmonize-tools/socio4health/compare/v0.1.7...v1.0.0
## [0.1.7]: https://github.com/harmonize-tools/socio4health/releases/tag/v0.1.7
