# Changelog

All notable changes to this project will be documented in this file.

The format is based on "Keep a Changelog" (https://keepachangelog.com/en/1.0.0/)


## [Unreleased]
### Added
 - `s4h_export()`: the load step. Saves extracted or harmonized DataFrames as Parquet, CSV or tables in a DuckDB database, writing large data partition by partition. Accepts a DataFrame, a list, or a dict keyed by country.
 - New `duckdb` extra: `pip install "socio4health-core[duckdb]"` (included in `pip install socio4health`).
 - `prune_columns()`: keep only the columns you need before the data is loaded, so large surveys use much less memory and time.
 - `keep_variable_in_largest_table()`: when several tables repeat a variable (e.g. age), keep it only in the table that covers the whole population.
 - `extract_and_prepare_data(cache_parquet=True)` passes the Parquet cache option to `Extractor`.
 - `extractor_utils.s4h_parse_ibge_layout()`: read the fixed-width layout of a PNAD / PNAD Contínua file from the IBGE dictionary published with it, optionally only for some variables.
 - `s4h_vertical_merge(overlap_basis="larger")`: only merge tables with nearly the same columns. With the default (`"smaller"`), a small table whose columns are almost all in a larger one is stacked into it, e.g. a survey module made of household keys stacked into the person table.
### Fixed
 - `s4h_vertical_merge` stacks the tables of a group with the same upper-case column names it uses to group them. Columns that differ only in case (`Secuencia_p` / `SECUENCIA_P`) used to stay separate, each empty for the other files' rows.
 - `group_and_onehot_encode`: units without any weight now get missing shares instead of 0.
 - `merge_factor` no longer replaces the expansion factor of every table with the one of the first table. Rows from other files (e.g. other months) used to lose their weight. Tables keep their own factor and the others get it from every table that has it.
 - Column mappings can build a variable from several raw columns: a key like `"WS2|WS1": "H_WATER"` takes the first non-empty value, in that order (`harmonize_columns_by_year`; also `s4h_coalesce_columns()` and `mapping_source_columns()`). Use the same key in the value mapping.
 - `s4h_code_from_flags()`: build a categorical code from yes/no columns (one per option), for questions where the main option is only asked when several are "yes".
 - Coalesce keys keep the raw columns that other mapping keys still use.
 - `use_person_factor()`: weight person-level tables with the population factor (e.g. ENAHO `FACPOB07`) and household tables with the household factor (`FACTOR07`).
 - `merge_factor(fallback_col=...)`: rows that still have no factor take it from another column (e.g. `FEX_C` when the `FEX_C18` projection file does not cover them).
 - `merge_factor` accepts several ID columns (e.g. `["CONGLOME", "VIVIENDA", "HOGAR"]`) when no single column identifies the household.
 - `harmonize_columns_by_year`: when a raw column already has the harmonized name of a mapped column (e.g. a raw `AREA` while `CLASE` is mapped to `AREA`), the mapped column now replaces it. Before, both columns were removed.
 - Duplicate column names are resolved by keeping the first column instead of removing all of them.
 - Category codes with leading zeros (`"02"`) now match mapping keys without them (`2`) and the other way round.
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
