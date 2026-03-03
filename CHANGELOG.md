# Changelog

All notable changes to this project will be documented in this file.

The format is based on "Keep a Changelog" (https://keepachangelog.com/en/1.0.0/)

## [Unreleased]
- Prepare improvements and documentation updates.

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
