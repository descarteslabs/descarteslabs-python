[![Build Status](https://travis-ci.org/descarteslabs/descarteslabs-python.svg?branch=master)](https://travis-ci.org/descarteslabs/descarteslabs-python)

Descarteslabs
=============

The documentation for the latest release can be found at [https://docs.descarteslabs.com](https://docs.descarteslabs.com)

Changelog
=========

## [Unreleased]
### Changed
- Removed extra keyword arguments from places client

## [0.11.2] - 2018-08-24
### Changed
- Moved metadata property filtering to common
- Deprecated `create_or_get_function` in tasks
- Renamed some examples

## [0.11.1] - 2018-08-17
### Added
- Namespaced auth environment variables: `DESCARTESLABS_CLIENT_SECRET` and `DESCARTESLABS_CLIENT_ID`. `CLIENT_SECRET` and `CLIENT_ID` will continue to work.
- Tasks runtime check for Python version.

### Changed
- Documentation updates
- Example updates

## [0.11.0] - 2018-07-12
### Added
- Scenes package
- More examples

### Changed
- Deprecated `add_namespace` argument in catalog client (defaults to `False`
  now, formerly `True`)

## [0.10.1] - 2018-05-30
### Changed
- Added org to token scope
- Removed deprecated key usage

## [0.10.0] - 2018-05-17
### Added
- Tasks service

## [0.9.1] - 2018-05-17
### Changed
- Patched bug in catalog service for py3

## [0.9.0] - 2018-05-11
### Added
- Catalog service
- Storage service

## [0.8.1] - 2018-05-03
### Changed
- Switched to `start_datetime` argument pattern instead of `start_date`
- Fixed minor regression with `descarteslabs.ext` clients
- Deprecated token param for `Service` class

### Added
- Raster stack method

## [0.8.0] - 2018-03-29
### Changed
- Removed deprecated searching by `const_id`
- Removed deprecated raster band methods
- Deprecated `sat_id` parameter for metadata searches
- Changed documentation from readthedocs to https://docs.descarteslabs.com

### Added
- Dot notation access to dictionaries returned by services

## [0.7.0] - 2018-01-24
### Changed
- Reorganization into a client submodule

## [0.6.2] - 2018-01-10
### Changed
- Fix regression for `NotFoundError`

## [0.6.1] - 2018-01-09
### Changed
- Reverted `descarteslabs.services.base` to `descarteslabs.services.service`
  
## [0.6.0] - 2018-01-08
### Changed
- Reorganization of services
- Places updated to v2 backend, provides units interface to statistics, which
  carries some backwards incompatibility.

### Added
- Places updated to v2 backend, provides units interface to statistics, which
  carries some backwards incompatibility.

## [0.5.0] - 2017-10-31
### Added
- Blosc Support for raster array compression transport
- Scrolling support for large metadata searches

### Changes
- Offset keyword argument in metadata.search is deprecated. Please use the
metadata.features for iterating over large search results

## [0.4.7] - 2017-10-09
### Added
- Complex filtering expressions for image attributes

### Fixes
- Raise explicitly on 409 response
- Keep retrying token refresh until token fully expired
- Fixed race condition when creating `.descarteslabs` directory

## [0.4.6] - 2017-09-08
### Added
- Added ext namespace
- Metadata multi-get

### Fixes
- Fix OpenSSL install on OSX

## [0.4.5] - 2017-08-29
### Fixes
- Automatic retry on 504
- Internal API refactoring / improvements for Auth

## [0.4.4] - 2017-08-03
### Added
- Add raster bands methods to metadata service.
- Deprecate raster band methods.
- Add `require_bands` param to derived bands search method.

### Fixes
- Test suite replaces original token when finished running script tests.

## [0.4.3] - 2017-07-18
### Added
- Support for derived bands endpoints.
- Direct access to `const_id` to `product` translation.

### Fixes
- `descarteslabs` scripts on windows OS.

## [0.4.2] - 2017-07-05
### Fixes
- Fix auth login

## [0.4.1] - 2017-07-05
### Added
- Add metadata.bands and metadata.products search/get capabilities.
- Add bands/products descriptions
- Additional Placetypes

### Fixes
- Better error messages with timeouts
- Update to latest version of `requests`

## [0.4.0] - 2017-06-22
### Changes
- Major refactor of metadata.search
  * Introduction of "Products" through `Metadata.products()`
  * metadata entries id now concatenate the product id and the old metadata
    keys. The original metadata keys are available through entry['key'].
  * Additional sorting available.

### Added
- Search & Raster using DLTile Feature GeoJSON or key. Uses output bounds,
  resolution, and srs to ease searching and rasterizing imagery over tiles.

### Fixes
- Better Error messaging

## [0.3.3] - 2017-06-20
### Added
- DLTile notebook
- `save` and `outfile_basename` in `Raster.raster()`

### Fixes
- Fix metadata.features


## [0.3.2] - 2017-05-27
### Fixes
- Strict "requests" versions needed due to upstream instability.


## [0.3.1] - 2017-05-17
### Fixes
- Fix python 3 command line compatibility


## [0.3.0] - 2017-05-15
### Changed
- API Change `descarteslabs`, `raster`, `metadata` have all been merged into
 '`descarteslabs`'. '`descarteslabs login`' is now '`descarteslabs auth
 login`', '`raster`'' is now '`descarteslabs raster`', etc.

### Added
- A Changelog
- Testing around command-line scripts

### Fixes
- Searching with cloud\_fraction = 0
- dltile API documentation

## [0.2.2] - 2017-05-04
### Fixes
- Fix login bug
- Installation of "requests\[security\]" for python < 2.7.9

## [0.2.1] - 2017-04-18
### Added
- Doctests

### Fixes
- Python 3 login bug

## [0.2.0] - 2017-04-11
### Added
- Search by Fractions

## [0.1.0] - 2017-03-24
### Added
- Initial release of client library

[Unreleased]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.11.2...HEAD
[0.11.2]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.11.2...v0.11.2
[0.11.1]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.11.0...v0.11.1
[0.11.0]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.10.1...v0.11.0
[0.10.1]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.10.0...v0.10.1
[0.10.0]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.9.1...v0.10.0
[0.9.1]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.8.1...v0.9.0
[0.8.1]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.6.2...v0.7.0
[0.6.2]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.6.1...v0.6.2
[0.6.1]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.4.7...v0.5.0
[0.4.7]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.4.6...v0.4.7
[0.4.6]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.4.5...v0.4.6
[0.4.5]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.4.4...v0.4.5
[0.4.4]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.4.3...v0.4.4
[0.4.3]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.4.2...v0.4.3
[0.4.2]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.3.3...v0.4.0
[0.3.3]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.2.2...v0.3.0
[0.2.2]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/descarteslabs/descarteslabs-python/releases/tag/v0.1.0


