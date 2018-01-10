[![Build Status](https://travis-ci.org/descarteslabs/descarteslabs-python.svg?branch=master)](https://travis-ci.org/descarteslabs/descarteslabs-python)

Descarteslabs
=============

Services
--------

This package includes service wrappers for Descartes Labs application services that 
do require additional dependencies (included in `requirements.txt`) and are thus not 
implicitly included in the root package (see above). Service wrappers include, 
primarily, Raster (image access), Metadata (image metadata) and Places (named
shapes and statistics).  These services are authenticated and in order to setup
authentication there is a convenience script to help you log in.

```bash
$ python setup.py install
$ pip install -r requirements.txt
$ descarteslabs auth login
```

For non-interactive environments, one needs to set the `CLIENT_ID` and `CLIENT_SECRET` 
environment variables. These can be retrieved from the `~/.descarteslabs/token_info.json`
created from the login process or generated fresh through [IAM](https://iam.descarteslabs.com).

```bash
$ export CLIENT_ID=...
$ export CLIENT_SECRET=...
```

Documentation
-------------
The documentation for the latest release can be found on [readthedocs](http://descartes-labs-python.readthedocs.io/)

Changelog
=========

## [Unreleased]
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
  * Introduction of "Products" through dl.metadata.products()
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
- `save` and `outfile_basename` in raster()

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

[Unreleased]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.6.2...HEAD
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

FAQ
---

If you are on older versions of Python 2.7, you may encounter warnings about
SSL such as:

  InsecurePlatformWarning: A true SSLContext object is not
  available. This prevents urllib3 from configuring SSL appropriately and 
  may cause certain SSL connections to fail. For more information, see 
  https://urllib3.readthedocs.org/en/latest  
  /security.html#insecureplatformwarning.

Please follow the instructions from
[stackoverflow](http://stackoverflow.com/questions/29099404/ssl-insecureplatform-error-when-using-requests-package)
and install the `"requests[security]"` package with, e.g. `pip install
"requests[security]"`.

