[![Build Status](https://travis-ci.org/descarteslabs/descarteslabs-python.svg?branch=master)](https://travis-ci.org/descarteslabs/descarteslabs-python)

Descarteslabs
=============

Services
--------

This package includes service wrappers for Descartes Labs application services that 
do require additional dependencies (included in requirements.txt) and are thus not 
implicitly included in the root package (see above). Service wrappers include, 
primarily, Metadata (image metadata) and Places (named shapes and statistics). 
These services are authenticated and in order to setup authentication there is a 
convenience script to help you log in.

```bash
$ python setup.py install
$ pip install -r requirements.txt
$ descarteslabs auth login
```

For non-interactive environments, one needs to set the CLIENT_ID and CLIENT_SECRET 
environment variables. These can be retrieved from the ~/.descarteslabs/token_info.json
created from the login process or generated fresh through through [IAM](https://iam.descarteslabs.com).

```bash
$ export CLIENT_ID=...
$ export CLIENT_SECRET=...
```

Documentation
-------------
The latest build of the documentation can be found on [readthedocs](http://descartes-labs-python.readthedocs.io/en/latest/)

Changelog
=========

## [Unreleased]

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

[Unreleased]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.3.1...HEAD
[0.3.1]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.3.2...v0.3.1
[0.3.0]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.3.2...v0.3.0
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

