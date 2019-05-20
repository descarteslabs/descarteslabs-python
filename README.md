[![Build Status](https://travis-ci.org/descarteslabs/descarteslabs-python.svg?branch=master)](https://travis-ci.org/descarteslabs/descarteslabs-python)

Descarteslabs
=============

The documentation for the latest release can be found at [https://docs.descarteslabs.com](https://docs.descarteslabs.com)

Changelog
=========

## [Unreleased]

### Added
- Metadata, Catalog, and Scenes now support a new `storage_state` property for managing image metadata and filtering search results. `storage_state="available"` is the default for new images and indicates that the raster data for that scene is available on the Descartes Labs platform. `storage_state="remote"` indicates that the raster data has not yet been processed and made available to client users.

### Changed
- Bands of different but compatible types can now be rastered together in `Scene.ndarray()` and `Scene.download()` as well as across multiple scenes in `SceneCollection.mosaic()`, `SceneCollection.stack()` and `SceneCollection.download()`. The result will have the most general data type.

### Fixed

## [0.19.0] - 2019-05-06
### Changed
- Removed deprecated method `Metadata.sources()`
- `FeatureCollection.filter(geometry)` will now raise an `InvalidQueryException` if you
  try to overwrite an existing geometry in the filter chain.  You can only set the
  geometry once.

### Fixed

## [0.18.0] - 2019-04-18
### Changed
- Many old and obsolete examples were removed from the package.
- `Scene.ndarray`, `SceneCollection.stack`, and `SceneCollection.mosaic` now will automatically mask alpha if the alpha band is available in the relevant scene(s), and will set `mask_alpha` to `False` if the alpha band does not exist.
- `FeatureCollection.add`, `FeatureCollection.upload`, `Vector.create_feature`, `Vector.create_features`, and `Vector.upload_features` all accept a `fix_geometry` string argument that determines how to handle certain problem geometries
including those which do not follow counter-clockwise winding order (which is required by the GeoJSON spec but not many
popular tools). Allowed values are ``reject`` (reject invalid geometries with an error), ``fix`` (correct invalid
geometries if possible and use this corrected value when creating the feature), and ``accept`` (the default) which will
correct the geometry for internal use but retain the original geometry in the results.
- `Vector.get_upload_results` and `Vector.get_upload_result` now accept a `pending` parameter to include pending uploads
in the results. Such pending results will have `status: PENDING` and, in lieu of a task id, the `id` attribute will contain
the upload id as returned by `Vector.upload_features`
- `UploadTask.status` no longer blocks until the upload task is completed, but rather returns the current status of the
upload job, which may be `PENDING`, `RUNNING`, `SUCCESS`, or `FAILURE`.
- The `FutureTask.ready` and `UploadTask.ready` property has been added to test whether the task has completed.
A return value of `True` means that if `get_result(wait=True)` were to be called, it would return without blocking.
- You can now export features to a storage `data` blob.  To export from the
`vector` client, use `Vector.export_product_from_query()` with a storage key
and an optional query.  This returns the task id of the export task.  You
can ask for status using `Vector.get_export_results()` for all export tasks
or `Vector.get_export_result()` for a specific task by task id.
- FeatureCollection has been extended with this functionality with a
`FeatureCollection.export()` method that takes a storage key.  This operates
on the filter chain that FeatureCollection represents, or the full product
if there is no filter chain.  It returns an `ExportTask` which behaves
similar to the `FutureTask`.
- `Catalog.upload_image()` and `Catalog.upload_ndarray()` now will return an `upload_id` that can be used to query the status of that upload using `Catalog.upload_result()`.  Note that the upload id is the image id and if you use identical image ids `Catalog.upload_result()` will only show the result of the most recent upload.

### Fixed
- Several typical kinds of non-conforming GeoJSON which previously caused errors can now be accepted or
fixed by the `FeatureCollection` and `Vector` methods for adding or uploading new vector geometries.

## [0.17.3] - 2019-03-06
### Changed
- Fixed issues with `Catalog.upload_ndarray()` under Windows
- Added header to client requests to better debug retries

### Fixed
- Improved error messages for Catalog client upload methods

## [0.17.2] - 2019-02-26
### Changed
- Tasks methods `create_function`, `create_or_get_function`, and `new_group` now have image as a required parameter
- The `name` parameter is renamed to `product_id` in `Vector.create_product`, and `FeatureCollection.create` and `FeatureCollection.copy`.  The 'name' parameter is renamed to `new_product_id` in `Vector.create_product_from_query`.  Using `name` will continue to work, but will be removed completely in future versions.
- The `name` parameter is no longer required, and is ignored for `Vector.replace_product`, `Vector.update_product`, `FeatureCollection.update` and `FeatureCollection.replace`.  This parameter will be removed completely in future versions.

### Added
- `Metadata.paged_search` has been added and essentially supports the original behavior of `Metadata.search` prior to release 0.16.0.
This method should generally be avoided in favor of `Metadata.features` (or `Metadata.search`).

## [0.17.1] - 2019-02-11
### Added

### Changed
- Fixed typo in `UploadTask.status` which caused exception when handling certain failure conditions
- `FeatureCollection.upload` parameter `max_errors` was not being passed to Vector client.
- Ensure `cloudpickle==0.4.0` is version used when creating `Tasks`.
- Eliminate redundant queries from `FeatureCollection.list`.

## [0.17.0] - 2019-02-07
### Added
- `FeatureCollection.upload` and `Vector.upload_features` now accept an optional `max_errors` parameter to control how many errors are acceptable before declaring an upload a failure.
- `UploadTask` (as returned by `FeatureCollection.upload` and `Vector.list_uploads`) now has added attributes to better identify what was processed and what errors occurred.
- `Storage` now has added methods `set_file` and `get_file` to allow for better uploading and downloading, respectively, of large files.
- `Storage` class now has an `exists()` method that checks whether an object exists in storage at the location of a given `key` and returns a boolean.
- `Scenes.search` allows `limit=None`
- `FeatureCollection.delete_features` added to support deleting `Feature`s that match a `filter`
- `FeatureCollection.delete_features` and `FeatureCollection.wait_for_copy` now use `AsyncJob` to poll for asynchronous job completion. 
- `Vector.delete_features_from_query` and `Vector.get_delete_features_status` added to support new `FeatureCollection` and `AsyncJob` methods.

### Changed
- Fixed tasks bugs when including modules with relative paths in `sys.path`

## [0.16.0] - 2019-01-28
### Added
- Tasks now support passing modules, data and requirements along with the function code, allowing for a more complex and customized execution environment.
- Vector search query results now report their total number of results by means of the standard `len()` function.

### Changed
- `Metadata.search` no longer has a 10,000-item limit, and the number of items returned will be closer to `limit`. This
method no longer accepts the `continuation_token` parameter.

## [0.15.0] - 2019-01-09
### Added
- Raster client can now handle arbitrarily large numbers of tiles generated from a shape using the new `iter_dltiles_from_shape()` method which allows you to iterate over large numbers of tiles in a time- and memory-efficient manner. Similarly the existing `dltiles_from_shape()` method can now handle arbitrarily large numbers of tiles although it can be very slow.
- Vector client `upload_features()` can now upload contents of a stream (e.g. `io.IOBase` derivative such as `io.StringIO`) as well as the contents of a named file.
- Vector FeatureCollection `add()` method can now handle an arbitrary number of Features. Use of the `upload_features()` method is still encouraged for large collections.
- Vector client now supports creating a new product from the results of a query against an existing product with the `create_product_from_query()` method. This support is also accessible via the new `FeatureCollection.copy()` method.
- XYZTile GeoContext class, helpful for rendering to web maps that use XYZ-style tiles in a spherical Mercator CRS.

### Changed
- Tasks client FutureTask now instantiates a client if none provided (the default).
- Catalog client methods now properly handle `add_namespace` parameter.
- Vector Feature now includes valid geojson type 'Feature'.
- Tasks client now raises new GroupTerminalException if a task group stops accepting tasks.
- General documentation fixes.

## [0.14.1] - 2018-11-26
### Added
- Scenes and raster clients have a `processing_level` parameter that can be used to turn on surface reflectance processing for products that support it

## [0.14.0] - 2018-11-07
### Changed
- `scenes.GeoContext`: better defaults and `bounds_crs` parameter
  - `bounds` are no longer limited to WGS84, but can be expressed in any `bounds_crs`
  - New `Scene.default_ctx` uses a Scene's `geotrans` to more accurately determine a `GeoContext` that will result in no warping of the original data, better handling sinusoidal and other non-rectilinear coordinate reference systems.
  - **Important:** the default GeoContexts will now return differently-sized rasters than before!
    They will now be more accurate to the original, unwarped data, but if you were relying on the old defaults, you should now explicitly set the `bounds` to `geometry.bounds`,
    `bounds_crs` to `"EPSG:4326"`, and `align_pixels` to True.
- `Scene.coverage` and `SceneCollection.filter_coverage` accept any geometry-like object, not just a `GeoContext`.

## [0.13.2] - 2018-11-06
### Changed
- `FutureTask` inheritance changed from `dict` to `object`.

### Added
- Can now specify a GPU parameter for tasks.
- `Vectors.upload` allows you to upload a JSON newline delimited file.
- `Vectors.list_uploads` allows you to list all uploads for a vector product.
- `UploadTask` contains the information about an upload and is returned by both methods.

## [0.13.1] - 2018-10-16
### Changed
- `Vector.list_products` and `Vector.search_features` get `query_limit` and `page_size` parameters.

### Fixed
- `Vector.upload_features` handles new response format.

### Added
- Vector client support for retrieving status information about upload jobs. Added methods `Vector.get_upload_results` and `Vector.get_upload_result`.

## [0.13.0] - 2018-10-05
### Changed
- Shapely is now a full requirement of this package. Note: Windows users should visit https://docs.descarteslabs.com/installation.html#windows-users for installation guidance.
- Reduced the number of retries for some failure types.
- Resolved intermittent `SceneCollection.stack` bug that manifested as `AttributeError: 'NoneType' object has no attribute 'coords'` due to Shapely thread-unsafety.
- Tracking system environment to improve installation and support of different systems.

### Added
- The vector service is now part of the public package. See `descarteslabs.vectors` and `descarteslabs.client.services.vector`.


## [0.12.0] - 2018-09-12
### Changed
- Fixed SSL problems when copying clients to forked processes or sharing them among threads
- Removed extra keyword arguments from places client
- Added deprecation warnings for parameters that have been renamed in the Metadata client
- Scenes now exposes more parameters from raster and metadata
- Scenes `descarteslabs.scenes.search` will take a python datetime object in addition to a string
- Scenes will now allow Feature and FeatureCollection in addition to GeoJSON geometry types
- Fixed Scenes issue preventing access to products with multi-byte data but single-byte alpha bands

### Added
- `Scene.download`, `SceneCollection.download`, and `SceneCollection.download_mosaic` methods
- Colormaps supported in `descarteslabs.scenes.display`
- Task namespaces are automatically created with the first task group


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

[Unreleased]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.17.3...HEAD
[0.17.3]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.17.2...v0.17.3
[0.17.2]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.17.1...v0.17.2
[0.17.1]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.17.0...v0.17.1
[0.17.0]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.16.0...v0.17.0
[0.16.0]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.15.0...v0.16.0
[0.15.0]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.14.1...v0.15.0
[0.14.1]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.14.0...v0.14.1
[0.14.0]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.13.2...v0.14.0
[0.13.2]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.13.1...v0.13.2
[0.13.1]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.13.0...v0.13.1
[0.13.0]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.12.0...v0.13.0
[0.12.0]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.11.2...v0.12.0
[0.11.2]: https://github.com/descarteslabs/descarteslabs-python/compare/v0.11.21...v0.11.2
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
