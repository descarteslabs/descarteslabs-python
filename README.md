[![Build Status](https://github.com/descarteslabs/descarteslabs-python/actions/workflows/public-ci.yml/badge.svg)](https://github.com/descarteslabs/descarteslabs-python/actions/workflows/public-ci.yml)

Descartes Labs Platform
=======================

The Descartes Labs Platform is designed to answer some of the world’s most complex and pressing geospatial analytics questions. Our customers use the platform to build algorithms and models that transform their businesses quickly, efficiently, and cost-effectively.

By giving data scientists and their line-of-business colleagues the best geospatial data and modeling tools in one package, we help turn AI into a core competency.

Data science teams can use our scaling infrastructure to design models faster than ever, using our massive data archive or their own.

Please visit [https://descarteslabs.com](https://descarteslabs.com) for more information about the Descartes Labs Platform and to request access.

The `descarteslabs` python package, available at [https://pypi.org/project/descarteslabs/](https://pypi.org/project/descarteslabs/), provides client-side access to the Descartes Labs Platform for our customers. You must be a registered customer with access to our Descartes Labs Platform before you can make use of this package with our platform.

The documentation for the latest release can be found at [https://docs.descarteslabs.com](https://docs.descarteslabs.com). For any issues please request Customer Support at [https://support.descarteslabs.com](https://support.descarteslabs.com).

Changelog
=========

## [2.1.1] - 2023-10-16

## Compute

- Filtering on datetime attributes (such as `Function.created_at`) didn't previously work with anything
  but `datetime` instances. Now it also handles iso format strings and unix timestamps (int or float).

## [2.1.0] - 2023-10-04

## General

- Following our lifecycle policy, client versions v1.11.0 and earlier are no longer supported. They may
  cease to work with the Platform at any time.
  
## Catalog

- The Catalog `Blob` class now has a `get_data()` method which can be used to retrieve the blob
  data directly given the id, without having to first retrieve the `Blob` metadata.

## Compute

- *Breaking Change* The status values for `Function` and `Job` objects have changed, to provide a
  better experience managing the flow of jobs. Please see the updated Compute guide for a full explanation.
  Because of the required changes to the back end, older clients (i.e. v2.0.3) are supported in a
  best effort manner. Upgrading to this new client release is strongly advised for all users of the
  Compute service.

- *Breaking Change* The base images for Compute have been put on a diet. They are now themselves built
  from "slim" Python images, and they no longer include the wide variety of extra Python packages that were
  formerly included (e.g. TensorFlow, SciKit Learn, PyTorch). This has reduced the base image size by
  an order of magnitude, making function build times and job startup overhead commensurately faster.
  Any functions which require such additional packages can add them in as needed via the `requirements=`
  parameter. While doing so will increase image size, it will generally still be much smaller and faster
  than the prior "Everything and the kitchen sink" approach. Existing Functions with older images will continue
  to work as always, but any newly minted `Function`` using the new client will be using one of the new
  slim images.

- Base images are now available for Python3.10 and Python3.11, in addition to Python3.8 and Python3.9.

- Job results and logs are now integrated with Catalog Storage, so that results and logs can be
  searched and retrieved directly using the Catalog client as well as using the methods in the Compute
  client. Results are organized under `storage_type=StorageType.COMPUTE`, while logs are organized under
  `storage_type=StorageType.LOGS`.

- The new `ComputeResult` class can be used to wrap results from a `Function`, allowing the user to
  specify additional attributes for the result which will be stored in the Catalog `Blob` metadata for
  the result. This allows the function to specify properties such as `geometry`, `description`,
  `expires`, `extra_attributes`, `writers` and `readers` for the result `Blob`. The use of
  `ComputeResult` is not required.

- A `Job` can now be assigned arbitrary tags (strings), and searched based on them.

- A `Job` can now be retried on errors, and jobs track error reasons, exit codes, and execution counts.

- `Function` and `Job` objects can now be filtered by class attributes (ex. 
  `Job.search().filter(Job.status == JobStatus.PENDING).collect()`).

- The `Job.cancel()` method can now be used to cancel the execution of a job which is currently
  pending or running. Pending jobs will immediately transition to `JobStatus.CANCELED` status,
  while running jobs will pass through `JobStatus.CANCEL` (waiting for the cancelation to be
  signaled to the execution engine), `JobStatus.CANCELING` (waiting for the execution to terminate),
  and `JobStatus.CANCELED` (once the job is no longer executing). Cancelation of running jobs is
  not guaranteed; a job may terminate successfully, or with a failure or timeout, before it can
  be canceled.

- The `Job.result()` method will raise an exception if the job does not have a status of
  `JobStatus.SUCCESS`. If `Job.result()` yields an `None` value, this means that there was no
  result (i.e. the execution returned a `None`).

- The `Job.result_blob()` method will return the Catalog Storage Blob holding the result, if any.

- The `Job.delete()` method will delete any job logs, but will not delete the job result unless
  the `delete_results` parameter is supplied.
  
- The `Function` object now has attributes `namespace` and `owner`.

- The `Function.wait_for_completion()` and new `Function.as_completed()` methods provide a richer
  set of functionality for waiting on and handling job completion.

- The `Function.build_log()` method now returns the log contents as a string, rather than printing
  the log contents.

- The `Job.log()` method now returns the log contents as a list of strings, rather than printing the log
  contents. Because logs can be unbounded in size, there's also a new `Job.iter_log()` method which returns
  an iterator over the log lines.
  
- The `requirements=` parameter to `Function` objects now supports more `pip` magic, allowing the use
  of special `pip` controls such as `-f`. Also parsing of package versions has been loosened to allow
  some more unusual version designators.

- Changes to the `Function.map()` method, with the parameter name change of `iterargs` changed to `kwargs`
  (the old name is still honored but deprecated), corrected documentation, and enhancements to support more
  general iterators and mappings, allowing for a more functional programming style.

- The compute package was restructured to make all the useful and relevant classes available at the top level.

## Utils

- Property filters can now be deserialized as well as serialized.

## [2.0.3] - 2023-07-13

### Compute

- Allow deletion of `Function` objects.
  - Deleting a Function will deleted all associated Jobs.
- Allow deletion of `Job` objects.
  - Deleting a Job will delete all associated resources (logs, results, etc). 
- Added attribute filter to `Function` and `Job` objects.
  - Attributes marked `filterable=True` can be used to filter objects on the compute backend api.
  - Minor optimization to `Job.iter_results` which now uses backend filters to load successful jobs.
- `Function` bundling has been enhanced.
  - New `include_modules` and `include_data` parameters allow for multiple other modules, non-code data files, etc to be added to the code bundle.
  - The `requirements` parameter has been improved to allow a user to pass a path to their own `requirements.txt` file instead of a list of strings.

## [2.0.2] - 2023-06-26

### Catalog

- Allow data type `int32` in geotiff downloads.
- `BlobCollection` now importable from `descarteslabs.catalog`.

### Documentation

- Added API documentation for dynamic compute and vector

## [2.0.1] - 2023-06-14

### Raster

- Due to recent changes in `urllib3`, rastering operations were failing to retry certain errors which ought to be retried, causing more failures to propagate to the user than was desirable. This is now fixed.

## [2.0.0] - 2023-06-12

(Release notes from all the 2.0.0 release candidates are summarized here for completeness.)

### Supported platforms

- Deprecated support for Python 3.7 (will end of life in July).
- Added support for Python 3.10 and Python 3.11
- AWS-only client. For the time being, the AWS client can be used to communicate with the legacy GCP platform (e.g. `DESCARTESLABS_ENV=gcp-production`) but only supports those services that are supported on AWS (`catalog` and `scenes`). This support may break at any point in the future, so it is strictly transitional.

### Dependencies

- Removed many dependencies no longer required due to the removal of GCP-only features.
- Added support for Shapely 2.X. Note that user code may also be affected by breaking changes in
  Shapely 2.X. Use of Shapely 1.8 is still supported.
- Updated requirements to avoid `urllib3>=2.0.0` which breaks all kinds of things.

### Configuration

- Major overhaul of the internals of the config process. To support other clients using namespaced packages within the `descarteslabs` package, the top level has been cleaned up, and most all the real code is down inside `descarteslabs.core`. End users should never have to import anything from `descarteslabs.core`. No more magic packages means that `pylint` will work well with code using `descarteslabs`.
- Configuration no longer depends upon the authorized user.

### Catalog

- Added support for data storage. The `Blob` class provides mechanism to upload, index, share, and retrieve arbitrary byte sequences (e.g. files). `Blob`s can be searched by namespace and name, geospatial coordinates (points, polygons, etc.), and tags. `Blob`s can be downloaded to a local file, or retrieved directly as a Python `bytes` object. `Blob`s support the same sharing mechanisms as `Product`s, with `owners`, `writers`, and `readers` attributes.
- Added support to `Property` for `prefix` filtering.
- The default `geocontext` for image objects no longer specifies a `resolution` but rather a `shape`, to ensure
  that default rastering preserves the original data and alignment (i.e. no warping of the source image).
- As with `resolution`, you can now pass a `crs` parameter to the rastering methods (e.g. `Image.ndarray`,
  `ImageCollection.stack`, etc.) to override the `crs` of the default geocontext.
- A bug in the code handling the default context for image collections when working with a product with a CRS based on degrees rather than meters has been fixed. Resolutions should always be specified in the units used by the CRS.

### Compute

- Added support for managed batch compute under the `compute` module.

### Raster Client

- Fixed a bug in the handling of small blocks (less than 512 x 512) that caused rasterio to generate bad download files (the desired image block would appear as a smaller sub-block rather than filling the resulting raster).

### Geo

- The defaulting of `align_pixels` has changed slightly for the `AOI` class. Previously it always defaulted to
  `True`. Now the default is `True` if `resolution` is set, `False` otherwise. This ensures that when specifying
  a `shape` and a `bounds` rather than a resolution,the `shape` is actually honored.
- When assigning a `resolution` to an `AOI`, any existing `shape` attribute is automatically unset, since the
  two attributes are mutually exclusive.
- The validation of bounds for a geographic CRS has been slightly modified to account for some of the
  irregularities of whole-globe image products, correcting unintended failures in the past.
- Fixed problem handling MultiPolygon and GeometryCollection when using Shapely 2.0.


## [2.0.0rc5] - 2023-06-01

### Catalog

- Loosen up the restrictions on the allowed alphabet for Blob names. Now almost any printable
  character is accepted save for newlines and commas.
- Added new storage types for Blobs: `StorageType.COMPUTE` (for Compute job results) and
  `StorageType.DYNCOMP` (for saved `dynamic-compute` operations).

### Compute

- Added testing of the client.

## [2.0.0rc4] - 2023-05-17

### Catalog

- The defaulting of the `namespace` value for `Blob`s has changed slightly. If no namespace is specified,
  it will default to `<org>:<hash>` with the user's org name and unique user hash. Otherwise, any other value,
  as before, will be prefixed with the user's org name if it isn't already so.
- `Blob.get` no longer requires a full id. Alternatively, you can give it a `name` and optionally a `namespace`
  and a `storage_type`, and it will retrieve the `Blob`.
- Fixed a bug causing summaries of `Blob` searches to fail.

### Compute

- `Function.map` and `Function.rerun` now save the created `Job`s before returning.
- `Job.get` return values fixed, and removed an extraneous debug print.

### General

- Updated requirements to avoid `urllib3>=2.0.0` which break all kinds of things.

## [2.0.0rc3] - 2023-05-03

### Geo

- The defaulting of `align_pixels` has changed slightly for the `AOI` class. Previously it always defaulted to
  `True`. Now the default is `True` if `resolution` is set, `False` otherwise. This ensures that when specifying
  a `shape` and a `bounds` rather than a resolution,the `shape` is actually honored.
- When assigning a `resolution` to an `AOI`, any existing `shape` attribute is automatically unset, since the
  two attributes are mutually exclusive.
- The validation of bounds for a geographic CRS has been slightly modified to account for some of the irregularities
  of whole-globe image products, correcting unintended failures in the past.

### Catalog

- The default `geocontext` for image objects no longer specifies a `resolution` but rather a `shape`, to ensure
  that default rastering preserves the original data and alignment (i.e. no warping of the source image).
- The `Blob.upload` and `Blob.upload_data` methods now return `self`, so they can be used in a fluent style.
- As with `resolution`, you can now pass a `crs` parameter to the rastering methods (e.g. `Image.ndarray`,
  `ImageCollection.stack`, etc.) to override the `crs` of the default geocontext.

### Compute

- A bevy of fixes to the client.

## [2.0.0rc2] - 2023-04-19

### Catalog

- Added support for data storage. The `Blob` class provides mechanism to upload, index, share, and retrieve arbitrary byte sequences (e.g. files). `Blob`s can be searched by namespace and name, geospatial coordinates (points, polygons, etc.), and tags. `Blob`s can be downloaded to a local file, or retrieved directly as a Python `bytes` object. `Blob`s support the same sharing mechanisms as `Product`s, with `owners`, `writers`, and `readers` attributes.
- Added support to `Property` for `prefix` filtering.

### Compute

- Added method to update user credentials for a `Function`.
- Added methods to retrieve build and job logs.

### General

- Added support for Shapely=2.X.

## [2.0.0rc1] - 2023-04-10

- This is an internal-only release. There is as of yet no updated documentation. However, the user-facing client APIs remain fully compatible with v1.12.1.

### Compute

- Added support for managed batch compute under the `compute` module.

### Auth and Configuration

- Removed the check on the Auth for configuration, since it is all AWS all the time.

### Raster Client

- Fixed a bug in the handling of small blocks (less than 512 x 512) that caused rasterio to generate bad download files (the desired image block would appear as a smaller sub-block rather than filling the resulting raster).

## [2.0.0rc0] - 2023-03-16

- This is an internal-only release. There is as of yet no updated documentation. However, the user-facing client APIs remain fully compatible with v1.12.1.

### Supported platforms

- Deprecated support for Python 3.7 (will end of life in July).
- Added support for Python 3.10 and Python 3.11
- AWS-only client. For the time being, the AWS client can be used to communicate with the legacy GCP platform (e.g. `DESCARTESLABS_ENV=gcp-production`) but only supports those services that are supported on AWS (`catalog` and `scenes`). This support may break at any point in the future, so it is strictly transitional.

### Dependencies

- Removed many dependencies no longer required due to the removal of GCP-only features.

### Configuration

- Major overhaul of the internals of the config process. To prepare for supporting other clients using namespaced packages within the `descarteslabs` package, the top level has been cleaned up, and most all the real code is down inside `descarteslabs.core`. However end users should never have to import anything from `descarteslabs.core`. No more magic packages means that `pylint` will work well with code using `descarteslabs`.
- GCP environments only support `catalog` and `scenes`. All other GCP-only features have been removed.

### Catalog

- A bug in the code handling the default context for image collections when working with a product with a CRS based on degrees rather than meters has been fixed. Resolutions should always be specified in the units used by the CRS.

## [1.12.1] - 2023-02-06

### Workflows

- Fixed a bug causing `descarteslabs.workflows.map.geocontext()` to fail with an import error. This problem
  also affected the autoscaling feature of workflows map layers.

### Catalog/Scenes/Raster

- Fixed a bug causing downloads of single-band images to fail when utilizing rasterio.

## [1.12.0] - 2023-02-01

### Catalog

- Catalog V2 is now fully supported on the AWS platform, including user ingest.
- Catalog V2 has been enhanced to provide substantially all the functionality of the Scenes API. The `Image` class now
  includes methods such as `ndarray` and `download`. A new `ImageCollection` class has been added, mirroring `SceneCollection`.
  The various `Search` objects now support a new `collect` method which will return appropriate `Collection` types
  (e.g. `ProductCollection`, `BandCollection`, and of course `ImageCollection`). Please see the updated Catalog V2
  guide and API documentation for more details.
- Previously, the internal implementation of the `physical_range` attribute on various band types was inconsistent with
  that of `data_range` and `display_range`. It has now been made consistent, which means it will either not be set,
  or will contain a 2-tuple of float values. It is no longer possible to explicitly set it to `None`.
- Access permissions for bands and images are now managed directly by the product. The `readers`, `writers`, and
  `owners` attributes have been removed from all the `*Band` classes as well as the `Image` class. Also the
  `Product.update_related_objects_permissions` and `Product.get_update_permissions_status` methods have been removed
  as these are no longer necessary or supported.
- All searches for bands (other than derived bands) and images must specify one or more product ids in the filtering.
  This requirement can be met by using the `bands()` and `images()` methods of a product to limit the search to that
  product, or through a `filter(properties.product_id==...)` clause on the search.
- Products have a new `product_tier` attribute, which can only be set or modified by privileged users.
- The `Image.upload_ndarray` will now accept either an ndarray or a list of ndarrays, allowing multiple files per image.
  The band definitions for the product must correspond to the order and properties of the multiple ndarrays.

### Scenes

- With the addition of the Scenes functionality to Catalog V2, you are strongly encouraged to migrate your Scenes-based
  code to use Catalog V2 instead. Scenes will be deprecated in a future release. Some examples of migrating from Scenes
  to Catalog V2 are included in the Catalog V2 guide. In the meantime the Scenes API has been completely reimplemented
  to use Catalog V2 under the hood. From a user perspective, existing code using the Scenes API should continue to
  function as normal, with the exception of a few differences around some little-used dark corners of the API.
- The Scenes `search_bands` now enforces the use of a non-empty `products=` parameter value. This was previously
  documented but not enforced.

### Metadata

- With the addition of the Scenes functionality to Catalog V2, you are strongly encouraged to migrate your Metadata-based
  code to use Catalog V2 instead. Metadata will be deprecated in a future release.
- As with Catalog and Scenes, one or more products must now be specified when searching for bands or images.

### Raster

- The Raster client API now requires a `bands=` parameter for all rastering operations, such as `raster`, `ndarray`
  and `stack`. It no longer defaults to all bands defined on the product.

### DLTile

- An off-by-1/2-pixel problem was identified in the coordinate transforms underlying
  `DLTile.rowcol_to_latlon` and `DLTile.latlon_to_rowcol`. The problem has been corrected,
  and you can expect to see slight differences in the results of these two methods.

### REST Clients

- All the REST client types, such as `Metadata` and `Raster`, now support `get_default_client()` and `set_default_client()`
  instances. This functionality was previously limited to the Catalog V2 `CatalogClient`. Whenever such a client is required,
  the client libraries use `get_default_client()` rather than using the default constructor. This makes it easy to
  comprehensively redirect the library to use a specially configured client when necessary.

### Geo package

- The `GeoContext` types that originally were part of the Scenes package are now available in the new `descarteslabs.geo` package,
  with no dependencies on Scenes. This is the preferred location from which to import these classes.

### Utils package

- The `descarteslabs.utils` package, added in the previous release for the AWS client only, now exists in the GCP client
  as well, and is the preferred location to pick up the `DotDict` and `DotList` classes, the `display` and `save_image` functions,
  and the `Properties` class for property filtering in Catalog V2.
- The `display` method now has added support for multi-image plots, see the API documentation for the `figsize`, `nrows`,
  `ncols` and `layout_direction` parameters.

### Property filtering

- The `property_filtering.GenericProperties` class has been replaced with `property_filtering.Properties`, but remains
  for back compatibility.
- Property filters now support `isnull` and `isnotnull` operations. This can be very useful for properties which may or
  may not be present, e.g. `properties.cloud_fraction.isnull | properties.cloud_fraction <= 0.2`.

### Configuration and Authentication

- The `Config` exceptions `RuntimeError` and `KeyError` were changed to `ConfigError` exceptions
  from `descarteslabs.exceptions`.
- `Auth` now retrieves its URL from the `Config` settings. If no valid configuration can be found,
  it reverts to the commercial service (`https://iam.descarteslabs.com`).

### General
- Dependencies for the descarteslabs library have been updated, but remain constrained to continue to support Python 3.7.
- Numerous bug fixes.

## [1.11.0] - 2022-07-20

### Installation

- The extra requirement options have changed. There are four extra requirement options now, `visualization`, `tables`,
  `complete`, and `tests`. `visualization` pulls in extra requirements to support operating in a Jupyter notebook or
  environment, enabling interactive maps and graphical displays. It is not required for operating in a "headless"
  manner. `tables` pulls in extra requirements to support the `Tables` client. `complete` is the combination of
  `visualization` and `tables`. `tests` pulls in extra requirements for running the tests. As always,
  `pip install 'descarteslabs[complete]'` will install a fully enabled client.

### Configuration

- The Descartes Labs client now supports configuration to support operating in different environments. By default,
  the client will configure itself for standard usage against the GCP platform (`"gcp-production"`), except in the case of AWS Marketplace users, for whom
  the client will configure itself against the AWS platform (`"aws-production"`).
  Alternate environments can be configured by setting the `DESCARTESLABS_ENV` environment variable before starting python, or by using a prelude like
  ```
  from descarteslabs.config import Settings
  Settings.select_env("environment-name")
  ```
  before any other imports of any part of the descarteslabs client package.
- The new AWS Enterprise Accelerator release currently includes only Auth, Configuration
  and the Scenes client.

### Auth and Exceptions

- The `descarteslabs.client.auth` package has moved to `descarteslabs.auth`. It is now imported
  into the original location at `descarteslabs.client.auth` to continue to work with existing
  code, but new code should use the new location.
- The `descarteslabs.client.exceptions` module has moved to `descarteslabs.exceptions`. It is
  now imported into the original location at `descarteslabs.client.exceptions` to continue to
  work with existing code, but new code should use the new location.

### Scenes

- Fixed an issue in `scenes.DLTile.from_shape` where there would be incomplete coverage of certain geometries. The function may now return more tiles than before.
- Added support for the new `all_touched` parameter to the different `GeoContext` types. Default behavior remains the same
as always, but if you set `all_touched=True` this communicates to the raster service that you want the image(s) rastered
using GDAL's `CUTLINE_ALL_TOUCHED` option which will change how source pixels are mapped to output pixels. This mode is
only recommended when using an AOI which is smaller than the source imagery pixel resolution.
- The DLTile support has been fixed to avoid generating gaps when tiling regions that span
  a large distance north-to-south and straddle meridians which are boundaries between
  UTM zones. So methods such as `DLTile.from_shape` may return more tiles than previously,
  but properly covering the region.
- Added support for retrieving products and bands.
  - Methods added: `get_product`, `get_band`, `get_derived_band`, `search_products`,
    `search_bands`, `search_derived_bands`.
  - Disallows search without `products` parameter.
- Scaling support has been enhanced to understand processing levels for newer products. The
  `Scene.scaling_parameters` and `SceneCollection.scaling_parameters` methods now accept
  a `processing_level` argument, and this will be factored in to the determination of
  the default result data type and scaling for all rastering operations such as `Scene.ndarray`
  and `SceneCollection.mosaic`.
- If the user provides the `rasterio` package (which implies providing GDAL), then rasterio
  will be used to save any downloaded images as GeoTIFF, allowing for the use of compression.
  Otherwise, by default the `tifffile` support will be used to generate the GeoTIFF files
  but compression is not supported in this mode.
- As the Places client has been deprecated, so has any use of the `place=` parameter supported
  by several of the Scenes functions and methods.

### Catalog

- (Core users only) Added support for specifying the image index to use when creating a new `Product`.
- Added support for defining per-processing-level `data_type`, `data_range`, `display_range`
  and `physical_range` properties on processing level steps.

### Discover

- Added support for filtering `Assets` by type and name fields.
  - Supported filter types `blob`, `folder`, `namespace`, `sym_link`, `sts_model`, and `vector`. Specifying multiple types will find assets matching any given type.
  - The name field supports the following wildcards:
    - `*` matches 0 or more of any character.
    - `?` matches 1 of any character.
  - Find assets matching type of `blob` and having a display name of `file name.json` or `file2name.txt` but **not** `filename.json`:
    - `Discover().list_assets("asset/namespace/org:some_org", filters="type=blob&name=file?name.*")`
    - `Discover().list_assets("asset/namespace/org:some_org", filters=AssetListFilter(type=AssetType.BLOB, name="file?name.*"))`
  - Find assets of type `blob` or `vector`:
    - `Discover().list_assets("asset/namespace/org:some_org", filters="type=blob,vector")`
    - `Discover().list_assets("asset/namespace/org:some_org", filters=AssetListFilter(type=[AssetType.BLOB, AssetType.VECTOR], name="file?name.*"))`

### Metadata

- `Metadata.products` and `Metadata.available_products` now properly implement paging so that
  by default, a DotList containing every matching product accessible to the user is returned.

### Raster

- If the user provides the `rasterio` package (which implies providing GDAL), then rasterio
  will be used to save any downloaded images as GeoTIFF, allowing for the use of compression.
  Otherwise, by default the `tifffile` support will be used to generate the GeoTIFF files
  but compression is not supported in this mode.

### Tables

- Fixed an issue that caused a user's schema to be overwritten if they didn't provide a primary
  key on table creation.
- Now uses Discover backend filtering for `list_tables()` instead of filtering on the client to
  improve performance.
- `list_tables()` now supports filtering tables by name
  - `Tables.list_tables(name="Test*.json")`

### Tasks

- New Tasks images for this release bump the versions of several dependencies, please see
  the Tasks guide for detailed lists of dependencies.

### Workbench

- The new Workbench release bumps the versions of several dependencies.

### Workflows

- Added support for the new `all_touched` parameter to the different `GeoContext` types.
  See description above under `Scenes`.

### General

- The Places client has been deprecated, and use thereof will generate a deprecation warning.
- The older Catalog V1 client has been deprecated, and use thereof will generate a deprecation
  warning. Please use the Catalog V2 client in its place.
- Documentation has been updated to include the `AWS Enterprise Accelerator" release.
- With Python 2 far in the rearview mirror, the depedencies on the `six` python package have
  been removed throughout the library, the distribution and all tasks images.

## [1.10.0] - 2022-01-18

### Python Versions Supported

- Added support for Python 3.9.
- Removed support for Python 3.6 which is now officially End Of Life.

### Workflows
- Added support for organizational sharing. You can now share using the `Organization` type:
  - `workflows.add_reader(Organization("some_org"))`

### Discover

- Added support for organizational sharing. You can now share using the `Organization` type:
  - `asset.share(with_=Organization("some_org"), as_="Viewer")`
- Allow user to list their organization's namespace.
  - `Discover().list_asset("asset/namespace/org:some_org")`
- Allow user to list their organization's users.
  - `Discover().list_org_users()`

### Tables - Added
- Added an **alpha** Tables client. The Tables module lets you organize, upload, and query tabular data and vector geometries. As an alpha release, we reserve the right to modify the Tables client API without any guarantees about backwards compatibility. See the [Tables API](https://docs.descarteslabs.com/descarteslabs/tables/readme.html) and [Tables Guide](https://docs.descarteslabs.com/guides/tables.html) documentation for more details.

### Scenes
- Added the `progress=` parameter to the various rastering methods such as `Scene.ndarray`,
  `Scene.download`, `SceneCollection.mosaic`, `SceneCollection.stack`, `SceneCollection.download`
  and `SceneCollection.download_mosaic`. This can be used to enable or disable the display
  of progress bars.

### Tasks images
- Support for Python 3.9 images has been added, and support for Python 3.6 images has been removed.
- Many of the add on packages have been upgraded to more recently released versions. In particular, `tensorflow` was updated from version 2.3 to version 2.7.
- GPU support was bumped up from CUDA 10 to CUDA 11.2

## [1.9.1] - 2021-12-20

### Raster

- Fixed a bug preventing retry-able errors (such as a 429) from being retried.

## [1.9.0] - 2021-11-09

### Catalog

- Allow retrieving Attribute as a class attribute. It used to raise an exception.

### Scenes

- Fixed a bug preventing the user from writing JPEG files with smaller than 256x256 tiles.
- Allow specifying a `NoData` value for non-JPEG GeoTIFF files.
- Include band description metadata in created GeoTIFF files.
- Support scaling parameters as lists as well as tuples.
- Add caching of band metadata to drastically reduce the number of metadata queries when creating `SceneCollections`.
- `DLTiles.from_shape` was failing to handle shape objects implementing the `__geo_interface__` API, most notably several of the Workflows `GeoContext` types. These now work as expected.
- Certain kinds of network issues could read to rastering operations raising an `IncompleteRead` exception. This is now correctly caught and retried within the client library.

### Tasks

- Users can now use `descarteslabs.tasks.update_credentials()` to update their task credentials in case they became outdated.

### Workflows

- We have introduced a hard limit of 120 as the number of outstanding Workflows compute jobs that a single user can have. This limit exists to minimize situations in which a user is unable to complete jobs in a timely manner by ensuring resources cannot be monopolized by any individual user. The API that backs the calls to `compute` will return a `descarteslabs.client.grpc.exceptions.ResourceExhausted` error if the caller has too many outstanding jobs. Prior to this release (1.9.0), these failures would be retried up to some small retry limit. With the latest client release however, the client will fail without retrying on an HTTP 429 (rate limit exceeded) error. For users with large (non-interactive) workloads who don’t mind waiting, we added a new `num_retries` parameter to the `compute` function; when specified, the client will handle any 429 errors and retry up to `num_retries` times.
- Workflows is currently optimized for interactive use cases. If you are submitting large numbers of long-running Workflows compute jobs with `block=False`, you should consider using Tasks and Scenes rather than the Workflows API.
- Removed `ResourceExhausted` exceptions from the list of exceptions we automatically catch and retry on for `compute` calls.

### Documentation

- Lots of improvements, additions, and clarifications in the API documentation.

## [1.8.2] - 2021-07-12

### General

- Workflows client no longer validates `processing_level` parameter values, as these have been enhanced to support new products and can only be validated server side.
- Catalog V2 bands now support the `vendor_band_name` field (known as `name_vendor` in Metadata/Catalog V1).
- Scenes support for masking in version 1.8.1 had some regressions which have been fixed. For this reason, version 1.8.1 has been pulled from PyPI.
- New task groups now default to a `maximum_concurrency` value of 5, rather than the previous 500. This avoids the common problem of deploying a task group with newly developed code, and having it scale up and turning small problems into big problems! You may still set values as large as 500.
- The Tasks client now provides an `update_group()` method which can be used to update many properties of an existing task group, including but not limited to `name`, `image`, `minimum_concurrency`, and `maximum_concurrency`.
- Improved testing across several sub-packages.
- Various documentation fixes.

## [1.8.1] - 2021-06-22

** Version Deprecated ** Due to some regressions in the Scenes API, this version has been removed from PyPI.

### General

- Added a new `common.dltile` library that performs geospatial transforms and tiling operations.
- Upgraded various dependencies: `requests[security]>=2.25.1,<3`,`six>=1.15.0`,`blosc==1.10.2`,` mercantile>=1.1.3`,`Pillow>=8.1.1`,`protobuf>=3.14.0,<4`,`shapely>=1.7.1,<2`,`tqdm>=4.32.1`,`traitlets>=4.3.3,<6;python_version<'3.7'`,`traitlets==5.0.5,<6;python_version>='3.7'`,`markdown2>=2.4.0,<3`,`responses==0.12.1`,`freezegun==0.3.12`,`imagecodecs>=2020.5.30;python_version<'3.7'`,`imagecodecs>=2021.5.20;python_version>='3.7'`,`tifffile==2020.9.3;python_version<'3.7'`,`tifffile==2021.4.8;python_version>='3.7'`

### Discover (alpha) - Added

- Added an **alpha** Discover client. Discover allows users to organize and share assets with other users. As an alpha release, we reserve the right to modify the Discover client API without any guarantees about backwards compatibility. See the [Discover API documentation](https://docs.descarteslabs.com/descarteslabs/discover/readme.html) for more details.

### Metadata/Catalog V1 - Changed

- **breaking** Image (Scene) metadata now accepts and returns the `bucket` and `directory` fields as lists of strings, of a length equal to that
  of the `files` fields. This allows the file assets making up an image to live in different locations. When creating new images,
  a simple string can still be provided for these fields. It will automatically be converted to a list of (duplicated) strings as
  necessary. As most users will never interact with these fields, the change should not affect user code.

### Metadata/Catalog V1/Catalog V2 - Changed

- `derived_params` field for Image (scene) metadata now supported for product-specific service-implemented "native derived bands" which may
  only be created for core products.

### Scenes - Changed

- Scenes now uses the client-side `dltile` library to make DLTiles. This improves performance when creating a large number of DLTile objects.
- Scenes DLTile `from_shape` now has a parameter to return tile keys only instead of full tile objects. Usage details can be found [in the docs](https://docs.descarteslabs.com/descarteslabs/scenes/docs/geocontext.html#descarteslabs.scenes.geocontext.DLTile.from_shape).
- Scenes DLTile now has new methods: `iter_from_shape` that takes the same arguments as `from_shape` but returns an iterator ([from_shape docs](https://docs.descarteslabs.com/descarteslabs/scenes/docs/geocontext.html#descarteslabs.scenes.geocontext.DLTile.iter_from_shape)), `subtile` that adds the ability to subdivide tiles ([subtile docs](https://docs.descarteslabs.com/descarteslabs/scenes/docs/geocontext.html#descarteslabs.scenes.geocontext.DLTile.subtile)), and `rowcol_to_latlon` and `latlon_to_rowcol` which converts pixel coordinates to spatial coordinates and vice versa ([rowcol_to_latlon docs](https://docs.descarteslabs.com/descarteslabs/scenes/docs/geocontext.html#descarteslabs.scenes.geocontext.DLTile.rowcol_to_latlon) and [latlon_to_rowcol docs](https://docs.descarteslabs.com/descarteslabs/scenes/docs/geocontext.html#descarteslabs.scenes.geocontext.DLTile.latlon_to_rowcol)).
- Scenes DLTile now has a new parameter `tile_extent` which is the total size of the tile in pixels including padding. Usage details can be found [in the docs](https://docs.descarteslabs.com/descarteslabs/scenes/docs/geocontext.html#descarteslabs.scenes.geocontext.DLTile.tile_extent).
- **breaking** Removed the dependence on `Raster` for tiling. The `raster_client` parameter has been removed from the `from_latlon`, `from_key`, `from_shape`, and `assign`DLTile methods.
- Tiling using `from_shape` may return a different number of tiles compared to previous versions under certain conditions. These tiles are usually found in overlapping areas between UTM zones and should not affect the overall coverage.
- DLTile geospatial transformations are guaranteed to be within eight decimal points of the past implementation.
- DLTile errors now come from the `dltile` library and error messages should now be more informative.
- When specifying output bounds in a spatial reference system different from the underlying raster, a densified representation of the bounding box is used internally to ensure that the returned image fully covers the bounds. For certain methods (like `mosaic`) this may change the returned image dimensions, depending on the SRSs involved.
- **breaking** As with the Metadata v1 client changes, the `bucket` and `directory` fields of the Scene properties are now multi-valued lists.
- Scenes does not support writing GeoTiffs to file-like objects. Non-JPEG GeoTiffs are always uncompressed.

### Raster - Changed
- `dltiles_from_shape`, `dltiles_from_latlon`, and `dltile` have been removed. **It is
  strongly recommended to test any existing code which uses the Raster API when upgrading to this
  release.**
- Fully masked arrays are now supported and are the default. Usage details can be found [in the docs](https://docs.descarteslabs.com/descarteslabs/client/services/raster/readme.html#descarteslabs.client.services.raster.Raster.ndarray)
- Added support to draw progress bar. Usage details can be found [in the docs](https://docs.stage.descarteslabs.com/descarteslabs/client/services/raster/readme.html).
- The signature and return value of `Raster.raster()` have changed. The `save=` parameter has been removed as the resulting download is always saved
  to disk, to a file named by the `outfile_basename=` parameter. The method returns a tuple containing the name of the resulting file and the metadata
  for the retrieval, which is now an ordinary Python dictionary.
- As with Scenes, when specifying output bounds in a spatial reference system different from the underlying raster, a densified representation of the bounding box is used internally to ensure that the returned image fully covers the bounds. For certain methods (like `mosaic`) this may change the returned image dimensions, depending on the SRSs involved.

## [1.8.0] - 2021-06-08

Internal release only. See 1.8.1 above.

## [1.7.1] - 2021-03-03

### General
- Upgraded various dependencies: `blosc==1.10.2`, `cachetools>=3.1.1`, `grpcio>=1.35.0,<2`, `ipyleaflet>=0.13.3,<1`, `protobuf>=3.14.0,<4`, `pyarrow>=3.0.0`, `pytz>=2021.1`
- Upgraded from using Travis to GitHub Actions for CI.

### Catalog
- Added support for the `physical_range` property on `SpectralBand` and `MicrowaveBand`.

### Workflows (channel `v0-19`) - Added
- Workflows sharing. Support has been added to manage sharing of `Workflow` objects with other authorized users. The `public` option for publishing workflows
has been removed now that `Workflow.add_public_reader()` provides the equivalent capability. See the [Workflows Guide](https://docs.descarteslabs.com/guides/workflows/sharing.html#sharing-workflows).
- Lots of improvements to [API documentation](https://docs.descarteslabs.com/descarteslabs/workflows/readme.html) and the [Workflows Guide](https://docs.descarteslabs.com/guides/workflows.html).

### Workflows - Fixed
- Allow constructing `Float` instances from literal python integers.

## [1.7.0] - 2021-03-02

### This release was withdrawn due to a compatibility problem

## [1.6.1] - 2021-01-27

Fixes a few buglets which slipped through. This release continues to use the workflows channel `v0-18`.

### Workflows - Fixed
- Fixed a problem with the defaulting of the visual options when generating tile URLs, making it possible
  to toggle the checkerboard option on a layer and see the difference.
- Support `axis=list(...)` for `Image`.
- Corrected the results of doing arithmetic on two widgets (e.g. adding two `IntSlider`s together should yield` an `Int`).
- For single-band imagery `VizOption` will accept a single two-tuple for the `scales=` argument.

## [1.6.0] - 2021-01-20

### Python Version Support
- Python 3.6 is now deprecated, and support will be removed in the next version.

### Catalog
- Added support to Bands for new processing levels and processing step specifications
  to support Landsat Collection 2.

### Workflows (channel `v0-18`) - Added
- The new channel `v0-18` utilizes a new and improved backend infrastructure. Any previously saved workflows and jobs from earlier channels are not accessible from the new infrastructure, so you will need to recreate and persist (e.g. publish) new versions using `v0-18`. Older releases and older channels can continue to access your originals if needed.
- **`wf.widgets` lets you quickly explore data interactively.** Add widgets anywhere in your code just like normal values, and the widgets will display automatically when you call `.visualize`.
- **View shared Workflows and XYZs in GIS applications using WMTS.** Get the URL with `wf.wmts_url()`, `XYZ.wmts_url()`, `Workflow.wmts_url()`.
  - Create publicly-accessible tiles and WMTS endpoints with `wf.XYZ(..., public=True)`. Anyone with the URL (which is a cryptographically random ID) can view the data, no login required. Set `days_to_expiration` to control how long the URL lasts.
  - `wf.XYZ.list()` to iterate through all XYZ objects you've created, and `XYZ.delete` to delete them.
  - Set default vizualization options (scales, colormap, bands, etc.) in `.publish` or `wf.XYZ` with `wf.VizOption`. These `viz_options` are used when displaying the published object in a GIS application, or with `wf.flows`.
- `ImageCollection.visualize()`: **display ImageCollections on `wf.map`**, and select the reduction operation (mean, median, mosaic, etc.) interactively
- `Image.reduction()` and `ImageCollection.reduction()` (like `ic.reduction("median", axis="images")`) to reduce an Image/ImageCollection with an operation provided by name
- `wf.map.controls` is accessible (you had to do `wf.map.map.controls` before)
- Access the parameters used in a Job with `Job.arguments` and `Job.geoctx`.

### Workflows - Fixed
- Errors like `In 'or': : operand type(s) all returned NotImplemented from __array_ufunc__` when using the bitwise-or operator `|` are resolved.
- Errors when using computed values in the `wf.Datetime` constructor (like `wf.Datetime(wf.Int(2019) + 1)`) are resolved.
- `wf.Timedelta` can be constructed from floats, and supports all binary operations that Python does (support for `/, //, %, *` added)
- In `.rename_bands`, prohibit renaming a band to a name that already exists in the Image/ImageCollection. Previously, this would succeed, but cause downstream errors.
- `.bandinfo.get("bandname", {})` now works---before, providing `{}` would fail with a TypeError
- Indexing an `Any` object (like `wf.Any({"foo": 1})["foo"]`) behaves correctly
- `wf.Datetime`s constructed from strings containing timezone information are handled correctly

### Workflows - Changed
- `.mask(new_mask)` ignores masked pixels in `new_mask`. Previously, masked pixels in `new_mask` were considered True, not False. Note that this is opposite of NumPy's behavior.
- If you `.publish` an object that depends on `wf.parameter`s or `wf.widgets`, it's automatically converted into a `wf.Function`.
- **breaking** `.compute` and `.inspect` no longer accept extra arguments that aren't required for the computation. If the object doesn't depend on any `wf.parameter`s or `wf.widgets`, passing extra keyword arguments will raise an error. Similarly, *not* providing keyword arguments for all parameters the object depends on will raise an error.
- **breaking** The `wf.XYZ` interface has changed; construct an XYZ with `wf.XYZ(...)` instead of `wf.XYZ.build(...).save()`
- Set `days_to_expiration` on `XYZ` objects. After this many days, the object is deleted.
- `Job` metadata is deleted after 10 days; `wf.Job.get(...)` on a job ID more than 10 days old will fail. Note that Job results have always been deleted after 10 days; now the metadata expires as well.
- `wf.Function` has better support for named arguments. Now, `f = wf.Function[{'x': wf.Int, 'y': wf.Str}, wf.Int]` requires two arguments `x` and `y`, and they can be given positionally (`f(1, "hi")`), by name in any order(`f(x=1, y="hi")` or `f(y="hi", x=1)`), or both (`f(1, y="hi")`). `wf.Function.from_callable` will generate a Function with the same names as the Python function you decorate or pass in. Therefore, when using `@wf.publish` as a decorator, the published Function will automatically have the same argument names as your Python function.

## [1.5.0] - 2020-09-22

### Python Version Support
- Python 3.8 is now supported in the client.
- As Python 3.5 has reached End Of Life, it is no longer supported by the descarteslabs client.

### Tasks Client
- Altered the behavior of Task function creation. Deprecation warnings will be issued when attempting to create
  a Task function for which support will be removed in the near future. **It is
  strongly recommended to test any existing code which uses the Tasks client when upgrading to this
  release.**
- New tasks public images for for use with Python 3.8 are available.

### Workflows (channel `v0-17`) - Fixed
- `.pick_bands` supports proxy `wf.Str` objects; `.unpack_bands` supports `wf.Str` and `wf.Tuple[wf.Str, ...]`.
- Better performance constructing a `wf.Array` from a `List` of numbers (like `wf.Array(ic.sum(["pixels", "bands"]))`)
- No more error using `@wf.publish` as a decorator on a function without a docstring

## [1.4.1] - 2020-09-02

### Fixed
No more irrelevant `DeprecationWarning`s when importing the `descarteslabs` package ([#235](https://github.com/descarteslabs/descarteslabs-python/issues/235)). Deprecated functionality in the package will now show `FutureWarning`s instead.

### Workflows (channel `v0-16`) - Fixed
- `wf.map.geocontext` doesn't raise an error about the CRS of the map
- `wf.flows` doesn't raise an error about versions from incompatible channels
## [1.4.0] - 2020-08-20

### Catalog client

- Example code has been cleaned up.

### Workflows (channel `v0-16`) - Added
- **Sharing of any Workflows object as a `Workflow`** with version and access control. Browse through shared `Workflow`s with the `wf.flows` browser widget.
- **Upload images to the DL catalog from Workflows jobs**. Usage details can be found [in the docs](https://docs.descarteslabs.com/descarteslabs/workflows/docs/destinations.html).
- `wf.np.median`
- `Job.cancel()` to cancel running jobs.
- Transient failures in Jobs are automatically retried, resulting in fewer errors.
- [Search widget](https://ipyleaflet.readthedocs.io/en/latest/api_reference/search_control.html) on `wf.map` by default.

### Workflows - Fixed
- Bitwise operations on imagery no longer fail
- `wf.np.linspace` no longer fails when being called correctly
- `.median` is slightly less prone to OOM errors

### Workflows - Changed
- **Breaking: Workflows sharing**: `wf.publish()` and `wf.use()` have new signatures, `wf.retrieve()` has been removed in favor of `wf.Workflow.get()` and `wf.VersionedGraft.get_version()` and the `wf.Workflow` object has been completely refactored. Detailed information is [in the docs](https://docs.descarteslabs.com/descarteslabs/workflows/docs/execution.html#module-descarteslabs.workflows.models).
- `Array.to_imagery` now accepts `KnownDict` for bandinfo and properties.
- `Number`s can now be constructed from `Str`s

## [1.3.0] - 2020-06-12

### Workflows (channel `v0-15`) - Added
- **Output formats for `.compute` including GeoTIFF, JSON**, PyArrow, and MessagePack. Usage details can be found [in the docs](https://docs.descarteslabs.com/descarteslabs/workflows/docs/formats.html).
- **Destinations for Job results: download and email**. Usage details can be found [in the docs](https://docs.descarteslabs.com/descarteslabs/workflows/docs/destinations.html).
- **Save `.compute` outputs to a file** with the `file=` argument.
- **Pixel value inspector**: click in the map widget to view pixel values.
- **`wf.ifelse`** for simple conditional logic.
- NumPy functions including `hypot`, `bitwise_and`, `bitwise_or`, `bitwise_xor`, `bitwise_not`, `invert`, and `ldexp`
- Bitwise `Array` and `MaskedArray` operations
- `size` attribute on `Array` and `MaskedArray`
- `astype` function on `Array` and `MaskedArray` for changing the dtype
- `flatten` function on `Array` and `MaskedArray` for flattening into a 1D array
- `MaskedArray.compressed` for getting all unmasked data as a 1D array
- `get` function on `Dict` and `KnownDict` for providing a default value if a key does not exist
- `nbands` attribute on `Image` and `ImageCollection`
- `proxify` can handle `scenes.GeoContext`s
- `Dict.contains`, `Dict.length`

### Workflows - Fixed
- **Fewer failures and hanging calls when connecting to the Workflows backend** (like `.compute`, `.visualize`, `Job.get`, etc.)
- **`wf.numpy.histogram` works correctly with computed values for `range` and `bins` (such as `range=[arr.min(), arr.max()]`)**
- More consistent throughput when a large number of jobs are submitted
- `Array`s can now be constructed from proxy `List`s
- `MaskedArray.filled` works correctly when passed Python values
- Long-running sessions (like Jupyter kernels) refresh credentials instead of failing with auth errors after many hours of use
- `wf.numpy.dot` and `wf.numpy.einsum` no longer fail when being called correctly
- Occasional errors like `('array-89199362e9a5d598fb5c82805136834d', 0, 0)` when calling `wf.compute()` with multiple values are resolved

### Workflows - Changed
- **`pick_bands` accepts duplicate band names.** Enjoy easier Sentinel-1 `"vv vh vv"` visualizations!
- **`ImageCollection.from_id` is always ordered by date**
- `wf.numpy.percentile` no longer accepts an `axis` argument
- **breaking** `wf.Job` construction and interface changes:
  - Use a single `wf.Job(..)` call instead of `wf.Job.build(...).execute()` to create and launch a Job
  - New `Job.result_to_file` method
  - `Job.status` is removed in favor of a single `Job.stage`
  - `wf.TimeoutError` renamed to `wf.JobTimeoutError`

## [1.2.0] - 2020-04-23

### Workflows (channel `v0-14`) - Added
- **191 functions from NumPy are available for Workflows `Array`s**, including parts of the `numpy.linalg` and `numpy.ma` submodules. See the full list [on the docs](https://docs.descarteslabs.com/descarteslabs/workflows/docs/types/numpy.html).
- `index_to_coords` and `coords_to_index` methods on `Image`/`ImageCollection`/`GeoContext` for converting between geospatial and array coordinates
- `value_at` function on `Image` and `ImageCollection` for extracting single pixel values at spatial coordinates.

### Workflows - Fixed
- Using datetimes as parameters to `visualize` behaves correctly.

## [1.1.3] - 2020-04-02

### Catalog client

- Fixed a bug that prevented uploading ndarrays of type `uint8`

### Workflows (channel `v0-13`) - Added
- Array support for `argmin`, `argmax`, `any`, `all`
- `pick_bands` supports an `allow_missing` kwarg to drop band names that may be missing from the data without an error.
- `wf.compute` supports passing lists or tuples of items to compute at the same time. Passing multiple items to `wf.compute`, rather than calling `obj.compute` for each separately, is usually faster.
- Casting from `Bool` to `Int`: `wf.Int(True)`
- Experimental `.inspect()` method for small computations during interactive use.

### Workflows - Changed
- **[breaking]** Array no longer uses type parameters: now you construct an Array with `wf.Array([1, 2, 3])`, not `wf.Array[wf.Int, 1]([1, 2, 3])`. Remember, Array is an experimental API and will continue to make frequent breaking changes!
- Workflows now reuses the same gRPC client by default---so repeated or parallel calls to `.compute`, etc. will be faster. Calling `.compute` within a thread pool will also be significantly more efficient.

### Workflows - Fixed
- `wf.numpy.histogram` correctly accepts a `List[Float]` as the `range` argument

## [1.1.2] - 2020-03-12

1.1.2 fixes a bug which caused Workflows map layers to behave erratically when changing colormaps.

## [1.1.1] - 2020-03-11

1.1.1 fixes a packaging issue that caused `import descarteslabs.workflows` to fail.

It also makes NumPy an explicit dependency. NumPy was already a transitive dependency, so this shouldn't cause any changes.

You should _NOT_ install version 1.1.0; 1.1.1 should be used instead in all circumstances.

## [1.1.0] - 2020-03-11

### Catalog client

- `Image.upload()` now emits a deprecation warning if the image has a `cs_code` or `projection` property.
  The projection defined in the uploaded file is always used and applied to the resulting image in the Catalog.
- `Image.upload_ndarray()` now emits a deprecation warning if the image has both a `cs_code` and a `projection`
  property. Only one of them may be supplied, and `cs_code` is given preference.

### Scenes
- `SceneCollection.download_mosaic` has new default behavior for `mask_alpha` wherein the `alpha` band will be
  used as a mask by default if it is available for all scenes in the collection, even if it is not specified in
  the list of bands.

### Workflows (channel `v0-12`) - Added
- **Experimental Array API** following the same syntax as NumPy arrays. It supports vectorized operations, broadcasting,
  and multidimensional indexing.
  - `ndarray` attribute of `Image` and `ImageCollection` will return a `MaskedArray`.
  - Over 60 NumPy ufuncs are now callable with Workflows `Array`.
  - Includes other useful `Array` functions like `min()`, `median()`, `transpose()`, `concatenate()`, `stack()`, `histogram()`, and `reshape()`.
- **`ImageCollection.sortby_composite()`** for creating an argmin/argmax composite of an `ImageCollection`.
- **Slicing** of `List`, `Tuple`, `Str`, and `ImageCollection`.
- `wf.range` for generating a sequence of numbers between start and stop values.
- `ImageCollectionGroupby.mosaic()` for applying `ImageCollection.mosaic` to each group.
- `wf.exp()`, `wf.square()`, `wf.log1p()`, `wf.arcsin()`, `wf.arccos()`, and `wf.arctan()`
- `Datetime.is_between()` for checking if a `Datetime` falls within a specified date range
- `FeatureCollection.contains()`
- Container operations on `GeometryCollection` including:
  - `GeometryCollection.contains()`
  - `GeometryCollection.sorted()`
  - `GeometryCollection.map()`
  - `GeometryCollection.filter()`
  - `GeometryCollection.reduce()`
- `List` and `Tuple` can now be compared with other instances of their type via `__lt__()`, `__eq__()` etc.
- `List.__add__()` and `List.__mul__()` for concatenating and duplicating `List`s.

### Workflows - Changed
- Products without alpha band and `nodata` value are rejected, instead of silently producing unwanted behavior.
- `ImageCollection.concat_bands` now throws a better error when trying to concatenate bands from another `ImageCollection` that is not the same length.
- `Any` is now promotable to all other types automatically.
- Better error when trying to iterate over Proxytypes.
- Interactive map: calls to `visualize` now clear layer errors.
- Interactive map: when setting scales, invalid values are highlighted in red.
- Interactive map: a scalebar is shown on the bottom-left by default.
- `ImageCollection.mosaic()` now in "last-on-top" order, which matches with GDAL and `dl.raster`. Use `mosaic(reverse=True)` for the same ordering as in v1.0.0.

### Workflows - Fixed
- Better errors when specifying invalid type parameters for Proxytypes that require them.
- Field access on `Feature`, `FeatureCollection`, `Geometry`, and `GeomeryCollection` no longer fails.
- In `from_id`, processing level 'cubespline' no longer fails.


## [1.0.0] - 2020-01-20

| As of January 1st, 2020, the client library no longer supports Python 2. For more information, please contact support@descarteslabs.com. For help with porting to Python 3, please visit https://docs.python.org/3/howto/pyporting.html. |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |

### Catalog client
- There is an entirely new backend supporting asynchronous uploads of image files and ndarrays with
  the catalog client. There are minor changes to the `ImageUpload` class (a new `events` field has subsumed
  `errors`, and the `job_id` field has been removed) but the basic interface is unchanged so most
  code will keep functioning without any changes.
- It is now possible to cancel image uploads.
- Errors messages are now easier to read.
- Many improvements to the documentation.
- You can now create or retrieve an existing object using the `get_or_create` method.
- Retrieving a `Band` or `Image` by name is now possible by calling `get_band` or `get_image` on the
  `Product` instance. You can also use the Product's `named_id` function to get a complete id for
  images and bands.
- A new convenience function `make_valid_name` on `Image` and `Band` classes will return a sanitized
  name without invalid characters.
- A new property `ATTRIBUTES` enumerates which attributes are available for a specific catalog object.
- Trying to set an attribute that does not exist will now raise `AttributeError`.
- `update_related_objects_permissions()` should no longer fail with a JSON serialization error.
- Setting a read-only attribute will now raise an `AttributeValidationError`.
- Saving a new object while one with the same id already exists will now raise a `ConflictError`
  instead of `BadRequestError`.
- If a retrieved object has since been deleted from the catalog, saving any changes or trying to
  reload it will now raise a `DeletedObjectError`.
- Resolution fields now accept string values such as "10m" or "0.008 degrees". If the value cannot
  be parsed, an `AttributeValidationError` will be raised.
- Changes to the `extra_properties` attribute are now tracked correctly.

### Packaging
- This release no longer supports Python 2.
- This package is now distributed as a Python 3 wheel which will speed up installation.

### Workflows (channel `v0-11`) - Added
- **Handling of missing data** via empty ImageCollections
  - `ImageCollection.from_id` returns an empty ImageCollection if no data exist for the given time/place, rather than an error
  - `ImageCollection.filter` returns an empty ImageCollection if the predicate is False for every Image, rather than an error
  - `Image.replace_empty_with` and `ImageCollection.replace_empty_with` for explicitly filling in missing data
  - See the [Workflows guide](https://docs.descarteslabs.com/guides/workflows.html) for more information
- **Docstrings and examples** on every class and function!
- **Assigning new metadata to Image properties** & bandinfo: `Image.with_properties()`, `Image.with_bandinfo()`
- Interactive map: **colorbar legends** on layers with colormaps (requires matplotlib)
- **`Dict.from_pairs`**: construct a Dict from a sequence of key-value pairs
- Map displays a **fullscreen button** by default (**[breaking]** if your code adds one, you'll now get two)
- **`wf.concat`** for concatentating `Image` and `ImageCollection` objects
  - `ImageCollection.concat` now accepts `Image` objects; new `Image.concat` accepts `Image` or `ImageCollection`
- **`ImageCollection.mosaic()`**
- `FeatureCollection.sorted()`, `FeatureCollection.length()`, `FeatureCollection.__reversed__()`
- `GeometryCollection.length()`, `GeometryCollection.__reversed__()`

### Workflows - Changed
- **`wf.zip` now supports `ImageCollection`**, `FeatureCollection`, `GeometryCollection` as well as `List` and `Str`
- **Get a GeoContext for the current bounds of the map in any resolution, shape, or CRS** (including `"utm"`, which automatically picks the right UTM zone for you) with `wf.map.geocontext`. Also now returns a Scenes GeoContext for better introspection and use with Raster.
- Better backend type-checking displays the possible arguments for most functions if called incorrectly
- `arr_shape` included when calling `wf.GeoContext.compute()`
- More readable errors when communication with the backend fails
- Interactive map: layout handles being resized, for example setting `wf.map.layout.height = '1000px'`
- `Any` is no longer callable; `Any.cast` encouraged
- `remove_layer` and `clear_layers` moved from `wf.interactive.MapApp` class to `wf.interactive.Map` (non-breaking change)
- **[possibly breaking]** band renaming in binary operators only occurs when broadcasting: `red + red` is just `red`, rather than `red_add_red`. `red + blue` is still `red_add_blue`. Code which depends on accessing bands by name may need to change.

### Workflows - Fixed
- `wf.where` propagates masks correctly, and handles metadata correctly with multi-band inputs
- `processing_level="surface"` actually returns surface-reflectance-processed imagery
- `ImageCollection.sorted()` works properly
- Viewing global-extent WGS84 images on the Workflows map no longer causes errors
- `List` proxytype no longer infinitely iterable in Python
- Repeated use of `axis="bands"` works correctly
- `ImageCollection.from_images` correctly aligns the bands of the inputs
- Numeric casting (`wf.Int(wf.Float(2.2))`) works as expected
- More descriptive error when constructing an invalid `wf.Datetime`
- Computing a single `Bool` value derived from imagery works correctly


## [0.28.1] - 2019-12-10

### Changed
- Update workflows client channel
- Workflows map UI is more stable: errors and layers won't fill the screen

## [0.28.0] - 2019-12-09
### Added
- Catalog client: Added an `update()` method that allows you to update multiple attributes at once.

### Changed
- Catalog client: Images and Bands no longer reload the Product after calling `save`
- Catalog client: Various attributes that are lists now correctly track changes when modifying them with list methods (e.g. `Product.owners.append("foo")`)
- Catalog client: Error messages generated by the server have a nicer format
- Catalog client: Fix a bug that caused waiting for tasks to never complete
- The minimum `numpy` version has been bumped to 1.17.14 for Python version > 3.5, which addresses a bug with `scenes.display`

### Workflows (channel `v0-10`) - Added
- `.compute()` is noticeably faster
- Most of the Python string API is now available on `workflows.Str`
- Interactive map: more descriptive error when not logged in to iam.descarteslabs.com
- Passing the wrong types into functions causes more descriptive and reliable errors

### Workflows - Fixed
- `RST_STREAM` errors when calling `.compute()` have been eliminated
- `Image/ImageCollection.count()` is much faster
- `.buffer()` on vector types now works correctly
- Calling `.compute()` on a `GeometryCollection` works

## [0.27.0] - 2019-11-18
### Added

- Catalog client: Added a `MaskBand.is_alpha` attribute to declare alpha channel behavior for a band.

### Changed

- The maximum number of `extra_properties` allowed for Catalog objects has been increased from 10 to 50.
- Fixed bug causing `SceneCollection.download` to fail.

### Workflows (channel `v0-9`) - Added
- When you call `.compute()` on an `Image` or `ImageCollection`, the `GeoContext` is included on the result object (`ImageResult.geocontext`, `ImageCollectionResult.geocontext`)

### Workflows - Fixed
- Passing a Workflows `Timedelta` object (instead of a `datetime.timedelta`) into functions expecting it now behaves correctly
- Arguments to the reducer function for `reduce` are now in the correct order

## [0.26.0] - 2019-10-30
### Added

- A new catalog client in `descarteslabs.catalog` makes searching and managing products, bands and images easier. This client encompasses functionality previously split between the `descarteslabs.Metadata` and `descarteslabs.Catalog` client, which are now deprecated. Learn how to use the new API in the [Catalog guide](https://docs.descarteslabs.com/guides/catalog_v2.html).
- Property filtering expressions such as used in `scenes.search()` and `FeatureCollection.filter()` now support an `in_()` method.

### Changed

- `SceneCollection.download` previously always returned successfully even if one or more of the downloads failed. Now if any of the downloads fail, a RuntimeError is raised, which will detail which destination files failed and why.
- Fixed a bug where geometries used with the Scenes client had coordinates with reduced precision.

### Workflows (channel `v0-8`) - Added
- **Interactive parameters**: add parameters to map layers and interactively control them using widgets
- **Spatial convolution** with `wf.conv2d`
- Result containers have helpful `repr`s when displayed
- `Datetime` and `Timedelta` are unpacked into `datetime.datetime` and `datetime.timedelta` objects when computed.

### Workflows - Changed
- **[breaking]** Result containers moved to `descarteslabs/workflows/results` and renamed, appending "Result" to disambiguate (e.g. ImageResult and ImageCollectionResult)
- **[breaking] `.bands` and `.images` attributes of ImageResult and ImageCollectionResult renamed `.ndarray`**
- **[breaking]** When `compute`-ing an `Image` or `ImageCollection`, **the order of `bandinfo` is only correct for Python >= 3.6**
- Interactive maps: coordinates are displayed in lat, lon order instead of lon, lat for easier copy-pasting
- Interactive maps: each layer now has an associated output that is populated when running autoscale and deleted when the layer is removed
- Interactive maps: `Image.visualize` returns a `Layer` object, making it easier to adjust `Layer.parameters` or integrate with other widgets

### Workflows - Fixed
- Composing operations onto imported Workflows no longer causes nondeterministic errors when computed
- Interactive maps: `remove_layer` doesn't cause an error
- No more errors when creating a `wf.parameter` for `Datetime` and other complex types
- `.where` no longer causes a backend error
- Calling `wf.map.geocontext()` when the map is not fully initialized raises an informative error
- Operations on numbers computed from raster data (like `img_collection.mean(axis=None)`) no longer fail when computed
- Colormap succeeds when the Image contains only 1 value

## [0.25.0] - 2019-08-22
### Added

### Changed
- `Raster.stack` `max_workers` is limited to 25 workers, and will raise a warning and set the value to 25 if a value more than 25 is specified.

### Workflows (channel `v0-7`) - Added
- Interactive maps: `clear_layers` and `remove_layer` methods
- ImageCollections: `reversed` operator
- ImageCollections: `concat` and `sorted` methods
- ImageCollections: `head`, `tail`, and `partition` methods for slicing
- ImageCollections: `where` method for filtering by condition
- ImageCollections `map_window` method for applying sliding windows
- ImageCollections: Indexing into ImageCollections is supported (`imgs[1]`)
- **[breaking]** Statistics functions are now applied to named axes
- DateTime, Timedelta, Geocontext, Bool, and Geometry are now computable
- ImageCollectionGroupby ProxyObject for grouping ImageCollection by properties, and applying functions over groups
- ImageCollections: `groupby` method
- `parameter` constructor

### Workflows - Changed
- Interactive maps: autoscaling is now done in the background
- Tiles requests can now include parameters
- `median` is noticeably faster
- `count` is no longer breaks colormaps
- `map`, `filter`, and `reduce` are 2x faster in the "PREPARING" stage
- Significantly better performance for functions that reference variables outside their scope, like
```
overall_comp = ndvi.mean(axis="images")
deltas = ndvi.map(lambda img: img - overall_comp)
```
- Full support for floor-division (`//`) between Datetimes and Timedeltas (`imgs.filter(lambda img: img.properties['date'] // wf.Timedelta(days=14)`)

### Workflows - Removed
- **[breaking]** `ImageCollection.one` (in favor of indexing)

## [0.24.0] - 2019-08-01
### Added
- `scenes.DLTile.assign(pad=...)` method added to ease creation of a tile in all ways indentical except for the padding.

### Changed
- The parameter `nbits` has been deprecated for catalog bands.

### Workflows (channel `v0-6`) - Added
- New interactive map, with GUI controls for multiple layers, scaling, and colormaps.
- Colormaps for single-band images.
- Map interface displays errors that occur while the backend is rendering images.
- ImageCollection compositing no longer changes band names (`red` does not become `red_mean`, for example).
- `.clip()` and `.scale()` methods for Image/ImageCollection.
- Support specifying raster resampler method.
- Support specifying raster processing level: `toa` (top-of-atmosphere) or `surface` [surface reflectance).
- No more tiles 400s for missing data; missing/masked pixels can optionally be filled with a checkerboard pattern.

### Workflows - Changed
- Workflows `Image.concat` renamed `Image.concat_bands`.
- Data are left in `data_range` values if `physical_range` is not set, instead of scaling to the range `0..1`.
- Selecting the same band name twice (`img.pick_bands("vv vv")`) properly raises an error.
- Reduced `DeprecationWarning`s in Python 3.7.

## [0.23.0] - 2019-07-12
### Added
- Alpha Workflows API client has been added. Access to the Workflows backend is restricted; contact [support](https://descarteslabs.atlassian.net/servicedesk/customer/portals) for more information.
- Workflows support for Python 3 added in channel v0-5.

### Changed

## [0.22.0] - 2019-07-09
### Added
- Scenes API now supports band scaling and output type specification for rastering methods.
- Methods in the Metadata, Raster, and Vector service clients that accepted GeoJSON geometries now also accept Shapely geometries.

### Changed

## [0.21.0] - 2019-06-19
### Added
- Add support for user cython modules in tasks.

### Changed
- Tasks webhook methods no longer require a `group_id` if a webhook id is provided.
- `catalog_id` property on images is no longer supported by the API
- Fix `scenes.display` handling of single band masked arrays with scalar masks
- Fix problems with incomplete `UploadTask` instances returned by `vectors.FeatureCollection.list_uploads`

## [0.20.0] - 2019-06-04
### Added
- Metadata, Catalog, and Scenes now support a new `storage_state` property for managing image metadata and filtering search results. `storage_state="available"` is the default for new images and indicates that the raster data for that scene is available on the Descartes Labs Platform. `storage_state="remote"` indicates that the raster data has not yet been processed and made available to client users.
- The following additional colormaps are now supported for bands – 'cool', 'coolwarm', 'hot', 'bwr', 'gist_earth', 'terrain'. Find more details about the colormaps [here](https://matplotlib.org/gallery/color/colormap_reference.html).
- `Scene.ndarray`, `SceneCollection.stack`, and `SceneCollection.mosaic` now support passing a string as the `mask_alpha` argument to allow users to specify an alternate band name to use for masking.
- Scenes now supports a new `save_image` function that allows a user to save a visualization given a filename and extension.
- Tasks now allows you to unambiguously get a function by group id using `get_function_by_id`.
- All Client APIs now accept a `retries` argument to override the default retry configuration. The default remains
the same as the prior behavior, which is to attempt 3 retries on errors which can be retried.

### Changed
- Bands of different but compatible types can now be rastered together in `Scene.ndarray()` and `Scene.download()` as well as across multiple scenes in `SceneCollection.mosaic()`, `SceneCollection.stack()` and `SceneCollection.download()`. The result will have the most general data type.
- Vector client functions that accept a `geometry` argument now support passing Shapely shapes in addition to GeoJSON.

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
- Offset keyword argument in metadata.search has been deprecated. Please use the
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
