from enum import Enum
import time
import itertools
import urllib3.exceptions
import requests.exceptions
import warnings

from descarteslabs.exceptions import ServerError

from .catalog_base import CatalogObjectBase, check_deleted
from .attributes import (
    Attribute,
    CatalogObjectReference,
    Timestamp,
    EnumAttribute,
    MappingAttribute,
    ListAttribute,
    TypedAttribute,
)
from .image import Image

from concurrent.futures import TimeoutError


class ImageUploadType(str, Enum):
    """The type of upload data.

    Attributes
    ----------
    NDARRAY : enum
        An multidimensional, homogeneous array of fixed-size items representing one
        or more images.
    FILE : enum
        A file on disk containing one or more images.
    """

    NDARRAY = "ndarray"
    FILE = "file"


class OverviewResampler(str, Enum):
    """Allowed overview resampler algorithms.

    Attributes
    ----------
    NEAREST : enum
        Applies a nearest neighbour (simple sampling) resampler
    AVERAGE : enum
        Computes the average of all non-NODATA contributing pixels.
    GAUSS : enum
        Applies a Gaussian kernel before computing the overview, which can lead to
        better results than simple averaging in e.g case of sharp edges with high
        contrast or noisy patterns.
    CUBIC : enum
        Applies a cubic convolution kernel.
    CUBICSPLINE : enum
        Applies a B-Spline convolution kernel.
    LANCZOS : enum
        Applies a Lanczos windowed sinc convolution kernel.
    AVERAGE_MP : enum
        Averages complex data in mag/phase space.
    AVERAGE_MAGPHASE : enum
        average_magphase
    MODE : enum
        Selects the value which appears most often of all the sampled points.
    """

    NEAREST = "nearest"
    AVERAGE = "average"
    GAUSS = "gauss"
    CUBIC = "cubic"
    CUBICSPLINE = "cubicspline"
    LANCZOS = "lanczos"
    AVERAGE_MP = "average_mp"
    AVERAGE_MAGPHASE = "average_magphase"
    MODE = "mode"


class ImageUploadStatus(str, Enum):
    """The status of the image upload operation.

    Attributes
    ----------
    TRANSFERRING : enum
        Upload has been initiated and file(s) are being transfered from
        the client to the platform.
    PENDING : enum
        The files were transfered to the platform, and are waiting for processing to begin.
    RUNNING : enum
        The processing step is currently running.
    SUCCESS : enum
        The upload processing completed successfully and the new image is available.
    FAILURE : enum
        The upload failed; error information is available.
    CANCELED : enum
        The upload was canceled by the user prior to completion.
    """

    TRANSFERRING = "transferring"
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILURE = "failure"
    CANCELED = "canceled"


class ImageUploadEventType(str, Enum):
    """The type of the image upload event.

    Attributes
    ----------
    QUEUE : enum
        The transfer of the file(s) was completed, and the upload processing
        request has been issued.
    CANCEL : enum
        The user has requested that the upload be canceled. If processing is
        already underway, it will continue.
    RUN : enum
        The processing step is starting.
    COMPLETE : enum
        All processing has completed. The upload status will reflect
        success, failure, or cancellation.
    ERROR : enum
        An error has been detected, but the operation may continue or be
        retried.
    TIMEOUT : enum
        The upload operation has timed out, and will be retried.
    LOG : enum
        The event contains logging output.
    USAGE : enum
        The event contains process resource usage information.
    """

    QUEUE = "queue"
    CANCEL = "cancel"
    RUN = "run"
    COMPLETE = "complete"
    ERROR = "error"
    TIMEOUT = "timeout"
    LOG = "log"
    USAGE = "usage"


class ImageUploadEventSeverity(str, Enum):
    """The severity of an image upload event.

    The severity values duplicate the standard python logging package
    level names and have the same meaning.

    Attributes
    ----------
    CRITICAL : enum
        Critical (error) event.
    ERROR : enum
        Error event.
    WARNING : enum
        Warning event.
    INFO : enum
        Informational event.
    DEBUG : enum
        Debug event.
    """

    CRITICAL = "CRITICAL"
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"
    DEBUG = "DEBUG"


class ImageUploadOptions(MappingAttribute):
    """Control of the upload process.

    Attributes
    ----------
    upload_type : str or ImageUploadType
        Type of upload job, see `ImageUploadType`.
    image_files : list(str)
        File basenames of the uploaded files.
    overviews : list(int)
        Overview generation control, only used when `upload_type` is
        `ImageUploadType.NDARRAY`.
    overview_resampler : str or OverviewResampler
        Overview resampler method, only used when `upload_type` is
        `ImageUploadType.NDARRAY`.
    upload_size : int
        When `upload_type` is `ImageUploadType.NDARRAY`,
        the total size of the array in bytes.
    """

    upload_type = EnumAttribute(ImageUploadType)
    image_files = ListAttribute(Attribute)
    overviews = ListAttribute(Attribute)
    overview_resampler = EnumAttribute(OverviewResampler)
    upload_size = Attribute()
    # worker_tag is for development and testing and should not be used by ordinary
    # clients, and as such is not documented above.
    worker_tag = Attribute()


class ImageUploadEvent(MappingAttribute):
    """Image upload event data.

    During the sequence of steps in the life-cycle of an upload, events are recorded
    at each change in upload status and as responsibility for the upload passes between
    different subsystems (referred to here as "components"). While the `ImageUpload`
    object provides the current status of the upload and the time at which that
    status was reached, the events associated with an upload record the circumstances
    for each of the changes in the upload status as they occurred.

    A typical upload, once complete, will have four events with the following
    `event_type`:

    * `ImageUploadEventType.QUEUE`
    * `ImageUploadEventType.RUN`
    * `ImageUploadEventType.USAGE`
    * `ImageUploadEventType.COMPLETE`

    """

    _doc_type = "image_upload_event"

    id = Attribute(readonly=True, doc="str: Unique id for the event.")
    event_datetime = Timestamp(
        readonly=True, doc="datetime: The time at which the event occurred."
    )
    component = Attribute(
        readonly=True,
        doc="""str: The component which generated the event.

        The value of this field depends on the internal details of how images are
        uploaded, but is useful to support personnel for understanding where a failure
        may have occurred.
        """,
    )
    component_id = Attribute(
        readonly=True,
        doc="""str: The unique identifier for the component instance which generated the event.

        This identifier is useful to support personnel for tracking down any errors
        which may have occurred.
        """,
    )
    event_type = EnumAttribute(
        ImageUploadEventType,
        readonly=True,
        doc="ImageUploadEventType: The type of the event.",
    )
    severity = EnumAttribute(
        ImageUploadEventSeverity,
        readonly=True,
        doc="ImageUploadEventSeverity: The severity of the event.",
    )
    message = Attribute(
        readonly=True, doc="str: Any message associated with the event."
    )


class ImageUpload(CatalogObjectBase):
    """The status object returned when you upload an image using
    :py:meth:`~descarteslabs.catalog.Image.upload` or
    :py:meth:`~descarteslabs.catalog.Image.upload_ndarray`.
    """

    _POLLING_INTERVALS = [1, 1, 1, 1, 1, 5, 10, 10, 30, 60]
    _TERMINAL_STATES = (
        ImageUploadStatus.SUCCESS,
        ImageUploadStatus.FAILURE,
        ImageUploadStatus.CANCELED,
    )

    _upload_model_classes = {ImageUploadEvent._doc_type: ImageUploadEvent}

    _doc_type = "image_upload"
    _url = "/uploads_v2"
    INCLUDE_EVENTS = "events"
    _default_includes = [INCLUDE_EVENTS]
    _no_inherit = True

    id = TypedAttribute(
        str,
        mutable=False,
        serializable=False,
        doc="str: Globally unique identifier for the upload.",
    )
    product_id = TypedAttribute(
        attribute_type=str,
        mutable=False,
        doc="""str: Product id of the product for this imagery.

        The product id for the `~descarteslabs.catalog.Product` to which this imagery
        will be uploaded.

        *Filterable, sortable*.
        """,
    )
    image_id = TypedAttribute(
        str,
        mutable=False,
        doc="""str: Image id of the image for this imagery.

        The image id for the `~descarteslabs.catalog.Image` to which this imagery will
        be uploaded.  This is identical to `image`.id.

        *Filterable*.
        """,
    )
    image = CatalogObjectReference(
        Image,
        require_unsaved=True,
        mutable=False,
        serializable=True,
        sticky=True,
        doc="""~descarteslabs.catalog.Image: Image instance with all desired metadata fields.

        Note that any values will override those determined from the image files
        themselves.
        """,
    )
    image_upload_options = ImageUploadOptions(
        sticky=True,
        mutable=False,
        doc="ImageUploadOptions: Control of the upload process.",
    )
    user = Attribute(
        readonly=True,
        doc="""str: The User ID of the user requesting the upload.

        *Filterable, sortable*.
        """,
    )
    resumable_urls = ListAttribute(
        Attribute,
        readonly=True,
        doc="""list(str): Upload URLs to which the client will transfer the file contents.

        This field is for internal use by the client only.
        """,
    )
    status = EnumAttribute(
        ImageUploadStatus,
        doc="""str or ImageUploadStatus: Current job status.

        To retrieve the latest status, use :py:meth:`reload`.

        *Filterable, sortable*.
        """,
    )
    events = ListAttribute(
        ImageUploadEvent,
        readonly=True,
        doc="list(ImageUploadEvent): List of events pertaining to the upload process.",
    )

    def _initialize(
        self,
        id=None,
        saved=False,
        relationships=None,
        related_objects=None,
        deleted=False,
        **kwargs
    ):
        # CatalogObjectBase only supports many to one, we need the other direction
        if relationships and related_objects:
            for name, relationship in relationships.items():
                # we depend on our attribute name (e.g. "events") being the same as the upstream
                value = []
                for related in relationship["data"]:
                    value.append(related_objects.get((related["type"], related["id"])))
                kwargs[name] = value

        super(ImageUpload, self)._initialize(
            id=id, saved=saved, deleted=deleted, **kwargs
        )

    @classmethod
    def search(cls, client=None, includes=True):
        """A search query for all uploads.

        Return an `Search` instance for searching image uploads.

        Parameters
        ----------
        includes : bool
            Controls the inclusion of events. If True, includes these objects.
            If False, no events are included. Defaults to True.
        client : :class:`CatalogClient`, optional
            A `CatalogClient` instance to use for requests to the Descartes Labs
            catalog.

        Returns
        -------
        :class:`~descarteslabs.catalog.search.Search`
            An instance of the `Search` class

        Example
        -------
        >>> from descarteslabs.catalog import (
        ...     ImageUpload,
        ...     ImageUploadStatus,
        ...     properties as p,
        ... )
        >>> search = ImageUpload.search().filter(p.status == ImageUploadStatus.FAILURE)
        >>> for result in search: # doctest: +SKIP
        ...     print(result) # doctest: +SKIP

        """
        from .search import Search

        return Search(cls, client=client, includes=includes)

    def wait_for_completion(self, timeout=None, warn_transient_errors=True):
        """Wait for the upload to complete.

        Parameters
        ----------
        timeout : int, optional
            If specified, will wait up to specified number of seconds and will raise
            a `concurrent.futures.TimeoutError` if the upload has not completed.
        warn_transient_errors : bool, optional, default True
            Any transient errors while periodically checking upload status are suppressed.
            If True, those errors will be printed as warnings.

        Raises
        ------
        concurrent.futures.TimeoutError
            If the specified timeout elapses and the upload has not completed.
        """
        if self.status in self._TERMINAL_STATES:
            return

        if timeout:
            timeout = time.time() + timeout
        intervals = itertools.chain(
            self._POLLING_INTERVALS, itertools.repeat(self._POLLING_INTERVALS[-1])
        )
        while True:
            try:
                self.reload()
            except (
                ServerError,
                urllib3.exceptions.MaxRetryError,
                requests.exceptions.RetryError,
                urllib3.exceptions.TimeoutError,
            ) as e:
                # If a reload fails, just try again on the next interval
                if warn_transient_errors:
                    warnings.warn(
                        "In wait_for_completion: error fetching status for ImageUpload {!r}; "
                        "will retry: {}".format(self.id, e)
                    )
            if self.status in self._TERMINAL_STATES:
                return
            interval = next(intervals)
            if timeout:
                t = timeout - time.time()
                if t <= 0:
                    raise TimeoutError()
                t = min(t, interval)
            else:
                t = interval
            time.sleep(t)

    def reload(self):
        """Reload all attributes from the Descartes Labs catalog.

        Refresh the state of this upload object.  The instance
        state must be in the `~descarteslabs.catalog.DocumentState.SAVED` state.
        If the status changes to ``ImageUploadStatus.SUCCESS`` then the `image`
        instance is also reloaded so that it contains the full state of the newly
        loaded image.

        Raises
        ------
        NotFoundError
            If the object no longer exists.
        ValueError
            If the catalog object is not in the ``SAVED`` state.
        DeletedObjectError
            If this catalog object was deleted.
        """
        oldstatus = self.status
        super(ImageUpload, self).reload()
        if self.status == ImageUploadStatus.SUCCESS and oldstatus != self.status:
            # image is not in a saved state, so doctor it up
            self.image._saved = True
            self.image._clear_modified_attributes()
            self.image.reload()

    @check_deleted
    def cancel(self):
        """Cancel the upload if it is not yet completed.

        Note that if the upload process is already running, it
        cannot be canceled unless a retryable error occurs.

        Raises
        ------
        NotFoundError
            If the object no longer exists.
        ValueError
            If the catalog object is not in the ``SAVED`` state.
        DeletedObjectError
            If this catalog object was deleted.
        ConflictError
            If the upload has a current status which does not allow it to be canceled.
        """
        self.status = ImageUploadStatus.CANCELED
        self.save()

    @classmethod
    def _load_related_objects(cls, response, client):
        """
        The relationships of the ImageUpload are not first-class CatalogObjects,
        so we need slightly different handling here.
        """
        related_objects = {}
        related_objects_serialized = response.get("included")
        if related_objects_serialized:
            for serialized in related_objects_serialized:
                model_class = cls._upload_model_classes[serialized["type"]]
                if model_class:
                    related = model_class(
                        validate=False, id=serialized["id"], **serialized["attributes"]
                    )
                    related_objects[(serialized["type"], serialized["id"])] = related

        return related_objects

    @classmethod
    def delete(cls, id, client=None, ignore_missing=False):
        """You cannot delete an ImageUpload.

        Raises
        ------
        NotImplementedError
            This method is not supported for ImageUploads.
        """
        raise NotImplementedError("Deleting ImageUploads is not permitted")

    def _instance_delete(self, ignore_missing=False):
        raise NotImplementedError("Deleting ImageUploads is not permitted")
