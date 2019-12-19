from enum import Enum
import uuid
import time

from .catalog_base import CatalogObject
from .attributes import (
    DocumentState,
    Attribute,
    CatalogObjectReference,
    ImmutableTimestamp,
    EnumAttribute,
    MappingAttribute,
    ListAttribute,
)
from .image import Image

from descarteslabs.client.exceptions import NotFoundError
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
    """The status of the image upload.

    Attributes
    ----------
    UPLOADING
        File(s) are being uploaded from client.
    PENDING
        The files were uploaded, awaiting processing.
    RUNNING
        The data is being processed.
    SUCCESS
        The upload completed successfully.
    FAILURE
        The upload failed; error information is available.
    CANCELED
        The upload was canceled by the user.
    """

    UPLOADING = "uploading"
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILURE = "failure"
    CANCELED = "canceled"


class ImageUploadOptions(MappingAttribute):
    """Image upload processing options.

    Attributes
    ----------
    upload_type : str
        Required: Type of upload job, see :class:`ImageUploadType`.
    image_files : list(str)
        Required: file basenames of the uploaded files.
    overviews : list(int)
        Overview generation control, only used when ``upload_type ==
        ImageUploadType::NDARRAY``.
    overview_resampler : :class:`OverviewResampler`
        Overview resampler method, only used when ``upload_type ==
        ImageUploadType::NDARRAY``.
    """

    upload_type = EnumAttribute(ImageUploadType)
    image_files = ListAttribute(Attribute)
    overviews = ListAttribute(Attribute)
    overview_resampler = EnumAttribute(OverviewResampler)


class UploadError(MappingAttribute):
    """Image upload error data.

    Attributes
    ----------
    stacktrace : str
        Stacktrace from the error generated by the ImageUpload operation.
    error_type: str
        Type of exception triggered.
    component : str
        Component of the upload process that triggered the error.
    component_id : str
        Identifier for the `component` that errored.
        If `component` for the error is `worker` this attribute represents the `result_key` of the `Task`.
    """

    stacktrace = Attribute(_mutable=False)
    error_type = Attribute(_mutable=False)
    component = Attribute(_mutable=False)
    component_id = Attribute(_mutable=False)


class ImageUpload(CatalogObject):
    # Be aware that the `|` characters below add whitespace.  The first one is needed
    # avoid the `Inheritance` section from appearing before the auto summary.
    """The informational object returned when you upload an image using
    `~descarteslabs.catalog.Image.upload` or
    `~descarteslabs.catalog.Image.upload_ndarray`.

    |

    Inheritance
    -----------
    For inherited parameters, methods, attributes, and properties, please refer to the
    base class:

    * :py:class:`descarteslabs.catalog.CatalogObject`

    |

    Attributes
    ----------
    id : str
        Globally unique identifier for the upload.
    product_id : str
        Product id for the `~descarteslabs.catalog.Product` to which this imagery will
        be uploaded.
    image_id : str
        Image id for the `~descarteslabs.catalog.Image` to which this imagery will be
        uploaded.
    image : :class:`~descarteslabs.catalog.Image`
        `~descarteslabs.catalog.Image` instance with all desired metadata fields (any
        values will override those determined from the image files themselves).
    image_upload_options : :class:`ImageUploadOptions`
        Optional control of the upload process, see :class:`ImageUploadOptions`.
    resumable_urls : list(str)
        Upload URLs (one per `ImageUploadOptions.image_files` element) to which the
        client will upload the file contents.
    status : str
        Current job status, see :class:`ImageUploadStatus`.
    start_datetime : str, datetime-like
        Starting time for upload process.
    end_datetime : str, datetime-like
        Ending time for upload process.
    job_id : str
        Unique identifier for the internal asynchronous upload process.
    events : list
        List of events pertaining to the upload process.
    errors : list
        List of any errors encountered during the upload process if the upload failed.
    """

    _POLLING_INTERVAL = 60
    _TERMINAL_STATES = (
        ImageUploadStatus.SUCCESS,
        ImageUploadStatus.FAILURE,
        ImageUploadStatus.CANCELED,
    )

    _doc_type = "image_upload"
    _url = "/uploads"

    product_id = Attribute(_mutable=False)
    image_id = Attribute(_mutable=False)
    image = CatalogObjectReference(
        Image, _allow_unsaved=True, _mutable=False, _serializable=True, _sticky=True
    )
    image_upload_options = ImageUploadOptions(_sticky=True)
    resumable_urls = Attribute(_serializable=False)
    status = EnumAttribute(ImageUploadStatus)
    start_datetime = ImmutableTimestamp()
    end_datetime = ImmutableTimestamp()
    job_id = Attribute()
    events = Attribute()
    errors = ListAttribute(UploadError, _mutable=False)

    def __init__(self, **kwargs):
        if kwargs.get("id") is None:
            kwargs["id"] = str(uuid.uuid4())

        super(ImageUpload, self).__init__(**kwargs)

    # override this to interpret 404s as PENDING and to not expect complete object
    # state until new ingest is available
    def reload(self):
        """Reload all attributes from the Descartes Labs catalog.

        Refresh the state of this catalog object from the object in the Descartes Labs
        catalog.  This way you can refresh the status and other attributes.

        Raises
        ------
        NotFoundError
            If the object no longer exists
        ValueError
            If the catalog object was not in the ``SAVED`` state.
        """
        if self.state != DocumentState.SAVED:
            raise ValueError(
                "{} instance with id {} has not been saved".format(
                    self.__class__.__name__, self.id
                )
            )

        try:
            data, related_objects = self._send_data(
                id=self.id, method=self._RequestMethod.GET, client=self._client
            )
        except NotFoundError:
            # with tasks-based ingest, jobs are unknown until completed
            if self.status == ImageUploadStatus.PENDING:
                self._clear_modified_attributes()
                return
            raise

        self._initialize(id=data["id"], saved=True, **data["attributes"])

    def wait_for_completion(self, timeout=None):
        """Wait for the upload to complete.

        Parameters
        ----------
        timeout : int, optional
            If specified, will wait up to specified number of seconds and will raise
            a `concurrent.futures.TimeoutError` if the upload has not completed.

        Raises
        ------
        concurrent.futures.TimeoutError
            If the specified timeout elapses and the upload has not completed.
        """
        if self.status in self._TERMINAL_STATES:
            return

        if timeout:
            timeout = time.time() + timeout
        while True:
            self.reload()
            if self.status in self._TERMINAL_STATES:
                return
            if timeout:
                t = timeout - time.time()
                if t <= 0:
                    raise TimeoutError()
                t = min(t, self._POLLING_INTERVAL)
            else:
                t = self._POLLING_INTERVAL
            time.sleep(t)
