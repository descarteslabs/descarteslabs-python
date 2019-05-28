import time

from descarteslabs.client.services.vector import Vector
from descarteslabs.vectors.exceptions import FailedJobError, WaitTimeoutError


class AsyncJob(object):
    """
    Base class that provides helpers to access information about
    asynchronous jobs produced when interacting with a ``FeatureCollection``.
    """

    COMPLETE_STATUSES = ["DONE", "SUCCESS", "FAILURE"]
    COMPLETION_POLL_INTERVAL_SECONDS = 5

    def __init__(self, id, vector_client=None):
        self.id = id

        self._vector_client = vector_client

        self.refresh()

    @property
    def vector_client(self):
        if self._vector_client is None:
            self._vector_client = Vector()

        return self._vector_client

    def refresh(self):
        """ Refresh the job information."""
        raise NotImplementedError("derived classes must implement refresh")

    def _properties_from_jsonapi(self, response):
        self.properties = response.data.attributes.asdict()

    def _check_complete(self, msg=None):
        if self.state not in AsyncJob.COMPLETE_STATUSES:
            return False

        if self.state == "FAILURE" or self.errors:
            raise FailedJobError(msg)

        return True

    def wait_for_completion(self, timeout=None):
        """
        Wait for a job operation to complete. Copies occur asynchronously
        and can take a long time to complete.  Features will not be accessible
        in the FeatureCollection until the copy completes.

        If the job ran, but failed, a FailedJobError is raised.
        If a timeout is specified and the timeout is reached, a WaitTimeoutError is raised.
        A BadRequestError is raised if the job could not begin execution.

        Parameters
        ----------
        timeout : int
            Number of seconds to wait before the wait times out.  If not specified, will
            wait indefinitely.

        Raises
        ------
        ~descarteslabs.vectors.exceptions.FailedJobError
            Raised when the copy job fails to complete successfully.
        ~descarteslabs.client.exceptions.NotFoundError
            Raised if the product or status cannot be found.
        ~descarteslabs.client.exceptions.RateLimitError
            Raised when too many requests have been made within a given time period.
        ~descarteslabs.client.exceptions.ServerError
            Raised when a unknown error occurred on the server.
        ~descarteslabs.vectors.exceptions.WaitTimeoutError
            Raised when the timeout is exceeded before the job completes.

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection
        >>> aoi_geometry = {
        ...    'type': 'Polygon',
        ...    'coordinates': [[[-109, 31], [-102, 31], [-102, 37], [-109, 37], [-109, 31]]]}
        >>> fc = FeatureCollection('d1349cc2d8854d998aa6da92dc2bd24')  # doctest: +SKIP
        >>> fc.filter(geometry=aoi_geometry)  # doctest: +SKIP
        >>> delete_job = fc.delete_features()  # doctest: +SKIP
        >>> delete_job.wait_for_completion()  # doctest: +SKIP
        """
        start_time = time.time()

        while True:
            self.refresh()

            if self._check_complete():
                break

            time.sleep(AsyncJob.COMPLETION_POLL_INTERVAL_SECONDS)
            if timeout is not None and (time.time() - start_time) > timeout:
                raise WaitTimeoutError("wait timeout reached")

    def _property(self, prop):
        return self.properties.get(prop, None)

    @property
    def state(self):
        """
        str : The state of the job, possible values are ``PENDING``, ``RUNNING``,
        ``DONE``, ``SUCCESS`` and ``FAILED``.
        """
        return self._property("state")

    @property
    def created(self):
        """str : UTC time that the job was created in ISO 8601 format."""
        return self._property("created")

    @property
    def started(self):
        """str : UTC time that the job started running in ISO 8601 format."""
        return self._property("started")

    @property
    def ended(self):
        """str : UTC time that the job stopped running in ISO 8601 format."""
        return self._property("ended")

    @property
    def errors(self):
        """list(str) : Rrrors encountered when running the job, if there were any."""
        return self._property("errors")


class DeleteJob(AsyncJob):
    """
    Job for deleting ``Features`` from a ``FeatureCollection``

    Attributes
    ----------
    id : str
        The unique identifier for the `FeatureCollection` whose ``Features`` the job id deleting.
    state : str
        The state of the job, possible values are ``PENDING``, ``RUNNING``,
        ``DONE``, ``SUCCESS`` and ``FAILED``.
    created : datetime
        UTC time that the job was created.
    started : datetime
        UTC time that the job started running.
    ended : datetime
        UTC time that the job stopped running.
    errors : list(str)
        List of errors encountered when running the job, if there were any.
    """

    def refresh(self):
        """
        Refresh the job information.

        Raises
        ------
        ~descarteslabs.client.exceptions.NotFoundError
            Raised if the product cannot be found.
        ~descarteslabs.client.exceptions.RateLimitError
            Raised when too many requests have been made within a given time period.
        ~descarteslabs.client.exceptions.ServerError
            Raised when a unknown error occurred on the server.
        """

        response = self.vector_client.get_delete_features_status(self.id)
        self._properties_from_jsonapi(response)

    def _check_complete(self):
        return super(DeleteJob, self)._check_complete(
            "delete features from product {}, job {} failed".format(self.id, self.id)
        )


class CopyJob(AsyncJob):
    """
    Job for copying ``Features`` from one ``FeatureCollection`` to a new ``FeatureCollection``.

    Attributes
    ----------
    id : str
        The unique identifier for the `FeatureCollection` the job is creating.
    state : str
        The state of the job, possible values are``PENDING``, ``RUNNING``,
        ``DONE``, ``SUCCESS`` and ``FAILED``.
    created : datetime
        UTC time that the job was created.
    started : datetime
        UTC time that the job started running.
    ended : datetime
        UTC time that the job stopped running.
    errors : list(str)
        List of errors encountered when running the job, if there were any.

    Example
    -------
    >>> from descarteslabs.vectors.async_job import CopyJob
    >>> job = CopyJob('d1349cc2d8854d998aa6da92dc2bd24')      # doctest: +SKIP
    """

    def refresh(self):
        """
        Refresh the job information.

        Raises
        ------
        ~descarteslabs.client.exceptions.NotFoundError
            Raised if the product or status cannot be found.
        ~descarteslabs.client.exceptions.RateLimitError
            Raised when too many requests have been made within a given time period.
        ~descarteslabs.client.exceptions.ServerError
            Raised when a unknown error occurred on the server.
        """
        response = self.vector_client.get_product_from_query_status(self.id)
        self._properties_from_jsonapi(response)

    def _check_complete(self):
        return super(CopyJob, self)._check_complete(
            "copy to product {} from job {} failed".format(self.id, self.id)
        )
