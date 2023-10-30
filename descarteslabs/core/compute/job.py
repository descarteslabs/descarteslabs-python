# Copyright 2018-2023 Descartes Labs.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import time
import warnings
from datetime import datetime
from typing import TYPE_CHECKING, Dict, List, Optional, Type

from strenum import StrEnum

from descarteslabs.exceptions import NotFoundError

from ..catalog import Blob, CatalogClient, DeletedObjectError, StorageType
from ..common.client import (
    Attribute,
    DatetimeAttribute,
    Document,
    DocumentState,
    ListAttribute,
    Search,
)
from .compute_client import ComputeClient
from .job_statistics import JobStatistics
from .result import Serializable

if TYPE_CHECKING:
    from .function import Function


class JobStatus(StrEnum):
    """The status of the Job."""

    PENDING = "pending"
    RUNNING = "running"
    CANCEL = "cancel"
    CANCELING = "canceling"
    SUCCESS = "success"
    FAILURE = "failure"
    TIMEOUT = "timeout"
    CANCELED = "canceled"

    @classmethod
    def terminal(cls):
        return [
            cls.SUCCESS,
            cls.FAILURE,
            cls.TIMEOUT,
            cls.CANCELED,
        ]


class JobSearch(Search["Job"]):
    def cancel(self):
        response = self._client.session.post(
            "/jobs/cancel", json=self._serialize(json_encode=False)
        )
        return [Job(**job, client=self._client, saved=True) for job in response.json()]

    def rerun(self):
        response = self._client.session.post(
            "/jobs/rerun", json=self._serialize(json_encode=False)
        )
        return [Job(**job, client=self._client, saved=True) for job in response.json()]

    def delete(self, delete_results: bool = False):
        json = self._serialize(json_encode=False)
        json["delete_results"] = delete_results
        response = self._client.session.post("/jobs/delete", json=json)
        return response.json()


class Job(Document):
    """A single invocation of a Function."""

    id: str = Attribute(
        str, filterable=True, readonly=True, sortable=True, doc="The ID of the Job."
    )
    function_id: str = Attribute(
        str,
        filterable=True,
        mutable=False,
        doc="The ID of the Function the Job belongs to.",
    )
    creation_date: datetime = DatetimeAttribute(
        filterable=True,
        readonly=True,
        sortable=True,
        doc="The date the Job was created.",
    )
    args: Optional[List] = Attribute(list, doc="The arguments provided to the Job.")
    error_reason: Optional[str] = Attribute(
        str,
        readonly=True,
        doc="The reason the Job failed.",
    )
    execution_count: Optional[int] = Attribute(
        int,
        filterable=True,
        readonly=True,
        sortable=True,
        doc="The number of attempts made to execute this job.",
    )
    exit_code: Optional[int] = Attribute(
        int,
        filterable=True,
        readonly=True,
        sortable=True,
        doc="The exit code of the Job.",
    )
    kwargs: Optional[Dict] = Attribute(dict, doc="The parameters provided to the Job.")
    last_completion_date: Optional[datetime] = DatetimeAttribute(
        filterable=True,
        readonly=True,
        sortable=True,
        doc="The date the Job was last completed or canceled.",
    )
    last_execution_date: Optional[datetime] = DatetimeAttribute(
        filterable=True,
        readonly=True,
        sortable=True,
        doc="The date the Job was last executed.",
    )
    runtime: Optional[int] = Attribute(
        int,
        filterable=True,
        readonly=True,
        sortable=True,
        doc="The time it took the Job to complete.",
    )
    status: JobStatus = Attribute(
        JobStatus,
        filterable=True,
        readonly=True,
        sortable=True,
        doc="""The current status of the Job.

        The status may occasionally need to be refreshed by calling :py:meth:`Job.refresh`
        """,
    )
    statistics: Optional[Dict] = Attribute(
        JobStatistics,
        readonly=True,
        doc="""The runtime utilization statistics for the Job.

        The statistics include the cpu, memory, and network usage of the Job.
        """,
    )
    tags: List[str] = ListAttribute(
        str,
        filterable=True,
        doc="A list of tags associated with the Job.",
    )

    # Lazy attributes
    provisioning_time: Optional[int] = Attribute(
        int,
        readonly=True,
        doc=(
            "The time it took to provision the Job. This attribute will only be available "
            "if include='timings' is specified in the request by setting params.",
        ),
    )
    pull_time: Optional[int] = Attribute(
        int,
        readonly=True,
        doc=(
            "The time it took to load the user code in the Job. This attribute will only"
            " be available if include='timings' is specified in the request by setting params.",
        ),
    )

    def __init__(
        self,
        function_id: str,
        args: Optional[List] = None,
        kwargs: Optional[Dict] = None,
        client: ComputeClient = None,
        **extra,
    ):
        """
        Parameters
        ----------
        function_id : str
            The id of the Function. A function must first be created to create a job.
        args : List, optional
            A list of positional arguments to pass to the function.
        kwargs : Dict, optional
            A dictionary of named arguments to pass to the function.
        client: ComputeClient, optional
            The compute client to use for requests.
            If not set, the default client will be used.
        """
        self._client = client or ComputeClient.get_default_client()
        super().__init__(function_id=function_id, args=args, kwargs=kwargs, **extra)

    # support use of jobs in sets
    def __hash__(self):
        return hash(self.id)

    # support use of jobs in sets
    def __eq__(self, other):
        if not isinstance(other, Job):
            return False

        return self.id == other.id

    def _get_result_namespace(self) -> str:
        """Returns the namespace for the Job result blob."""
        namespace = self._client.get_namespace(self.function_id)

        if not namespace:
            # Fetching the function from the server will set the namespace
            # during hydration in Function.__init__
            namespace = self.function.namespace

        return namespace

    @property
    def function(self) -> "Function":
        """Returns the Function the Job belongs to."""
        from .function import Function

        return Function.get(self.function_id)

    @classmethod
    def get(cls, id, client: ComputeClient = None, **params) -> "Job":
        """Retrieves the Job by id.

        Parameters
        ----------
        id : str
            The id of the Job to fetch.
        client: ComputeClient, optional
            If set, the result will be retrieved using the configured client.
            Otherwise, the default client will be used.
        include : List[str], optional
            List of additional attributes to include in the response.
            Allowed values are:

            - "timings": Include additional debugging timing information about the Job.

        Example
        -------
        >>> from descarteslabs.compute import Job
        >>> job = Job.get(<job-id>)
        Job <job-id>: pending
        """
        client = client or ComputeClient.get_default_client()
        response = client.session.get(f"/jobs/{id}", params=params)
        return cls(**response.json(), client=client, saved=True)

    @classmethod
    def list(
        cls, page_size: int = 100, client: ComputeClient = None, **params
    ) -> JobSearch:
        """Retrieves an iterable of all jobs matching the given parameters.

        If you would like to filter Jobs, use :py:meth:`Job.search`.

        Parameters
        ----------
        page_size : int, default=100
            Maximum number of results per page.
        client: ComputeClient, optional
            If set, the result will be retrieved using the configured client.
            Otherwise, the default client will be used.

        Example
        -------
        >>> from descarteslabs.compute import Job
        >>> fn = Job.list(<function_id>)
        [Job <job-id1>: pending, Job <job-id2>: pending, Job <job-id3>: pending]
        """
        params = {"page_size": page_size, **params}
        search = Job.search(client=client).param(**params)

        # Deprecation: remove this in a future release
        if "function_id" in params or "status" in params:
            examples = []

            if "function_id" in params:
                examples.append(f"Job.function_id == '{params['function_id']}'")

            if "status" in params:
                if not isinstance(params["status"], list):
                    params["status"] = [params["status"]]

                examples.append(f"Job.status.in_({params['status']})")

            warnings.warn(
                "The `function_id` parameter is deprecated. "
                "Use `Job.search().filter({})` instead.".format(" & ".join(examples))
            )

        return search

    def cancel(self):
        """Cancels the Job.

        If the Job is already canceled or completed, this will do nothing.

        If the Job is still pending, it will be canceled immediately.

        If the job is running, it will be canceled as soon as possible. However, it may
        complete before the cancel request is processed.
        """
        if self.state != DocumentState.SAVED:
            raise ValueError("Cannot cancel a Job that has not been saved")

        response = self._client.session.post(f"/jobs/{self.id}/cancel")
        self._load_from_remote(response.json())

    def delete(self, delete_result: bool = False):
        """Deletes the Job.

        Also deletes any job log blob for the job. Use `delete_result=True` to delete the
        job result blob as well.

        Parameters
        ----------
        delete_result : bool, False
            If set, the result of the job will also be deleted.
        """
        if self.state == DocumentState.NEW:
            raise ValueError("Cannot delete a Job that has not been saved")

        self._client.session.delete(f"/jobs/{self.id}?delete_results={delete_result}")
        self._deleted = True

    def refresh(self, client: ComputeClient = None) -> None:
        """Update the Job instance with the latest information from the server.

        Parameters
        ----------
        client: ComputeClient, optional
            If set, the result will be retrieved using the configured client.
            Otherwise, the default client will be used.
        """
        if self.pull_time or self.provisioning_time:
            params = {"include": ["timings"]}
        else:
            params = {}

        response = self._client.session.get(f"/jobs/{self.id}", params=params)
        self._load_from_remote(response.json())

    def result(
        self,
        cast_type: Optional[Type[Serializable]] = None,
        catalog_client: CatalogClient = None,
    ):
        """Retrieves the result of the Job.

        Parameters
        ----------
        cast_type: Type[Serializable], None
            If set, the result will be deserialized to the given type.
        catalog_client: CatalogClient, None
            If set, the result will be retrieved using the configured catalog client.
            Otherwise, the default catalog client will be used.

        Raises
        ------
        ValueError
            When job has not completed successfully or
            when `cast_type` does not implement Serializable.
        """
        if self.status != JobStatus.SUCCESS:
            # just check if maybe it is meanwhile done.
            self.refresh()
            if self.status != JobStatus.SUCCESS:
                if self.status in JobStatus.terminal():
                    raise ValueError(
                        f"Job {self.id} has not completed successfully, status is {self.status}"
                    )
                else:
                    raise ValueError(
                        f"Job {self.id} has not completed, status is {self.status}. "
                        "Please wait for the job to complete."
                    )

        if not catalog_client:
            catalog_client = self._client.catalog_client

        try:
            namespace = self._get_result_namespace()
            result = Blob.get_data(
                id=f"{StorageType.COMPUTE}/{namespace}/{self.function_id}/{self.id}",
                client=catalog_client,
            )
        except NotFoundError:
            return None
        except ValueError:
            raise
        except DeletedObjectError:
            raise

        if not result:
            return None

        if cast_type:
            deserialize = getattr(cast_type, "deserialize", None)

            if deserialize and callable(deserialize):
                return deserialize(result)
            else:
                raise ValueError(f"Type {cast_type} must implement Serializable.")

        try:
            return json.loads(result)
        except Exception:
            return result

    def result_blob(
        self,
        catalog_client: CatalogClient = None,
    ):
        """Retrieves the Catalog Blob holding the result of the Job.

        If there is no result Blob, `None` will be returned.

        Parameters
        ----------
        catalog_client: CatalogClient, None
            If set, the result will be retrieved using the configured client.
            Otherwise, the default client will be used.

        Raises
        ------
        ValueError
            When job has not completed successfully or
            when `cast_type` does not implement Serializable.
        """
        if self.status != JobStatus.SUCCESS:
            # just check if maybe it is meanwhile done.
            self.refresh()
            if self.status != JobStatus.SUCCESS:
                if self.status in JobStatus.terminal():
                    raise ValueError(
                        f"Job {self.id} has not completed successfully, status is {self.status}"
                    )
                else:
                    raise ValueError(
                        f"Job {self.id} has not completed, status is {self.status}. "
                        "Please wait for the job to complete."
                    )

        if not catalog_client:
            catalog_client = self._client.catalog_client

        return Blob.get(
            name=f"{self.function_id}/{self.id}",
            namespace=self._get_result_namespace(),
            storage_type=StorageType.COMPUTE,
            client=catalog_client,
        )

    @classmethod
    def search(cls, client: ComputeClient = None) -> JobSearch:
        """Creates a search for Jobs.

        The search is lazy and will be executed when the search is iterated over or
        :py:meth:`Search.collect` is called.

        Parameters
        ----------
        client: ComputeClient, optional
            If set, the result will be retrieved using the configured client.
            Otherwise, the default client will be used.

        Example
        -------
        >>> from descarteslabs.compute import Job, JobStatus
        >>> jobs: List[Job] = Job.search().filter(Job.status == JobStatus.SUCCESS).collect()
        Collection([Job <job-id1>: success, <job-id2>: success])
        """
        client = client or ComputeClient.get_default_client()
        return JobSearch(Job, client, url="/jobs")

    def wait_for_completion(self, timeout=None, interval=10):
        """Waits until the Job is completed.

        Parameters
        ----------
        timeout : int, default=None
            Maximum time to wait before timing out.
            If not set, the call will block until job completion.
        interval : int, default=10
            Interval in seconds for how often to check if jobs have been completed.
        """
        start_time = time.time()

        while True:
            self.refresh()

            if self.status in JobStatus.terminal():
                break

            if timeout:
                t = timeout - (time.time() - start_time)
                if t <= 0:
                    raise TimeoutError(
                        f"Job {self.id} did not complete before timeout!"
                    )

                t = min(t, interval)
            else:
                t = interval

            time.sleep(t)

    def save(self):
        """Creates the Job if it does not already exist.

        If the job already exists, it will be updated on the server if modifications
        were made to the Job instance.
        """
        if self.state == DocumentState.SAVED:
            return

        if self.state == DocumentState.MODIFIED:
            response = self._client.session.patch(
                f"/jobs/{self.id}", json=self.to_dict(only_modified=True)
            )
        elif self.state == DocumentState.NEW:
            response = self._client.session.post(
                "/jobs", json=self.to_dict(exclude_none=True)
            )
        else:
            raise ValueError(
                f"Unexpected Job state {self.state}."
                f'Reload the job from the server: Job.get("{self.id}")'
            )

        self._load_from_remote(response.json())

    def iter_log(self, timestamps: bool = True):
        """Retrieves the log for the job, returning an iterator over the lines.

        Parameters
        ----------
        timestamps : bool, True
            If set, log timestamps will be included and converted to the users system
            timezone from UTC.

            You may consider disabling this if you use a structured logger.
        """
        return self._client.iter_log_lines(
            f"/jobs/{self.id}/log", timestamps=timestamps
        )

    def log(self, timestamps: bool = True):
        """Retrieves the log for the job, returning a string.

        As logs can potentially be unbounded, consider using :py:meth:`Job.iter_log`.

        Parameters
        ----------
        timestamps : bool, True
            If set, log timestamps will be included and converted to the users system
            timezone from UTC.

            You may consider disabling this if you use a structured logger.
        """
        return "\n".join(self.iter_log(timestamps=timestamps))
