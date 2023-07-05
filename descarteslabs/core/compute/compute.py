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

import glob
import gzip
import inspect
import io
import json
import os
import re
import time
import warnings
import zipfile
from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import Callable, Dict, Iterable, List, Optional, Type, Union

from strenum import StrEnum

from ..client.services.service import ThirdPartyService
from ..common.client import Attribute, DatetimeAttribute, Document, DocumentState
from .compute_client import ComputeClient


class FunctionStatus(StrEnum):
    "The status of the Function."

    AWAITING_BUNDLE = "awaiting_bundle"
    BUILDING = "building"
    BUILD_FAILED = "build_failed"
    RUNNING = "running"
    SUCCESS = "success"
    FAILURE = "failure"


class JobStatus(StrEnum):
    """The status of the Job."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILURE = "failure"
    TIMEOUT = "timeout"


class Serializable:
    def serialize(self) -> bytes:
        raise NotImplementedError()

    @classmethod
    def deserialize(cls, data: bytes):
        raise NotImplementedError()


class Job(Document):
    """A single invocation of a Function."""

    id: str = Attribute(str, readonly=True, doc="The ID of the Job.")
    function_id: str = Attribute(
        str, mutable=False, doc="The ID of the Function the Job belongs to."
    )
    creation_date: datetime = DatetimeAttribute(
        readonly=True, doc="The date the Job was created."
    )
    args: Optional[List] = Attribute(list, doc="The arguments provided to the Job.")
    kwargs: Optional[Dict] = Attribute(dict, doc="The parameters provided to the Job.")
    runtime: Optional[int] = Attribute(
        int, readonly=True, doc="The time it took the Job to complete."
    )
    status: JobStatus = Attribute(
        JobStatus,
        readonly=True,
        doc="""The current status of the Job.

        The status may occasionally need to be refreshed by calling :py:meth:`Job.refresh`
        """,
    )

    def __init__(
        self,
        function_id: str,
        args: Optional[List] = None,
        kwargs: Optional[Dict] = None,
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
        """
        super().__init__(function_id=function_id, args=args, kwargs=kwargs, **extra)

    def __repr__(self) -> str:
        return f"Job {self.id}: {self.status}"

    @property
    def function(self) -> "Function":
        """Returns the Function the Job belongs to."""
        return Function.get(self.function_id)

    @classmethod
    def get(cls, id) -> "Job":
        """Retrieves the Job by id.

        Parameters
        ----------
        id : str
            The id of the Job to fetch.

        Example
        -------
        >>> from descarteslabs.compute import Job
        >>> job = Job.get(<job-id>)
        Job <job-id>: pending
        """
        client = ComputeClient.get_default_client()
        response = client.session.get(f"/jobs/{id}")
        return cls(**response.json(), saved=True)

    @classmethod
    def list(
        cls,
        function_id: str = None,
        status: Union[JobStatus, List[JobStatus]] = None,
        page_size: int = 100,
    ) -> Iterable["Job"]:
        """Retrieves an iterable of all jobs matching the given parameters.

        Parameters
        ----------
        function_id : str, None
            If set, only jobs for the given function will be included.
        status : Union[JobStatus, List[JobStatus]], None
            If set, only jobs matching one of the provided statuses will be included.
        page_size : int, default=100
            Maximum number of results per page.

        Example
        -------
        >>> from descarteslabs.compute import Job
        >>> fn = Job.list(<function_id>)
        [Job <job-id1>: pending, Job <job-id2>: pending, Job <job-id3>: pending]
        """
        client = ComputeClient.get_default_client()
        params = {"page_size": page_size}

        if function_id:
            params["function_id"] = function_id

        if status:
            if not isinstance(status, list):
                status = [status]

            params["status"] = status

        paginator = client.iter_pages("/jobs", params=params)

        for data in paginator:
            yield cls(**data, saved=True)

    def delete(self):
        """Deletes the Job."""
        if self.state == DocumentState.NEW:
            raise ValueError("Cannot delete a Job that has not been saved")

        client = ComputeClient.get_default_client()
        client.session.delete(f"/jobs/{self.id}")
        self._deleted = True

    def refresh(self) -> None:
        """Update the Job instance with the latest information from the server."""
        client = ComputeClient.get_default_client()

        response = client.session.get(f"/jobs/{self.id}")
        self._load_from_remote(response.json())

    def result(self, cast_type: Optional[Type[Serializable]] = None):
        """Retrieves the result of the Job.

        Parameters
        ----------
        cast_type: Type[Serializable], None
            If set, the result will be deserialized to the given type.

        Raises
        ------
        ValueError
            When `cast_type` does not implement Serializable.
        """
        client = ComputeClient.get_default_client()
        response = client.session.get(f"/jobs/{self.id}/result", stream=True)
        result = response.content

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

    def wait_for_completion(self, timeout=None, interval=10):
        """Waits until the Job is completed.

        Parameters
        ----------
        timeout : int, default=None
            Maximum time to wait before timing out.
            If not set, the call will block until job completion.
        interval : int, default=10
            Interval for how often to check if jobs have been completed.
        """
        print(f"Job {self.id} starting status {self.status}")
        last_status = self.status
        start_time = time.time()

        while True:
            self.refresh()

            if self.status != last_status:
                print(f"Job {self.id} updated from {last_status} to {self.status}")
                last_status = self.status

            if self.status in [JobStatus.SUCCESS, JobStatus.FAILURE, JobStatus.TIMEOUT]:
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

        client = ComputeClient.get_default_client()

        if self.state == DocumentState.MODIFIED:
            response = client.session.patch(
                f"/jobs/{self.id}", json=self.to_dict(only_modified=True)
            )
        elif self.state == DocumentState.NEW:
            response = client.session.post(
                "/jobs", json=self.to_dict(exclude_none=True)
            )
        else:
            raise ValueError(
                f"Unexpected Job state {self.state}."
                f'Reload the job from the server: Job.get("{self.id}")'
            )

        self._load_from_remote(response.json())

    def log(self, timestamps: bool = True):
        """Retrieves the log for the job.

        Parameters
        ----------
        timestamps : bool, True
            If set, log timestamps will be included and converted to the users system
            timezone from UTC.

            You may consider disabling this if you use a structured logger.
        """
        client = ComputeClient.get_default_client()
        logs = client.iter_log_lines(f"/jobs/{self.id}/log", timestamps=timestamps)

        for log in logs:
            print(log)


class Cpus(float):
    """Validates CPUs for a Function"""

    NON_NUMERIC = r"[^0-9.]"

    def __new__(cls, value):
        if isinstance(value, str):
            value = re.sub(cls.NON_NUMERIC, "", value)

        return super().__new__(cls, value)


class Memory(int):
    """Validates Memory for a Function"""

    MEMORY_MB = re.compile(r"[\s]*mb|mi$", flags=re.IGNORECASE)
    MEMORY_GB = re.compile(r"[\s]*gb|gi$", flags=re.IGNORECASE)

    def __new__(cls, memory: Union[str, int, float]) -> None:
        if isinstance(memory, str):
            if memory.isnumeric():
                pass
            elif re.search(cls.MEMORY_MB, memory):
                memory = re.sub(cls.MEMORY_MB, "", memory)
            elif re.search(cls.MEMORY_GB, memory):
                memory = re.sub(cls.MEMORY_GB, "", memory)
                memory = int(float(memory) * 1024)
            else:
                raise ValueError(f"Unable to convert memory to megabytes: {memory}")

        return super().__new__(cls, memory)


class Function(Document):
    """The serverless cloud function that you can call directly or submit many jobs to."""

    id: str = Attribute(str, readonly=True, doc="The ID of the Function.")
    creation_date: datetime = DatetimeAttribute(
        readonly=True,
        doc="""The date the Function was created.""",
    )
    name: str = Attribute(str, doc="The name of the Function.")
    image: str = Attribute(
        str,
        mutable=False,
        doc="The base image used to create the Function.",
    )
    cpus: float = Attribute(
        Cpus,
        doc="The number of cpus to request when executing the Function.",
    )
    memory: int = Attribute(
        Memory,
        doc="The amount of memory, in megabytes, to request when executing the Function.",
    )
    maximum_concurrency: int = Attribute(
        int,
        doc="The maximum number of Jobs that execute at the same time for this Function.",
    )
    status: FunctionStatus = Attribute(
        FunctionStatus,
        readonly=True,
        doc="The status of the Function.",
    )
    timeout: int = Attribute(
        int,
        doc="The number of seconds Jobs can run before timing out for this Function.",
    )
    retry_count: int = Attribute(
        int,
        doc="The total number of retries requested for a Job before it failed.",
    )

    def __init__(
        self,
        function: Callable = None,
        requirements: List[str] = None,
        # include_data: List[str] = None,
        name: str = None,
        image: str = None,
        cpus: Cpus = None,
        memory: Memory = None,
        maximum_concurrency: int = None,
        timeout: int = None,
        retry_count: int = None,
        **extra,
    ):  # check to see if we need more validation here (type conversions)
        """
        Parameters
        ----------
        function : Callable
            The function to be called in a Compute Job.
        requirements : List[str], optional
            A list of Python dependencies required by this function.
        include_data : List[str], optional
            Non-Python data files to include in the task group.
        name : str, optional
            Name of the function, will take name of function if not provided.
        image : str
            The location of a docker image to be used for the environment where the function
            will be executed.
        cpus : Cpus
            The number of CPUs requested for a single Job.
        memory : Memory
            The maximum memory requirement for a single Job.
        maximum_concurrency : str
            The maximum number of jobs to run in parallel.
        timeout : int, optional
            Maximum runtime for a single job in seconds. Job will be killed if it exceeds
            this limit.
        retry_count : int, optional
            Number of times to retry a job if it fails.

        Examples
        --------
        Retrieving an existing function and executing it.

        >>> fn = Function.get(<function-id>)
        >>> fn()
        Job <job id>: "pending"

        Creating a new function.

        >>> from descarteslabs.compute import Function
        >>> def test_func():
        ...     print("Hello :)")
        >>> fn = Function(
        ...     test_func,
        ...     requirements=[],
        ...     name="my_func",
        ...     image="test_image",
        ...     cpus=1,
        ...     memory=16,
        ...     maximum_concurrency=5,
        ...     timeout=3600,
        ...     retry_count=1,
        ... )
        >>> fn()
        Job <job id>: "pending"
        """
        self._function = function
        self._requirements = requirements
        # self._include_data = include_data

        # if user doesn't give a name, use the name of the function
        if not name and self._function:
            name = self._function.__name__

        super().__init__(
            name=name,
            image=image,
            cpus=cpus,
            memory=memory,
            maximum_concurrency=maximum_concurrency,
            timeout=timeout,
            retry_count=retry_count,
            **extra,
        )

    def __call__(self, *args, **kwargs):
        self.save()
        job = Job(function_id=self.id, args=args, kwargs=kwargs)
        job.save()
        return job

    def _bundle(self) -> str:
        function = self._function

        if not function:
            raise ValueError("Function not provided!")

        if function.__name__ == "<lambda>":
            raise ValueError("Cannot execute lambda functions. Use `def` instead.")

        try:
            src = inspect.getsource(function)
        except Exception:
            # Unable to retrieve the source try dill
            try:
                import dill

                src = dill.source.getsource(function)
            except ImportError:
                raise ValueError(
                    "Unable to retrieve the source of interactively defined functions."
                    " To support this install dill: pip install dill"
                )

        src = src.strip()
        main = f"{src}\n\n\nmain = {function.__name__}\n"

        if self._requirements:
            requirements = "\n".join(self._requirements) + "\n"
        else:
            requirements = None

        # include_data = self._data_globs_to_paths()

        try:
            with NamedTemporaryFile(delete=False, suffix=".zip", mode="wb") as f:
                with zipfile.ZipFile(
                    f, mode="w", compression=zipfile.ZIP_DEFLATED
                ) as bundle:
                    bundle.writestr("function.py", main)

                    # TODO: include data
                    # for file_path in include_data:
                    #     bundle.write(file_path, os.path.relpath(file_path, "data"))

                    if requirements:
                        bundle.writestr("requirements.txt", requirements)

            return f.name
        except Exception:
            if os.path.exists(f.name):
                os.remove(f.name)
            raise

    def _data_globs_to_paths(self) -> List[str]:
        data_files = []

        for pattern in self._include_data:
            is_glob = glob.has_magic(pattern)
            matches = glob.glob(pattern)

            if not matches:
                if is_glob:
                    warnings.warn(f"Include data pattern had no matches: {pattern}")
                else:
                    raise ValueError(f"No data file found for path: {pattern}")

            for relative_path in matches:
                path = os.path.abspath(relative_path)

                if os.path.exists(path):
                    if os.path.isdir(path):
                        relative_path = relative_path.rstrip("/")

                        raise ValueError(
                            "Cannot accept directories as include data."
                            " Use globs instead: {} OR {}".format(
                                f"{relative_path}/*.*", f"{relative_path}/**/*.*"
                            )
                        )
                    else:
                        data_files.append(path)
                else:
                    raise ValueError(f"Data file does not exist: {path}")

        return data_files

    @classmethod
    def get(cls, id: str):
        """Get Function by id.

        Parameters
        ----------
        id : str
            Id of function to get.

        Example
        -------
        >>> from descarteslabs.compute import Function
        >>> fn = Function.get(<func_id>)
        <Function name="test_name" image=test_image cpus=1 memory=16 maximum_concurrency=5 timeout=3 retries=1
        """
        client = ComputeClient.get_default_client()
        response = client.session.get(f"/functions/{id}")
        return cls(**response.json(), saved=True)

    @classmethod
    def list(
        cls,
        status: Union[FunctionStatus, List[FunctionStatus], None] = None,
        page_size: int = 100,
    ):
        """Lists all Functions for a user.

        Parameters
        ----------
        status : FunctionStatus, List[FunctionStatus], optional
            Functions with any of the specified statuses will be included.
        page_size : int, default=100
            Maximum number of results per page.

        Example
        -------
        >>> from descarteslabs.compute import Function
        >>> fn = Function.list()
        """
        client = ComputeClient.get_default_client()
        params = {"page_size": page_size}

        if status:
            if not isinstance(status, list):
                status = [status]

            params["status"] = status

        paginator = client.iter_pages("/functions", params=params)

        for data in paginator:
            yield cls(**data, saved=True)

    @classmethod
    def update_credentials(cls):
        """Updates the credentials for the Functions and Jobs run by this user.

        These credentials are used by other Descarteslabs services.

        If the user invalidates existing credentials and needs to update them,
        you should call this method.

        Notes
        -----
        Credentials are automatically updated when a new Function is created.
        """
        client = ComputeClient.get_default_client()
        client.set_credentials()

    @property
    def jobs(self) -> Iterable[Job]:
        """Returns all the Jobs for the Function."""
        return Job.list(self.id)

    def build_log(self):
        """Retrieves the build log for the Function."""
        client = ComputeClient.get_default_client()
        response = client.session.get(f"/functions/{self.id}/log")

        print(gzip.decompress(response.content).decode())

    def delete(self):
        """Deletes the Function and all associated Jobs."""
        if self.state == DocumentState.NEW:
            raise ValueError("Cannot delete a Function that has not been saved")

        client = ComputeClient.get_default_client()

        for job in self.jobs:
            job.delete()

        client.session.delete(f"/functions/{self.id}")
        self._deleted = True

    def save(self):
        """Creates the Function if it does not already exist.

        If the Function already exists, it will be updated on the server if the Function
        instance was modified.

        Examples
        --------
        Create a Function without creating jobs:

        >>> from descarteslabs.compute import Function
        >>> def test_func():
        ...     print("Hello :)")
        >>> fn = Function(
        ...     test_func,
        ...     requirements=[],
        ...     name="my_func",
        ...     image="test_image",
        ...     cpus=1,
        ...     memory=16,
        ...     maximum_concurrency=5,
        ...     timeout=3600,
        ...     retry_count=1,
        ... )
        >>> fn.save()

        Updating a Function:

        >>> from descarteslabs.compute import Function
        >>> fn = Function.get(<func_id>)
        >>> fn.memory = 4096  # 4 Gi
        >>> fn.save()
        """

        if self.state == DocumentState.SAVED:
            # Document already exists on the server without changes locally
            return

        client = ComputeClient.get_default_client()

        if self.state == DocumentState.NEW:
            self.update_credentials()

            code_bundle_path = self._bundle()
            response = client.session.post(
                "/functions", json=self.to_dict(exclude_none=True)
            )
            response_json = response.json()
            self._load_from_remote(response_json["function"])

            # Upload the bundle to s3
            s3_client = ThirdPartyService()
            upload_url = response_json["bundle_upload_url"]
            code_bundle = io.open(code_bundle_path, "rb")
            headers = {
                "content-type": "application/octet-stream",
            }
            s3_client.session.put(upload_url, data=code_bundle, headers=headers)

            # Complete the upload with compute
            response = client.session.post(f"/functions/{self.id}/bundle")
            self._load_from_remote(response.json())
        elif self.state == DocumentState.MODIFIED:
            response = client.session.patch(
                f"/functions/{self.id}", json=self.to_dict(only_modified=True)
            )
            self._load_from_remote(response.json())
        else:
            raise ValueError(
                f"Unexpected Function state {self.state}."
                f'Reload the function from the server: Function.get("{self.id}")'
            )

        self._load_from_remote(response.json())

    def map(self, args, iterargs=None) -> List[Job]:
        """Submits multiple jobs efficiently with positional args to each function call.

        Preferred over repeatedly calling the function, such as in a loop, when submitting
        multiple jobs.

        Parameters
        ----------
        args : iterable
            An iterable of arguments. A job will be submitted with each element as the
            first positional argument to the function.
        kwargs : List[iterable], optional
            If additional iterable arguments are passed, the function must take that
            many arguments and is applied to the items from all iterables in parallel.
        """
        client = ComputeClient.get_default_client()

        # save in case the function doesn't exist yet
        self.save()

        response = client.session.post(
            "/jobs/bulk",
            json={"function_id": self.id, "bulk_args": args, "bulk_kwargs": iterargs},
        )

        return [Job(**job, saved=True) for job in response.json()]

    def rerun(self):
        """Submits all the failed and timed out jobs to be rerun."""
        client = ComputeClient.get_default_client()

        response = client.session.post("/jobs/rerun", json={"function_id": self.id})
        return [Job(**job, saved=True) for job in response.json()]

    def refresh(self):
        """Updates the Function instance with data from the server."""
        client = ComputeClient.get_default_client()

        response = client.session.get(f"/functions/{self.id}")
        self._load_from_remote(response.json())

    def iter_results(self, cast_type: Type[Serializable] = None):
        """Iterates over all successful job results."""
        # TODO: optimize by filtering on server
        for job in self.jobs:
            if job.status != JobStatus.SUCCESS:
                continue
            yield job.result(cast_type=cast_type)

    def results(self, cast_type: Type[Serializable] = None):
        """Retrieves all the job results for the Function as a list.

        Notes
        -----
        This immediately downloads all results into a list and could run out of memory.
        If the result set is large, strongly consider using :py:meth:`Function.refresh`
        instead.
        """
        return list(self.iter_results(cast_type=cast_type))

    def wait_for_completion(self, timeout=None, interval=10):
        """Waits until all submitted jobs for a given Function are completed.

        Parameters
        ----------
        timeout : int, default=None
            Maximum time to wait before timing out. If not set, this will hang until
            completion.
        interval : int, default=10
            Interval for how often to check if jobs have been completed.
        """
        print(f"Function {self.name} starting status {self.status}")
        last_status = self.status
        start_time = time.time()

        while True:
            self.refresh()

            if self.status != last_status:
                print(
                    f"Function {self.name} updated from {last_status} to {self.status}"
                )
                last_status = self.status

            if self.status in [FunctionStatus.SUCCESS, FunctionStatus.FAILURE]:
                break

            if timeout:
                t = timeout - (time.time() - start_time)
                if t <= 0:
                    raise TimeoutError(
                        f"Function {self.name} did not complete before timeout!"
                    )

                t = min(t, interval)
            else:
                t = interval

            time.sleep(t)
