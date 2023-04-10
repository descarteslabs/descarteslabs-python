import glob
import inspect
import io
import json
import os
import time
import warnings
import zipfile
from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import Callable, Dict, Iterable, List, Optional, Type

from strenum import StrEnum

from ..client.services.service import ThirdPartyService
from .compute_client import ComputeClient


class State(StrEnum):
    MODIFIED = "modified"  # changed but needs to be saved
    NEW = "new"
    SAVED = "saved"  # synced with the server


class FunctionStatus(StrEnum):
    AWAITING_BUNDLE = "awaiting_bundle"
    BUILDING = "building"
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILURE = "failure"


class JobStatus(StrEnum):
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


class ComputeObject(object):
    def __init__(self):
        self._state = State.NEW

    def __setattr__(self, name, value):
        super(ComputeObject, self).__setattr__("_state", State.MODIFIED)
        super(ComputeObject, self).__setattr__(name, value)


class Job(ComputeObject):
    """A single invocation of a Function."""

    def __init__(
        self,
        function_id: str,
        args: Optional[List] = None,
        kwargs: Optional[Dict] = None,
        id: Optional[str] = None,
        creation_date: datetime = None,
        status: str = None,
        runtime: int = None,
    ):
        """
        Parameters
        ----------
        function_id : str
            Id of the Function. You can retrieve it, but this is not settable.
        args : List, optional
            A list of positional arguments to pass to the function.
        kwargs : Dict, optional
            A dictionary of named arguments to pass to the function.
        id : str, optional
            Id of a job.
        creation_date : datetime, optional
            Time of job creation. You can retrieve it, but this is not settable.
        status : JobStatus, optional
            Status of the job. You can retrieve it, but this is not settable.
        runtime : int, optional
            Property indicating the time spent executing the job in seconds.
        """
        self.id = id
        self.function_id = function_id
        if creation_date:
            creation_date = datetime.fromisoformat(creation_date)
        self.creation_date = creation_date
        self.args = args
        self.kwargs = kwargs
        self.status = status
        self.runtime = runtime
        # must be last to set State to NEW
        super().__init__()

    def __repr__(self) -> str:
        return f"Job {self.id}: {self.status}"

    def _load_from_json(self, data: dict):
        self.id = data.get("id", self.id)
        self.function_id = data.get("function_id", self.function_id)
        if "creation_date" in data:
            self.creation_date = datetime.fromisoformat(data["creation_date"])

        self.args = data.get("args", self.args)
        self.kwargs = data.get("kwargs", self.kwargs)
        self.status = data.get("status", self.status)
        self.runtime = data.get("runtime", self.runtime)
        self._state = State.SAVED

    @property
    def function(self) -> "Function":
        return Function.get(self.function_id)

    @classmethod
    def get(cls, id) -> "Job":
        """Retrieves Job from id.

        Parameters
        ----------
        id : str
            Id of Job to get.

        Example
        -------
        >>> from descarteslabs.compute import Job
        >>> job = Job.get(<job-id>)
        Job <job-id>: pending
        """
        client = ComputeClient.get_default_client()
        response = client.session.get(f"/jobs/{id}")
        return cls(**response.json(), _state=State.SAVED)

    @classmethod
    def list(cls, function_id, page_size=100) -> Iterable["Job"]:
        """Retrieves an iterable of all jobs for a given function id.

        Parameters
        ----------
        function_id : str
            Id of function to list associated jobs.
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
        paginator = client.paginate(f"/functions/{function_id}/jobs", params=params)

        for data in paginator:
            yield cls(**data, _state=State.SAVED)

    def refresh(self) -> None:
        client = ComputeClient.get_default_client()

        response = client.session.get(f"/jobs/{self.id}")
        self._load_from_json(response.json())

    def result(self, cast_type: Optional[Type[Serializable]] = None):
        """Retrieves the result of the job."""
        client = ComputeClient.get_default_client()
        response = client.session.get(f"/jobs/{self.id}/result")
        result = response.content

        if not result:
            return None

        if cast_type:
            deserialize = getattr(cast_type, "serialize", None)
            if deserialize and callable(deserialize):
                return deserialize(result)
            else:
                raise ValueError("Must implement Serializable.")
        else:
            try:
                return json.loads(result)
            except Exception:
                return result

    def wait_for_completion(self, timeout=None, interval=10):
        """Waits until Job is completed.

        Parameters
        ----------
        timeout : int, default=None
            Maximum time to wait before timing out. If not set, this will hang until
            completion.
        interval : int, default=10
            Interval for how often to check if jobs have been completed.
        """
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
        If the Job does exist, updates it.
        """
        client = ComputeClient.get_default_client()

        if self._state == State.SAVED:
            return
        elif self._state == State.MODIFIED:
            payload = {
                "args": self.args,
                "kwargs": self.kwargs,
            }
            response = client.session.patch("/jobs/{self.id}", json=payload)
        elif self._state == State.NEW:
            payload = {
                "function_id": self.function_id,
                "args": self.args,
                "kwargs": self.kwargs,
            }
            response = client.session.post("/jobs", json=client._remove_nulls(payload))
        else:
            raise ValueError(
                "Unexpected Job state {self._state}."
                f'Reload the job from the server: Job.get("{self.id}")'
            )

        self._load_from_json(response.json())
        self._state = State.SAVED

    def log(self):
        # need s3 set up, log path in job needs to be set to the bucket
        # credentials stuff
        raise NotImplementedError()


class Function(ComputeObject):
    """The serverless cloud function that you can call directly or submit many jobs to."""

    def __init__(
        self,
        function: Callable = None,
        requirements: List[str] = None,
        # include_data: List[str] = None,
        id: str = None,
        name: str = None,
        image: str = None,
        cpus: int = None,
        memory: str = None,
        maximum_concurrency: int = None,
        status: FunctionStatus = None,
        timeout: int = None,
        retry_count: int = None,
        **kwargs,
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
        id : str, optional
            This property is set by the server when the function is fetched.
            You can retrieve it, but this is not settable.
        name : str, optional
            Name of the function, will take name of function if not provided.
        image : str
            The location of a docker image to be used for the environment where the function
            will be executed.
        cpus : int
            The number of CPUs requested for a single Job.
        memory : str
            The maximum memory requirement for a single Job.
        maximum_concurrency : str
            The maximum number of jobs to run in parallel.
        status : `FunctionStatus`, optional
            Status of the function. You can retrieve it, but this is not settable.
        timeout : int, optional
            Maximum runtime for a single job in seconds. Job will be killed if it exceceds
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

        self.id = id

        # if user doesn't give a name, use the name of the function
        if not name and self._function:
            name = self._function.__name__
        self.name = name
        self.image = image
        self.cpus = cpus
        self.memory = memory
        self.maximum_concurrency = maximum_concurrency
        self.status = status
        self.timeout = timeout
        self.retry_count = retry_count
        # must be last to set State to NEW
        super().__init__()

    def __call__(self, *args, **kwargs):
        self.save()
        job = Job(function_id=self.id, args=args, kwargs=kwargs)
        job.save()
        return job

    def __repr__(self) -> str:
        return (
            f"<Function name={self.name} id={self.id} image={self.image} "
            f"cpus={self.cpus} memory={self.memory} "
            f"maximum_concurrency={self.maximum_concurrency} status={self.status} "
            f"timeout={self.timeout} retries={self.retry_count}"
        )

    def _load_from_json(self, data: dict):
        self.id = data.get("id", self.id)
        self.name = data.get("name", self.name)
        self.image = data.get("image", self.image)
        self.cpus = data.get("cpus", self.cpus)
        self.memory = data.get("memory", self.memory)
        self.maximum_concurrency = data.get(
            "maximum_concurrency", self.maximum_concurrency
        )
        self.status = data.get("status", self.status)
        self.timeout = data.get("timeout", self.timeout)
        self.retry_count = data.get("retry_count", self.retry_count)

    def _bundle(self) -> str:
        function = self._function

        if not function:
            raise ValueError("Function not provided!")

        if function.__name__ == "<lambda>":
            raise ValueError("Cannot execute lambda functions. Use `def` instead.")

        src = inspect.getsource(function).strip()
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

    def save(self):
        """Creates the Function if it does not already exist.
        If the Function does exist, updates it.

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
        client = ComputeClient.get_default_client()

        if self._state == State.NEW:
            payload = {
                "name": self.name,
                "image": self.image,
                "cpus": self.cpus,
                "memory": self.memory,
                "maximum_concurrency": self.maximum_concurrency,
                "timeout": self.timeout,
                "retry_count": self.retry_count,
            }
            code_bundle_path = self._bundle()
            response = client.session.post(
                "/functions", json=client._remove_nulls(payload)
            )
            response_json = response.json()
            self._load_from_json(response_json["function"])

            # Upload the bundle to s3
            s3_client = ThirdPartyService()
            upload_url = response_json["bundle_upload_url"]
            code_bundle = io.open(code_bundle_path, "rb")
            s3_client.session.put(upload_url, data=code_bundle)

            # Complete the upload with compute
            response = client.session.post(f"/functions/{self.id}/bundle")
            self._load_from_json(response.json())
        elif self._state == State.MODIFIED:
            response = client.session.patch(f"/functions/{self.id}", json=self.__dict__)
            self._load_from_json(response.json())
        else:
            raise ValueError(
                "Unexpected Function state {self._state}."
                f'Reload the function from the server: Function.get("{self.id}")'
            )

        self._load_from_json(response.json())
        self._state = State.SAVED

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
        return cls(**response.json(), _state=State.SAVED)

    @classmethod
    def list(cls, status=None, page_size=100):
        """Lists all Functions for a user.

        Parameters
        ----------
        status : `FunctionStatus`, optional
            Status of the Function.
        page_size : int, default=100
            Maximum number of results per page.

        Example
        -------
        >>> from descarteslabs.compute import Function
        >>> fn = Function.list()
        """
        client = ComputeClient.get_default_client()
        params = {"status": status, "page_size": page_size}
        paginator = client.paginate("/functions", params=params)

        for data in paginator:
            yield cls(**data, _state=State.SAVED)

    @property
    def jobs(self) -> Iterable[Job]:
        return Job.list(self.id)

    def map(self, args, *iterargs) -> List[Job]:
        """Submits multiple jobs efficiently with positional args to each function call.
        Preferred over repeatedly calling the function when submitting multiple jobs.

        Parameters
        ----------
        args : iterable, optional
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

        return [Job(**job) for job in response.json()]

    def rerun(self):
        """Submits all the failed and timed out jobs to be rerun."""
        client = ComputeClient.get_default_client()

        response = client.session.post("/jobs/rerun", json={"function_id": self.id})
        return [Job(**job) for job in response.json()]

    def refresh(self):
        client = ComputeClient.get_default_client()

        response = client.session.get(f"/functions/{self.id}")
        self._load_from_json(response.json())

    def iter_results(self, cast_type: Type[Serializable] = None):
        """Iterates over all successful job results."""
        # TODO: optimize by filtering on server
        for job in self.jobs:
            if job.status != JobStatus.SUCCESS:
                continue
            yield job.result(cast_type=cast_type)

    def results(self, cast_type: Type[Serializable] = None):
        """In the future, Jay wants syntactic sugar for something like this:
            Function.jobs.filter(lambda j: j.status == "SUCCESS").iter_results()
        Right now, results() just stuffs iter_results into a list. Limited to 1 MB.
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
