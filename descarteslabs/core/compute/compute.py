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

import builtins
import dis
import glob
import gzip
import importlib
import inspect
import io
import json
import os
import re
import sys
import time
import warnings
import zipfile
from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import Callable, Dict, List, Optional, Type, Union

import pkg_resources
from strenum import StrEnum

from ..client.services.service import ThirdPartyService
from ..common.client import (
    Attribute,
    DatetimeAttribute,
    Document,
    DocumentState,
    Search,
)
from .compute_client import ComputeClient

ENTRYPOINT = "__dlentrypoint__.py"
DATA = "data"
REQUIREMENTS = "requirements.txt"


class BoundGlobalError(NameError):
    """
    Raised when a global is referenced in a function where it won't be available
    when executed remotely.
    """

    pass


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
    kwargs: Optional[Dict] = Attribute(dict, doc="The parameters provided to the Job.")
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
    def list(cls, page_size: int = 100, **params) -> Search["Job"]:
        """Retrieves an iterable of all jobs matching the given parameters.

        If you would like to filter Jobs, use :py:meth:`Job.search`.

        Parameters
        ----------
        page_size : int, default=100
            Maximum number of results per page.

        Example
        -------
        >>> from descarteslabs.compute import Job
        >>> fn = Job.list(<function_id>)
        [Job <job-id1>: pending, Job <job-id2>: pending, Job <job-id3>: pending]
        """
        params = {"page_size": page_size, **params}
        search = Job.search().param(**params)

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

    @classmethod
    def search(cls) -> Search["Job"]:
        """Creates a search for Jobs.

        The search is lazy and will be executed when the search is iterated over or
        :py:meth:`Search.collect` is called.

        Example
        -------
        >>> from descarteslabs.compute import Job, JobStatus
        >>> jobs: List[Job] = Job.search().filter(Job.status == JobStatus.SUCCESS).collect()
        Collection([Job <job-id1>: success, <job-id2>: success])
        """
        return Search(Job, ComputeClient.get_default_client(), url="/jobs")

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

    id: str = Attribute(
        str,
        filterable=True,
        readonly=True,
        sortable=True,
        doc="The ID of the Function.",
    )
    creation_date: datetime = DatetimeAttribute(
        filterable=True,
        readonly=True,
        sortable=True,
        doc="""The date the Function was created.""",
    )
    name: str = Attribute(
        str,
        filterable=True,
        sortable=True,
        doc="The name of the Function.",
    )
    image: str = Attribute(
        str,
        filterable=True,
        mutable=False,
        doc="The base image used to create the Function.",
    )
    cpus: float = Attribute(
        Cpus,
        filterable=True,
        sortable=True,
        doc="The number of cpus to request when executing the Function.",
    )
    memory: int = Attribute(
        Memory,
        filterable=True,
        sortable=True,
        doc="The amount of memory, in megabytes, to request when executing the Function.",
    )
    maximum_concurrency: int = Attribute(
        int,
        filterable=True,
        sortable=True,
        doc="The maximum number of Jobs that execute at the same time for this Function.",
    )
    status: FunctionStatus = Attribute(
        FunctionStatus,
        filterable=True,
        readonly=True,
        sortable=True,
        doc="The status of the Function.",
    )
    timeout: int = Attribute(
        int,
        filterable=True,
        sortable=True,
        doc="The number of seconds Jobs can run before timing out for this Function.",
    )
    retry_count: int = Attribute(
        int,
        filterable=True,
        sortable=True,
        doc="The total number of retries requested for a Job before it failed.",
    )
    job_statistics: Dict = Attribute(
        dict,
        readonly=True,
        doc=(
            "Statistics about the Job statuses for this Function. This attribute will only be "
            "available if includes='job.statistics' is specified in the request."
        ),
    )

    _ENTRYPOINT_TEMPLATE = "{source}\n\n\nmain = {function_name}\n"
    _IMPORT_TEMPLATE = "from {module} import {obj}"
    _SYS_PACKAGES = ".*(?:site|dist)-packages"

    def __init__(
        self,
        function: Callable = None,
        requirements: List[str] = None,
        include_data: List[str] = None,
        include_modules: List[str] = None,
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
            Non-Python data files to include in the compute function.
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
        self._include_data = include_data
        self._include_modules = include_modules

        # if name is not provided and function is a string, use the name of the function
        # if name is not provided and function is a callable, set the name to __name__
        if not name and self._function:
            if isinstance(self._function, str):
                name = self._function.split(".")[-1]
            else:
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

    def _sys_paths(self):
        """Get the system paths."""

        if not hasattr(self, "_cached_sys_paths"):
            # use longest matching path entries.
            self._cached_sys_paths = sorted(
                map(os.path.abspath, sys.path), key=len, reverse=True
            )
        return self._cached_sys_paths

    def _get_globals(self, func):
        """Get the globals for a function."""

        # Disassemble the function and capture the output
        buffer = io.StringIO()
        save_stdout = sys.stdout

        try:
            sys.stdout = buffer
            dis.dis(func)
        finally:
            sys.stdout = save_stdout

        # Search for LOAD_GLOBAL instruction and capture the var name
        search_expr = ".* LOAD_GLOBAL .*\\((.*)\\)"
        compiled_search = re.compile(search_expr)

        # Non-builtin globals are collected here
        globs = set()

        for line in buffer.getvalue().split("\n"):
            result = compiled_search.match(line)

            if result:
                name = result.group(1)

                if not hasattr(builtins, name):
                    globs.add(name)

        return sorted(globs)

    def _find_object(self, name):
        """Search for an object as specified by a fully qualified name.
        The fully qualified name must refer to an object that can be resolved
        through the module search path.

        Parameters
        ----------
        name : str
            Fully qualified name of the object to search for.
            Must refer to an object that can be resolved through the module search path.

        Returns
        -------
        object
            The object specified by the fully qualified name.
        module_path : list
            The fully qualified module path.
        object_path : list
            The fully qualified object path.
        """

        module_path = []
        object_path = []

        obj = None
        parts = name.split(".")

        for part in parts:
            error = None

            if hasattr(obj, part):
                # Could be any object (module, class, etc.)
                obj = getattr(obj, part)

                if inspect.ismodule(obj) and not object_path:
                    module_path.append(part)
                else:
                    object_path.append(part)
            else:
                # If not found, assume it's a module that must be loaded
                if object_path:
                    error = "'{}' has no attribute '{}'".format(type(obj), part)
                    raise NameError(
                        "Cannot resolve function name '{}': {}".format(name, error)
                    )
                else:
                    module_path.append(part)

                    current_module_path = ".".join(module_path)
                    try:
                        obj = importlib.import_module(current_module_path)
                    except Exception as ex:
                        traceback = sys.exc_info()[2]
                        raise NameError(
                            "Cannot resolve function name '{}', error importing module {}: {}".format(
                                name, current_module_path, ex
                            )
                        ).with_traceback(traceback)

        # When we're at the end, we should have found a valid object
        return obj, module_path, object_path

    def _bundle(self):
        """Bundle the function and its dependencies into a zip file."""

        function = self._function
        include_modules = self._include_modules
        requirements = self._requirements

        if not function:
            raise ValueError("Function not provided!")

        data_files = self._data_globs_to_paths()

        try:
            with NamedTemporaryFile(delete=False, suffix=".zip", mode="wb") as f:
                with zipfile.ZipFile(
                    f, mode="w", compression=zipfile.ZIP_DEFLATED
                ) as bundle:
                    self._write_main_function(function, bundle)
                    self._write_data_files(data_files, bundle)

                    if include_modules:
                        self._write_include_modules(self._include_modules, bundle)

                    if requirements:
                        bundle.writestr(
                            REQUIREMENTS, self._bundle_requirements(requirements)
                        )
            return f.name
        except Exception:
            if os.path.exists(f.name):
                os.remove(f.name)
            raise

    def _write_main_function(self, f, archive):
        """Write the main function to the archive."""

        is_named_function = isinstance(f, str)

        if is_named_function:
            f, module_path, function_path = self._find_object(f)

            if not callable(f):
                raise ValueError(
                    "Compute main function must be a callable: `{}`".format(f)
                )

            # Simply import the module
            source = self._IMPORT_TEMPLATE.format(
                module=".".join(module_path), obj=function_path[0]
            )
            function_name = ".".join(function_path)
        else:
            # make sure function_name is set
            function_name = f.__name__

            if not inspect.isfunction(f):
                raise ValueError(
                    "Compute main function must be user-defined function: `{}`".format(
                        f
                    )
                )

            # We can't get the code for a given lambda
            if f.__name__ == "<lambda>":
                raise ValueError(
                    "Compute main function cannot be a lambda expression: `{}`".format(
                        f
                    )
                )

            # Furthermore, the given function cannot refer to globals
            bound_globals = self._get_globals(f)

            if bound_globals:
                raise BoundGlobalError(
                    "Illegal reference to one or more global variables in your "
                    "function: {}".format(bound_globals)
                )

            try:
                source = inspect.getsource(f).strip()
            except Exception:
                try:
                    import dill

                    source = dill.source.getsource(f).strip()
                except ImportError:
                    raise ValueError(
                        "Unable to retrieve the source of interactively defined functions."
                        " To support this install dill: pip install dill"
                    )

        entrypoint_source = self._ENTRYPOINT_TEMPLATE.format(
            source=source, function_name=function_name
        )
        archive.writestr(ENTRYPOINT, entrypoint_source)

    def _write_data_files(self, data_files, archive):
        """Write the data files to the archive."""

        for path, archive_path in data_files:
            archive.write(path, archive_path)

    def _find_module_file(self, mod_name):
        """Search for module file in python path. Raise ImportError if not found."""

        try:
            mod = importlib.import_module(mod_name)
            mod_file = mod.__file__.replace(".pyc", ".py", 1)
            return mod_file

        except ImportError as ie:
            # Search for possible pyx file
            mod_basename = "{}.pyx".format(mod_name.replace(".", "/"))
            for s in sys.path:
                mod_file_option = os.path.join(s, mod_basename)
                if os.path.isfile(mod_file_option):
                    # Check that found cython source not in CWD (causes build problems)
                    if os.getcwd() == os.path.dirname(os.path.abspath(mod_file_option)):
                        raise ValueError(
                            "Cannot include cython modules from working directory: `{}`.".format(
                                mod_file_option
                            )
                        )
                    else:
                        return mod_file_option

            # Raise caught ImportError if we still haven't found the module
            raise ie

    def _write_include_modules(self, include_modules, archive):
        """Write the included modules to the archive."""

        for mod_name in include_modules:
            mod_file = self._find_module_file(mod_name)

            # detect system packages from distribution or virtualenv locations.
            if re.match(self._SYS_PACKAGES, mod_file) is not None:
                raise ValueError(
                    "Cannot include system modules: `{}`.".format(mod_file)
                )

            if not os.path.exists(mod_file):
                raise IOError(
                    "Source code for module is missing, only byte code exists: `{}`.".format(
                        mod_name
                    )
                )
            sys_path = self._sys_path_prefix(mod_file)

            self._include_init_files(os.path.dirname(mod_file), archive, sys_path)
            archive_names = archive.namelist()
            # this is a package, get all decendants if they exist.
            if os.path.basename(mod_file) == "__init__.py":
                for dirpath, dirnames, filenames in os.walk(os.path.dirname(mod_file)):
                    for file_ in [f for f in filenames if f.endswith((".py", ".pyx"))]:
                        path = os.path.join(dirpath, file_)
                        arcname = self._archive_path(path, None, sys_path)
                        if arcname not in archive_names:
                            archive.write(path, arcname=arcname)
            else:
                archive.write(
                    mod_file, arcname=self._archive_path(mod_file, None, sys_path)
                )

    def _include_init_files(self, dir_path, archive, sys_path):
        """Include __init__.py files for all parent directories."""

        relative_dir_path = os.path.relpath(dir_path, sys_path)
        archive_names = archive.namelist()
        # have we walked this path before?
        if os.path.join(relative_dir_path, "__init__.py") not in archive_names:
            partial_path = ""
            for path_part in relative_dir_path.split(os.sep):
                partial_path = os.path.join(partial_path, path_part)
                rel_init_location = os.path.join(partial_path, "__init__.py")
                abs_init_location = os.path.join(sys_path, rel_init_location)
                if not os.path.exists(abs_init_location):
                    raise IOError(
                        "Source code for module is missing: `{}`.".format(
                            abs_init_location
                        )
                    )
                if rel_init_location not in archive_names:
                    archive.write(abs_init_location, arcname=rel_init_location)

    def _bundle_requirements(self, requirements):
        """Bundle the requirements into the archive."""

        if not pkg_resources:
            warnings.warn(
                "Your Python does not have a recent version of `setuptools`. "
                "For a better experience update your environment by running `pip install -U setuptools`."
            )
        if isinstance(requirements, str):
            return self._requirements_file(requirements)
        else:
            return self._requirements_list(requirements)

    def _requirements_file(self, requirements):
        """Read the requirements file and validate it."""

        if not os.path.isfile(requirements):
            raise ValueError(
                "Requirements file at {} not found. Did you mean to specify a single requirement? "
                "Pass it wrapped in a list.".format(requirements)
            )
        with open(requirements) as f:
            requirements_string = f.read()
        if pkg_resources:
            try:
                list(pkg_resources.parse_requirements(requirements_string))
            except ValueError as ex:
                raise ValueError(
                    "Invalid Python requirement in file: {}".format(str(ex))
                )
        return requirements_string

    def _requirements_list(self, requirements):
        """Validate the requirements list."""

        if pkg_resources:
            bad_requirements = []
            for requirement in requirements:
                try:
                    pkg_resources.Requirement.parse(requirement)
                except ValueError:
                    bad_requirements.append(requirement)
            if bad_requirements:
                raise ValueError(
                    "Invalid Python requirements: {}".format(",".join(bad_requirements))
                )
        return "\n".join(requirements)

    def _sys_path_prefix(self, path):
        """Get the system path prefix for a given path."""

        absolute_path = os.path.abspath(path)
        for sys_path in self._sys_paths():
            if absolute_path.startswith(sys_path):
                return sys_path
        else:
            raise IOError("Location is not on system path: `{}`".format(path))

    def _archive_path(self, path, archive_prefix, sys_path):
        """Get the archive path for a given path."""

        if archive_prefix:
            return os.path.join(archive_prefix, os.path.relpath(path, sys_path))
        else:
            return os.path.relpath(path, sys_path)

    def _data_globs_to_paths(self) -> List[str]:
        """Convert data globs to absolute paths."""

        data_files = []

        # if there are no data files, return empty list
        if not self._include_data:
            return data_files

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
                        data_files.append(
                            (
                                path,
                                self._archive_path(
                                    path, None, sys_path=self._sys_path_prefix(path)
                                ),
                            ),
                        )
                else:
                    raise ValueError(f"Data file does not exist: {path}")

        return data_files

    @classmethod
    def get(cls, id: str, **params):
        """Get Function by id.

        Parameters
        ----------
        id : str
            Id of function to get.
        include : List[str], optional
            List of additional attributes to include in the response.
            Allowed values are:

            - "job.statistics": Include statistics about the Job statuses for this Function.

        Example
        -------
        >>> from descarteslabs.compute import Function
        >>> fn = Function.get(<func_id>)
        <Function name="test_name" image=test_image cpus=1 memory=16 maximum_concurrency=5 timeout=3 retries=1
        """
        client = ComputeClient.get_default_client()
        response = client.session.get(f"/functions/{id}", params=params)
        return cls(**response.json(), saved=True)

    @classmethod
    def list(cls, page_size: int = 100, **params) -> Search["Function"]:
        """Lists all Functions for a user.

        If you would like to filter Functions, use :py:meth:`Function.search`.

        Parameters
        ----------
        page_size : int, default=100
            Maximum number of results per page.
        include : List[str], optional
            List of additional attributes to include in the response.
            Allowed values are:

            - "job.statistics": Include statistics about the Job statuses for this Function.

        Example
        -------
        >>> from descarteslabs.compute import Function
        >>> fn = Function.list()
        """
        params = {"page_size": page_size, **params}
        return cls.search().param(**params)

    @classmethod
    def search(cls) -> Search["Function"]:
        """Creates a search for Functions.

        The search is lazy and will be executed when the search is iterated over or
        :py:meth:`Search.collect` is called.

        Example
        -------
        >>> from descarteslabs.compute import Function, FunctionStatus
        >>> fns: List[Function] = (
        ...     Function.search()
        ...     .filter(Function.status.in_([
        ...         FunctionStatus.BUILDING, FunctionStatus.AWAITING_BUNDLE
        ...     ])
        ...     .collect()
        ... )
        Collection([Function <fn-id1>: building, Function <fn-id2>: awaiting_bundle])
        """
        return Search(Function, ComputeClient.get_default_client(), url="/functions")

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
    def jobs(self) -> Search[Job]:
        """Returns all the Jobs for the Function."""
        return Job.search().filter(Job.function_id == self.id)

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

        if self.job_statistics:
            params = {"include": ["job.statistics"]}
        else:
            params = {}

        response = client.session.get(f"/functions/{self.id}", params=params)
        self._load_from_remote(response.json())

    def iter_results(self, cast_type: Type[Serializable] = None):
        """Iterates over all successful job results."""
        for job in self.jobs.filter(Job.status == JobStatus.SUCCESS):
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
