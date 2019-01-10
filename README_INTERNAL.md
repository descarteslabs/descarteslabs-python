Python client internal README
=============================

This is internal documentation that is not included with a public release.


Releasing the Python client
---------------------------

- Update the changelog in README.md in this directory with the new version
  and make sure the changelog is accurate
- Update the version in descarteslabs/client/version.py
- Submit and merge a PR with the above
- Wait for drone (https://drone.descarteslabs.com/descarteslabs/monorepo/)
  to run the build after merging into monorepo master -
  copybara will mirror the changes to the public repo at
  https://github.com/descarteslabs/descarteslabs-python
- Create a release in the public repo with a tag corresponding to your
  version, i.e., "vX.Y.Z"
- Publish the package at this tag to pypi - at the moment only Justin
  and Sam can do that
- Update the version in images/tasks/public/py\*/requirements.txt.default
- Build the images
- Retrieve the versions of installed python packages with `pip freeze`
  and update docs/guides/tasks.rst


Running tests on Python 3
-------------------------

Currently we are only able to run client tests in the monorepo through bazel with Python 2. Until we have a broader fix for that, it's possible to run the client tests on Python 3 like this:

- Create a virtualenv with your desired Python 3 version and activate it (e.g., `virtualenv py3env -p path/to/python3`)
- Install requirements in the virtualenv: `pip install -r descarteslabs/client/packaging/requirements.txt`
- Install nose in the virtualenv: `pip install nose`
- Run the test with `nosetests descarteslabs/client/`

This may not work for all tests for various reasons but it's a start.

Note that Travis CI runs tests on Python 3 through the public repo: https://travis-ci.org/descarteslabs/descarteslabs-python