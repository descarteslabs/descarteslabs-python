[![CircleCI](https://circleci.com/gh/descarteslabs/descarteslabs-python/tree/master.svg?style=svg&circle-token=8db9ebe68bc456a689d0e54ad04f9adb5bd6238d)](https://circleci.com/gh/descarteslabs/descarteslabs-python/tree/master)

Descarteslabs
=============

Services
--------

This package includes service wrappers for Descartes Labs application services that 
do require additional dependencies (included in requirements.txt) and are thus not 
implicitly included in the root package (see above). Service wrappers include, 
primarily, Runcible (image metadata) and Waldo (named shapes and statistics). 
These services are authenticated and in order to setup authentication there is a 
convenience script to help you log in.

```bash
$ python setup.py install
$ pip install -r requirements.txt
$ descarteslabs login
```

For non-interactive environments, one needs to set the CLIENT_ID and CLIENT_SECRET 
environment variables. These can be retrieved from the ~/.descarteslabs/token_info.json
creatd from the login process or generated fresh through through [IAM](https://iam.descarteslabs.com).

```bash
$ export CLIENT_ID=...
$ export CLIENT_SECRET=...
```
