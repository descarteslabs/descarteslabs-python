.. _quickstart:

Quickstart
==========

Eager to get started? Getting started with our platform is easy, but requires a few steps.

Installation
------------
Install the latest client library via :command:`pip`::

    $ pip install descarteslabs

.. note::

   The latest development version can always be found on
   `GitHub <https://github.com/descarteslabs/descarteslabs-python>`_.

Configuration
-------------
Before you can begin using the services provided by the platform, you need to set up authentication credentials. The
easiest way is to use the CLI helper::

    $ descarteslabs login

For non-interactive environments, one needs to set the CLIENT_ID and CLIENT_SECRET 
environment variables. These can be retrieved from the ~/.descarteslabs/token_info.json
created from the login process or generated fresh through through [IAM](https://iam.descarteslabs.com).

::

    $ export CLIENT_ID=...
    $ export CLIENT_SECRET=...

Using the services
------------------
After the credentials are correctly configured, you should be able to easily access the services available to you::

    >>> import descarteslabs as dl
    >>> dl.places.find('illinois')
    [{'name': 'Illinois', 'placetype': 'region', ...}]