# flake8: noqa
"""
===============================
Tables Example
===============================

The tables client exposes a highly scalable backend for ingesting
data and performing various types of queries, such as geospatial and
temporal, over that data. This example creates a new table, ingests some
rows, performs some basic queries, deletes some rows, shares the table
with others, and then deletes the table.

The API reference for these methods and classes is at :py:mod:`descarteslabs.tables`.
"""

import os
from tempfile import mkstemp

from descarteslabs.tables import Tables

t = Tables()

################################################
# First let's download a sample dataset in the form of a shape file so that we don't
# have to create our own.  We will use active fire data from NASA FIRMS
# (https://firms.modaps.eosdis.nasa.gov/usfs/active_fire) and assume that it was saved
# to a file called /tmp/modis_fire.zip. Example command to download:
# `wget -O /tmp/modis_fire.zip https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/shapes/zips/MODIS_C6_1_USA_contiguous_and_Hawaii_7d.zip`

import requests

url = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/shapes/zips/MODIS_C6_1_USA_contiguous_and_Hawaii_7d.zip"
r = requests.get(url)

fd, tmpf = mkstemp(suffix=".zip")
try:
    with os.fdopen(fd, mode="w+b") as f:
        f.write(r.content)

    ################################################
    # Once you have data, the next step is to create a table that supports the features in
    # this shape file.  We need a schema and an SRID, and a quick way to get that is by
    # using :meth:`Tables.inspect_dataset <descarteslabs.tables.inspect_dataset>` Note
    # that this method of deriving the schema and srid should not be relied upon without
    # manually inspecting the output due to the sheer amount of conversions possible. For
    # example, converting a ``date`` type may not translate properly so it is encumbent on
    # the user to ensure everything looks correct before creating a table.
    schema, srid = t.inspect_dataset(tmpf)
    print(schema, srid)

    ################################################
    # Now let's create our table with :meth:`Tables.create_table
    # <descarteslabs.tables.create_table>`. The table must have a primary key, which is
    # either provided on creation or created by the service. We'll create one on our own
    # and make it auto-increment.
    from uuid import uuid4

    table_name = f"fires_{uuid4().hex}"
    schema["properties"]["ID"] = "auto"
    t.create_table(table_name, srid=srid, schema=schema, primary_key="ID")

    ################################################
    # Now that we've created the table, we can start inserting rows into it. We have the
    # dataset from earlier, so let's just insert that using :meth:`Tables.insert_rows
    # <descarteslabs.tables.insert_rows>`.  Like many other asynchronous client calls in
    # the platform, a job ID is returned that can be used to poll the status of the job.
    # :meth:`Tables.wait_until_completion <descarteslabs.tables.wait_until_completion>`
    # can be used to halt execution of the program until the ingest job succeeds.
    jobid = t.upload_file(tmpf, table_name)
    t.wait_until_completion(jobid)

finally:
    try:
        os.remove(tmpf)
    except Exception:
        pass

################################################
# Once the data has been ingested, the ibis library provides an interface for us to
# lazily evalute queries and retrieve them as Geo/DataFrames.
fires = t.table(table_name)
print(fires.execute())

################################################
# There are many different types of queries that can be performed. For example, to
# select specific columns:
print(fires["ID", "LONGITUDE", "LATITUDE", "ACQ_TIME"].execute())

################################################
# A more in depth example: let's find the first row with SATELLITE == 'T' whose
# geometry intersects with the highest confidence field.  Note that you will need the
# ibis library to do this intersection, which is installed as a dependency of the
# tables client
import ibis

confidence = fires["CONFIDENCE", "geom"].execute()
confidence = confidence[confidence["CONFIDENCE"] > 90]
df_geom = fires[
    (fires.SATELLITE == "T")
    & fires.geom.intersects(ibis.literal(confidence.iloc[0].geom, type="point;4326"))
]
print(df_geom.execute())

################################################
# If we want to remove some rows from the table, we need to get a list of the primary
# keys associated with those rows. We can use the :meth:`Tables.delete_rows
# <descarteslabs.tables.delete_rows>` method to achieve this. Let's delete every row
# with < 20 confidence.
to_delete = fires[fires.CONFIDENCE < 20].ID.execute()
jobid = t.delete_rows(to_delete.to_list(), table_name)
t.wait_until_completion(jobid)

################################################
# Sharing is handled by the Discover service. Therefore, we will need the
# :class:`Discover <descarteslabs.discover>` client to do this.
from descarteslabs.discover import Discover, Organization

d = Discover()

################################################
# To share a table you own with someone as a "viewer" (i.e. read-only), use
# :meth:`Discover.discover.table.share <descarteslabs.discover.table.share>`

d.table(table_name).share(with_=Organization("descarteslabs"), as_="viewer")

################################################
# To change access to a table from viewer to editor (i.e. read/write), use
# :meth:`Discover.discover.table.replace_shares
# <descarteslabs.discover.table.replace_shares>`
d.table(table_name).replace_shares(
    user=Organization("descarteslabs"), from_role="viewer", to_role="editor"
)

################################################
# Finally, to revoke access to a table use :meth:`Discover.discover.table.revoke
# <descarteslabs.discover.table.revoke>`
d.table(table_name).revoke(from_=Organization("descarteslabs"), as_="editor")

################################################
# Once you are finished with a table, you can delete the entire thing with
# :meth:`Tables.delete_table <descarteslabs.tables.delete_table>`
t.delete_table(table_name)
