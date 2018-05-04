import os
import tempfile

from descarteslabs.ext.storage import Storage

storage_client = Storage()

# Create a temporary file-like object for this example. If you
# were uploading from an actual file, you'd do something like:
# with open("path/to/my/file", "r") as f:
with tempfile.TemporaryFile() as f:
    # Write some data
    f.write("hello file\n")
    f.write("another line\n")

    # Seek back to the beginning of the file
    f.seek(0, os.SEEK_SET)

    # When you pass a file-like object to .set, it will send
    # all the contents of the file up to storage. It will be
    # sent in chunks if the file is large so that you don't
    # have to read all of the data into memory
    storage_client.set("my-file", f)

v = storage_client.get("my-file")
print(v)
