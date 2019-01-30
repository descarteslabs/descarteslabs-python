from descarteslabs.client.services.storage import Storage

storage_client = Storage()

storage_client.set("my-key", "some-value")
v = storage_client.get("my-key")

assert v == "some-value"

key_list = storage_client.list()
assert "my-key" in key_list

for key in storage_client.iter_list():
    if "my-key" == key:
        print("Found it!")
        break
else:
    raise RuntimeError("Failed to find it!")
