# N-cube Python SDK

## Install

```shell
pip install ncube-sdk
```

## Use

```python
import os
import ncube_sdk.client

# initialize
client = ncube_sdk.client.init(
    service_url=os.environ["INGEST_URL"],
    schema_id=os.environ["INGEST_SCHEMA_ID"],
    service_auth=(os.environ["INGEST_AUTH_USER"], os.environ["INGEST_AUTH_PASS"]))

# emit events
client.emit({"field1": "value1", "field2": "value2"})
```