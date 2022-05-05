# N-cube Python SDK

## Usage

```python
import ncube_sdk.client

# initialize
client = ncube_sdk.client.init(
    service_url=ingest_service_url, schema_id=schema_id, service_auth=(username, password))

# emit events
client.emit({"field1": "value1", "field2": "value2"})
```