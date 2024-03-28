#  Copyright (c) 2022 N Cube Single Member Private Company
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging
import os

import ncube_sdk.client


def main():
    logging.basicConfig(level=logging.DEBUG)
    logging.debug("Starting!")
    ingest_service_url = os.environ["INGEST_SERVICE_URL"]
    ingest_service_auth = (
        os.environ.get("INGEST_SERVICE_AUTH_USER"),
        os.environ.get("INGEST_SERVICE_AUTH_PASS"),
    )
    ingest_service_verify_ssl = "INGEST_SERVICE_DONT_VERIFY_SSL" not in os.environ
    if ingest_service_auth[0] is None:
        ingest_service_auth = None
    schema_id = os.environ["SCHEMA_ID"]
    schema_service_url = os.environ.get("SCHEMA_SERVICE_URL")
    validate_before_emit = fetch_schemas = bool(schema_service_url)
    schema_service_auth = (
        os.environ.get("SCHEMA_SERVICE_AUTH_USER"),
        os.environ.get("SCHEMA_SERVICE_AUTH_PASS"),
    )
    if schema_service_auth[0] is None:
        schema_service_auth = None
    raise_on_emit_failure = "RAISE_ON_EMIT_FAILURE" in os.environ
    raise_on_validate_failure = "RAISE_ON_VALIDATE_FAILURE" in os.environ

    client = ncube_sdk.client.init(
        ingest_service_url=ingest_service_url,
        schema_id=schema_id,
        ingest_service_auth=ingest_service_auth,
        raise_on_emit_failure=raise_on_emit_failure,
        validate_before_emit=validate_before_emit,
        raise_on_validate_failure=raise_on_validate_failure,
        ingest_service_verify_ssl=ingest_service_verify_ssl,
        fetch_schemas=fetch_schemas,
        schema_service_url=schema_service_url,
        schema_service_auth=schema_service_auth,
        schema_service_verify_ssl=ingest_service_verify_ssl,
        http_retries=5,
    )

    import time

    cnt = 0
    while True:
        cnt += 1
        client.emit({"num_field": str(cnt)})
        print(cnt)
        time.sleep(1)


if __name__ == "__main__":
    main()
