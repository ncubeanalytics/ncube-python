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
    url = os.environ["SERVICE_URL"]
    auth = (
        os.environ["SERVICE_AUTH_USER"],
        os.environ["SERVICE_AUTH_PASS"],
    )
    schema_id = os.environ["SCHEMA_ID"]
    client = ncube_sdk.client.init(
        url,
        schema_id,
        service_auth=auth,
        http_retries=5,
    )

    import time

    cnt = 0
    while True:
        cnt += 1
        client.emit({"cnt": cnt})
        print(cnt)
        time.sleep(1)


if __name__ == "__main__":
    main()
