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
from __future__ import absolute_import, unicode_literals

import logging

import requests
import requests.adapters
from urllib3.util.retry import Retry

__all__ = ["raise_if_err", "create_session"]

_logger = logging.getLogger(__name__)


def raise_if_err(r):
    # type: (requests.Response) -> None
    if not r.ok:
        # add a log that includes request content and other info, because the requests built-in
        # exception to string does not contain enough useful information to determine what went
        # wrong just from the exception logs
        _logger.warning(
            "Request failed %s %s failed, status=%s, headers=%s content=%s",
            r.request.method,
            r.request.url,
            r.status_code,
            r.headers,
            r.content,
        )
        r.raise_for_status()


class HTTPAdapterWithTimeout(requests.adapters.HTTPAdapter):
    def __init__(
        self,
        pool_connections=requests.adapters.DEFAULT_POOLSIZE,
        pool_maxsize=requests.adapters.DEFAULT_POOLSIZE,
        max_retries=requests.adapters.DEFAULT_RETRIES,
        pool_block=requests.adapters.DEFAULT_POOLBLOCK,
        timeout=None,
    ):
        self.timeout = timeout
        # has to be set before super __init__, since that
        # one calls the init_poolmanager() where we use the self.timeout
        super(HTTPAdapterWithTimeout, self).__init__(
            pool_connections, pool_maxsize, max_retries, pool_block
        )

    def init_poolmanager(
        self,
        connections,
        maxsize,
        block=requests.adapters.DEFAULT_POOLBLOCK,
        **pool_kwargs
    ):
        if "timeout" not in pool_kwargs:
            pool_kwargs["timeout"] = self.timeout
        # the poolmanager setting is actually ignored because the adapter's
        # send() always sets the timeout to None. we're also overriding the
        # send() to respect the timeout
        # https://github.com/requests/requests/issues/2011#issuecomment-64440818
        return super(HTTPAdapterWithTimeout, self).init_poolmanager(
            connections, maxsize, block, **pool_kwargs
        )

    def send(
        self, request, stream=False, timeout=None, verify=True, cert=None, proxies=None
    ):
        if timeout is None:
            timeout = self.timeout
        return super(HTTPAdapterWithTimeout, self).send(
            request, stream, timeout, verify, cert, proxies
        )


def create_session(
    num_workers=1,
    num_hosts=1,
    retry=10,
    backoff_factor=0.2,
    timeout=5 * 60,
    retry_status_codes=(413, 429, 500, 502, 503, 504),
    retry_allowed_methods=Retry.DEFAULT_METHOD_WHITELIST,  # pass a falsy value to retry on all methods
    respect_retry_after_header=True,
):
    # type: (...) -> requests.Session
    session = requests.Session()
    adapter = HTTPAdapterWithTimeout(
        # number of different hosts we will be talking to
        pool_connections=num_hosts,
        # number of maximum concurrent requests the session will be doing (i.e. with multiple threads that share it)
        pool_maxsize=num_workers,
        max_retries=Retry(
            total=retry,
            read=retry,
            connect=retry,
            status=retry,
            backoff_factor=backoff_factor,
            status_forcelist=retry_status_codes,
            respect_retry_after_header=respect_retry_after_header,
            method_whitelist=retry_allowed_methods,
        ),
        timeout=timeout,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
