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

import json
import logging
import threading
from collections import defaultdict
from time import time

from typing import Dict, Tuple

try:
    from queue import Empty, SimpleQueue as Queue
except ImportError:
    from Queue import Empty, Queue

from .http import create_session, raise_if_err

__all__ = ["init", "Client"]

_logger = logging.getLogger(__name__)

NDJSON_MEDIA_TYPE = "application/x-ndjson"
_work_done = object()


def _get_main_thread():
    # type: () -> threading.Thread
    try:
        return threading.main_thread()
    except AttributeError:
        for t in threading.enumerate():
            if isinstance(t, threading._MainThread):
                return t


def _batch_worker_loop(
    q,
    ingest_service_url,
    ingest_service_auth,
    batch_size,
    batch_interval_seconds,
    http_retries,
    http_retry_backoff,
    thread_running_event,
):
    # type: (Queue, str, Tuple[str, str], int, int, int, float, threading.Event) -> None
    # the thread receives payloads and sends them over
    # how should it send over?
    # version 1.
    #   accumulate into a batch, send to batch HTTP api
    # version 2.
    #   stream over the websocket

    # how to stop the thread gracefully?
    # version 1: make daemonic
    # version 2: listen on main thread and signal bg thread somehow
    # how to signal bg thread? add a TERMINATION item to its queue

    # XXX: configuration setting to crash if exception
    should_exit = False
    try:
        started = thread_running_event.wait(60)
        if not started:
            raise Exception(
                "Timed-out waiting for signal to start the background worker thread"
            )
        s = create_session(
            retry=http_retries,
            backoff_factor=http_retry_backoff,
            retry_allowed_methods=False,
        )
        s.auth = ingest_service_auth

        flush_count = 0
        item_count = 0
        batch = []
        batch_start = time()

        while True:
            timeout = batch_start + batch_interval_seconds - time()
            if timeout <= 0:
                batch_done = True
            else:
                try:
                    queue_item = q.get(timeout=timeout)
                except Empty:
                    batch_done = True
                else:
                    if queue_item is _work_done:
                        _logger.debug(
                            "Background worker thread received signal to stop, sending remaining items..."
                        )
                        batch_done = True
                        should_exit = True
                    else:
                        schema_id, data = queue_item
                        batch.append((schema_id, data))
                        batch_done = len(batch) >= batch_size

            if batch_done:
                if len(batch) > 0:
                    schema_items = defaultdict(bytes)
                    for schema_id, data in batch:
                        schema_items[schema_id] += data + b"\n"
                    for schema_id, schema_data in schema_items.items():
                        r = s.post(
                            ingest_service_url + "/" + schema_id,
                            headers={"Content-Type": NDJSON_MEDIA_TYPE},
                            data=schema_data,
                        )
                        raise_if_err(r)
                    flush_count += 1
                    item_count += len(batch)
                    if flush_count % 100 == 0:
                        _logger.debug(
                            "Sent %s items in %s batches so far.",
                            item_count,
                            flush_count,
                        )
                batch = []
                batch_start = time()
            if should_exit:
                break
    except Exception as ex:
        _logger.exception("Background worker thread failed with error %s", ex)
        thread_running_event.clear()
    else:
        _logger.debug("Background worker thread stopped.")


class _Worker(object):
    def __init__(
        self,
        ingest_service_url,
        ingest_service_auth,
        batch_size,
        batch_interval_seconds,
        max_restarts,
        http_retries,
        http_retry_backoff,
    ):
        # type: (str, Tuple[str, str], int, int, int, int, float) -> _Worker
        self._service_url = ingest_service_url
        self._service_auth = ingest_service_auth
        self._q = Queue()
        self._start_lock = threading.Lock()
        self._thread_running = threading.Event()
        self._thread_starts = 0
        self._batch_size = batch_size
        self._batch_interval_seconds = batch_interval_seconds
        self._max_restarts = max_restarts
        self._http_retries = http_retries
        self._http_retry_backoff = http_retry_backoff
        self._send_work_done_on_exit()

    def _send_work_done_on_exit(self):
        main_thread = _get_main_thread()

        def send_work_done():
            main_thread.join()
            self._q.put(_work_done)
            _logger.debug("Signalled background worker thread to stop.")

        on_exit_thread = threading.Thread(
            target=send_work_done,
            name="ncube-sdk-python-exit-signaller",
        )
        on_exit_thread.daemon = True
        on_exit_thread.start()

    def send(self, data, schema_id):
        # type: (bytes, str) -> bool
        # send to background thread queue
        # make queue unlimited size?
        # version 1. make queue unlimited size
        # version 2: fixed size. configurable size from memory_buffer_size
        # if the queue is full, decide what to do:
        #   drop items
        #   raise exception/return error code
        # how can the queue become full?
        #    version 1. if events cant be sent to ingest service
        #    version 2. if events cannot be written to disk
        #    version 3. if events cannot be written to disk and azure blob
        # if the queue is getting close to full, start logging warnings
        # return a future that is resolved when ? any of the above is true? with an enum specifying
        # the manner it was resolved with
        if not self.ensure_thread_running():
            return False
        self._q.put((schema_id, data))
        return True

    def ensure_thread_running(self):
        # type: () -> bool
        if self._thread_running.is_set():
            return True
        if self._thread_starts > self._max_restarts:
            _logger.debug(
                "Thread has restarted %s times, will not attempt to restart.",
                self._max_restarts,
            )
            return False
        with self._start_lock:
            if self._thread_running.is_set():
                return True
            t = threading.Thread(
                target=_batch_worker_loop,
                name="ncube-sdk-python-background-worker",
                args=(
                    self._q,
                    self._service_url,
                    self._service_auth,
                    self._batch_size,
                    self._batch_interval_seconds,
                    self._http_retries,
                    self._http_retry_backoff,
                    self._thread_running,
                ),
            )
            t.start()
            self._thread_running.set()
            self._thread_starts += 1
            _logger.debug(
                "Started background worker thread. Restarts: %s",
                self._thread_starts - 1,
            )
            return True


class Client:
    def __init__(
        self,
        ingest_service_url,
        schema_id,
        ingest_service_auth,
        batch_size,
        batch_interval_seconds,
        raise_on_emit_failure,
        validate_before_emit,
        raise_on_validate_failure,
        jsonschemas,
        fetch_schemas,
        schema_service_url,
        schema_service_auth,
        max_background_worker_restarts,
        http_retries,
        http_retry_backoff,
    ):
        # type: (str, str, Tuple[str, str], int, int, bool, bool, bool, Dict[str, Dict], bool, str, Tuple[str,str], int, int, float) -> Client
        self._schema_id = schema_id
        self._raise_on_emit_failure = raise_on_emit_failure
        if validate_before_emit:
            try:
                import jsonschema
            except ImportError:
                import warnings

                warnings.warn(
                    "validate_before_emit cannot be set because jsonschema dependency is missing, "
                    "install the package with option ncube_sdk[jsonschema] to enable"
                )
                validate_before_emit = False
                self.validator = None
            else:
                from .validate import Validator

                self.validator = Validator(
                    raise_on_validate_failure=raise_on_validate_failure,
                    jsonschemas=jsonschemas,
                    fetch_schemas=fetch_schemas,
                    schema_service_url=schema_service_url,
                    schema_service_auth=schema_service_auth,
                )
        self._validate_before_emit = validate_before_emit
        self._worker = _Worker(
            ingest_service_url,
            ingest_service_auth,
            batch_size,
            batch_interval_seconds,
            max_restarts=0 if raise_on_emit_failure else max_background_worker_restarts,
            http_retries=http_retries,
            http_retry_backoff=http_retry_backoff,
        )

    def _validate(self, payload, schema_id=None):
        # type: (dict, str) -> None
        schema_id = schema_id or self._schema_id
        self.validator.validate(payload, schema_id)

    def emit(self, payload, schema_id=None):
        # type: (dict, str) -> bool
        if self._validate_before_emit:
            self._validate(payload, schema_id)
        return self.emitb(
            json.dumps(payload, allow_nan=False).encode("utf-8"), schema_id
        )

    def emitb(self, payload, schema_id=None):
        # type: (bytes, str) -> bool
        schema_id = schema_id or self._schema_id
        if schema_id is None:
            raise Exception(
                "You need to either pass a schema_id or initialize the client with one"
            )
        sent = self._worker.send(payload, schema_id)
        if not sent:
            err_msg = (
                "Cannot emit, the background worker thread is in a disfunctional state. Please check "
                "previous error logs."
            )
            if self._raise_on_emit_failure:
                raise Exception(err_msg)
            else:
                _logger.error(
                    "Cannot emit, the background worker thread is in a disfunctional state. Please check "
                    "previous error logs."
                )
        return sent


def init(
    ingest_service_url,
    schema_id,
    ingest_service_auth=None,
    batch_size=100,
    batch_interval_seconds=5,
    raise_on_emit_failure=False,
    validate_before_emit=False,
    raise_on_validate_failure=True,
    jsonschemas=None,
    fetch_schemas=False,
    schema_service_url=None,
    schema_service_auth=None,
    max_background_worker_restarts=0,
    http_retries=10,
    http_retry_backoff=0.3,
):
    # type: (str, str, Tuple[str, str], int, int, bool, bool, bool, Dict[str, Dict], bool, str, Tuple[str, str], int, int, float) -> Client
    return Client(
        ingest_service_url=ingest_service_url,
        schema_id=schema_id,
        ingest_service_auth=ingest_service_auth,
        batch_size=batch_size,
        batch_interval_seconds=batch_interval_seconds,
        raise_on_emit_failure=raise_on_emit_failure,
        validate_before_emit=validate_before_emit,
        raise_on_validate_failure=raise_on_validate_failure,
        jsonschemas=jsonschemas,
        fetch_schemas=fetch_schemas,
        schema_service_url=schema_service_url,
        schema_service_auth=schema_service_auth,
        max_background_worker_restarts=max_background_worker_restarts,
        http_retries=http_retries,
        http_retry_backoff=http_retry_backoff,
    )
