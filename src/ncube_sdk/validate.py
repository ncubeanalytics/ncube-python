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

from jsonschema import validate
from typing import Any, Dict, Text, Tuple

from .http import create_session, raise_if_err

_logger = logging.getLogger(__name__)


def _log_or_raise(raise_, err_template, *err_args):
    # type: (bool, Text, *Any) -> None
    if raise_:
        raise Exception(err_template % err_args)
    else:
        _logger.error(err_template, *err_args)


class Fetcher(object):
    def __init__(
        self,
        schema_service_url,
        schema_service_auth,
    ):
        # type: (str, Tuple[str, str]) -> None
        self.schema_service_url = schema_service_url
        self.session = create_session(
            retry=3,
            timeout=10,
            retry_allowed_methods=False,
        )
        self.session.auth = schema_service_auth

    def fetch_jsonschema(self, schema_id):
        # type: (str) -> Dict
        r = self.session.get(
            "{}/schema/{}?draft=false&include_jsonschema&include_revisions".format(
                self.schema_service_url, schema_id
            )
        )
        raise_if_err(r)
        schema = r.json()["item"]
        max_revision = 0
        jsonschema = None
        for revision in schema["revisions"]:
            if revision["revision"] > max_revision:
                max_revision = revision["revision"]
                jsonschema = revision["jsonschema"]
        return jsonschema


class Validator(object):
    def __init__(
        self,
        raise_on_validate_failure,
        jsonschemas,
        fetch_schemas,
        schema_service_url,
        schema_service_auth,
    ):
        # type: (bool, Dict[str, Dict], bool, str, Tuple[str, str]) -> None
        self.raise_on_validate_failure = raise_on_validate_failure
        if jsonschemas is None:
            jsonschemas = {}
        self.jsonschemas = jsonschemas
        self.fetch_schemas = fetch_schemas
        if fetch_schemas:
            if schema_service_url is None:
                raise Exception("schema_service_url is missing")
            self.fetcher = Fetcher(schema_service_url, schema_service_auth)

    def validate(self, payload, schema_id):
        # type: (dict, str) -> None
        if schema_id not in self.jsonschemas:
            if self.fetch_schemas:
                try:
                    jsonschema_ = self.fetcher.fetch_jsonschema(schema_id)
                except Exception as ex:
                    _log_or_raise(
                        self.raise_on_validate_failure,
                        "Failed to retrieve jsonschema for %s with error: %s",
                        schema_id,
                        ex,
                    )
                    return
                else:
                    self.jsonschemas[schema_id] = jsonschema_
            else:
                _log_or_raise(
                    self.raise_on_validate_failure,
                    "Failed to validate schema %s, no jsonschema is provided",
                    schema_id,
                )
        jsonschema_ = self.jsonschemas[schema_id]

        try:
            validate(payload, jsonschema_)
        except Exception as ex:
            _log_or_raise(
                self.raise_on_validate_failure,
                "Failed to validate payload with error: %s",
                ex,
            )
