# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Server-issued request correlation and purpose-signed internal execution context.

X-Superset-Execution-Context is short-lived and purpose-separated from sessions,
JWTs and CSRF. It authenticates source/report/warmup metadata and the original
producer initiator (nullable for scheduled work). Effective identity always
comes from authenticated server state. Unsigned headers/traceparent cannot set
either actor or source. Producers send the signature only to Superset's origin;
no URLs, bodies, filters, cookies or token values enter telemetry.
"""

from __future__ import annotations

import hashlib
import os
from typing import Any
from uuid import uuid4

from flask import current_app, Flask, g, has_app_context, request
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from superset.observability.context import (
    _loaded_request_identity,
    get_context,
    restore_context,
    wire_context,
)
from superset.observability.diagnostics import record_failure
from superset.observability.events import operation
from superset.observability.runtime import enabled

EXECUTION_HEADER = "X-Superset-Execution-Context"
REQUEST_ID_HEADER = "X-Superset-Request-Id"
_SIGNED_KEYS = frozenset(
    (
        "source",
        "execution_id",
        "request_actor_user_id",
        "task_id",
        "parent_task_id",
        "report_schedule_id",
        "report_execution_id",
        "warmup_run_id",
        "chart_id",
        "dashboard_id",
        "dataset_id",
        "database_id",
        "force_refresh",
        "async_execution",
    )
)


def _serializer() -> URLSafeTimedSerializer | None:
    secret = current_app.config.get("SECRET_KEY")
    if not isinstance(secret, (str, bytes)) or not secret:
        return None
    return URLSafeTimedSerializer(
        secret,
        salt="superset-observability-internal-execution-v1",
        signer_kwargs={"digest_method": hashlib.sha256},
    )


def signed_execution_headers() -> dict[str, str]:
    """Sign allowlisted execution metadata for same-origin requests only."""
    if not has_app_context() or not enabled():
        return {}
    try:
        signer = _serializer()
        if signer is None:
            return {}
        values = wire_context()
        context = {
            key: value
            for key, value in values.items()
            if key in _SIGNED_KEYS | {"traceparent"}
        }
        context["request_actor_user_id"] = get_context().get("request_actor_user_id")
        return {
            EXECUTION_HEADER: signer.dumps({"purpose": "execution", "context": context})
        }
    except Exception:  # pylint: disable=broad-except
        return {}


def _incoming_context() -> dict[str, object]:
    value = request.headers.get(EXECUTION_HEADER, "")
    if not value or len(value) > 4096:
        return {}
    try:
        signer = _serializer()
        if signer is None:
            return {}
        payload = signer.loads(value, max_age=60)
        if (
            not isinstance(payload, dict)
            or payload.get("purpose") != "execution"
            or not isinstance(payload.get("context"), dict)
        ):
            return {}
        return {
            key: value
            for key, value in payload["context"].items()
            if key in _SIGNED_KEYS | {"traceparent"}
        }
    except (BadSignature, SignatureExpired, TypeError, ValueError):
        return {}


def _actor() -> dict[str, object]:
    """Do not force authentication merely to observe an incoming request."""
    return _loaded_request_identity()


def register_http(app: Flask) -> None:  # noqa: C901
    """Register idempotent request scopes, including teardown/error responses."""
    if app.extensions.get("superset_observability_http"):
        return
    app.extensions["superset_observability_http"] = True

    @app.before_request
    def begin_request() -> None:
        request_id = uuid4().hex
        g.observability_request_id = request_id
        g.observability_identity_pid = os.getpid()
        if not enabled():
            return
        try:
            incoming = _incoming_context()
            # Only the W3C trace context is accepted from ordinary callers, not
            # baggage/source/identity. Signed source context replaces it.
            if "traceparent" not in incoming:
                incoming["traceparent"] = request.headers.get("traceparent")
            actor = _actor()
            if "request_actor_user_id" in incoming:
                actor["request_actor_user_id"] = incoming["request_actor_user_id"]
            incoming.update(
                request_id=request_id, source=incoming.get("source", "system"), **actor
            )
            scope = restore_context(incoming)
            scope.__enter__()
            g.observability_context_scope = scope
            request_operation = operation("http.request", http_method=request.method)
            request_operation.__enter__()
            g.observability_request_operation = request_operation
        except Exception:  # pylint: disable=broad-except
            record_failure()

    @app.after_request
    def finish_response(response: Any) -> Any:
        response.headers[REQUEST_ID_HEADER] = g.get(
            "observability_request_id", uuid4().hex
        )
        request_operation = g.get("observability_request_operation")
        if request_operation is not None:
            request_operation.set_attribute("http_status_code", response.status_code)
            request_operation.set_attribute(
                "status_class", f"{response.status_code // 100}xx"
            )
            request_operation.set_attribute(
                "outcome", "error" if response.status_code >= 500 else "success"
            )
            request_operation.__exit__(None, None, None)
        return response

    @app.teardown_request
    def teardown_request(error: BaseException | None) -> None:
        request_operation = g.pop("observability_request_operation", None)
        if request_operation is not None:
            request_operation.__exit__(type(error) if error else None, error, None)
        context_scope = g.pop("observability_context_scope", None)
        if context_scope is not None:
            try:
                context_scope.__exit__(None, None, None)
            except Exception:  # pylint: disable=broad-except
                record_failure()
