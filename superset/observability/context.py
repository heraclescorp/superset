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
"""An explicit, payload-free telemetry schema, separate from query/cache identity.

Only primitive allowlisted metadata crosses the request/task boundary. No SQL,
filters, arbitrary mappings, exception messages, URLs, credentials or result data
are accepted. Identifier lists are deliberately limited to asset relationships.
"""

from __future__ import annotations

import hashlib
import hmac
import math
import os
import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from types import MappingProxyType
from typing import TypeAlias

from superset.observability.diagnostics import record_failure

TelemetryValue: TypeAlias = str | int | float | bool | None
SafeValue: TypeAlias = TelemetryValue | tuple[int | str, ...]

_TOKEN = re.compile(r"[A-Za-z0-9_:.+-]{1,160}\Z", re.ASCII)
_WAREHOUSE = re.compile(r"[A-Za-z0-9_ .()+-]{1,128}\Z", re.ASCII)
_NAME = re.compile(r"[a-z][a-z0-9_.-]{0,95}\Z", re.ASCII)
_TRACEPARENT = re.compile(r"00-([0-9a-f]{32})-([0-9a-f]{16})-0[01]\Z")
_SOURCES = frozenset(
    {
        "chart",
        "sql_lab",
        "report",
        "warmup",
        "export",
        "metadata",
        "system",
        "browser",
        "observability",
    }
)
_ID_KEYS = frozenset(
    (
        "request_id",
        "execution_id",
        "request_actor_user_id",
        "effective_user_id",
        "guest_actor_id",
        "chart_id",
        "dashboard_id",
        "dataset_id",
        "database_id",
        "sql_lab_query_id",
        "sql_lab_client_id",
        "task_id",
        "celery_task_id",
        "report_schedule_id",
        "report_execution_id",
        "warmup_run_id",
        "statement_id",
        "snowflake_query_id",
        "snowflake_session_id",
        "parent_task_id",
        "browser_event_id",
        "browser_view_id",
        "browser_load_id",
        "view_id",
        "client_event_id",
        "event_id",
        "sql_fingerprint",
    )
)
_TOKEN_KEYS = frozenset(
    (
        "operation",
        "outcome",
        "error_type",
        "error_code",
        "error_category",
        "warehouse_name",
        "db_system",
        "phase",
        "worker_pool",
        "snapshot_kind",
        "queue_name",
        "state",
        "dependency_state",
        "task_status",
        "task_type",
        "celery_task_name",
        "consumer_action",
        "browser_event",
        "browser_event_name",
        "trust",
        "role",
        "cache_backend",
        "cache_type",
        "cache_layer",
        "statement_type",
        "task_origin",
        "strategy",
        "status",
        "http_method",
        "status_class",
        "datasource_type",
    )
)
_NUMBER_KEYS = frozenset(
    (
        "statement_index",
        "query_index",
        "retry_count",
        "rows",
        "result_bytes",
        "duration_seconds",
        "active_duration_seconds",
        "scheduled_at",
        "submitted_at",
        "published_at",
        "received_at",
        "started_at",
        "ended_at",
        "finished_at",
        "client_timestamp",
        "expected_charts",
        "completed_charts",
        "dependency_waiting",
        "waiting_count",
        "queued_count",
        "running_count",
        "oldest_age_seconds",
        "task_count",
        "cache_count",
        "http_status_code",
        "elapsed_seconds",
    )
)
_TIME_KEYS = frozenset(
    (
        "scheduled_at",
        "submitted_at",
        "published_at",
        "received_at",
        "started_at",
        "ended_at",
        "finished_at",
        "client_timestamp",
    )
)
_BOOL_KEYS = frozenset(
    (
        "cache_hit",
        "async_execution",
        "force_refresh",
        "force_cancel",
        "abandoned",
        "joined",
        "created",
        "cancelled",
        "retrying",
        "is_disconnect",
        "result_cache_hit",
    )
)
_LIST_KEYS = frozenset({"chart_ids", "dashboard_ids", "dataset_ids", "database_ids"})
_context: ContextVar[Mapping[str, SafeValue]] = ContextVar(
    "superset_telemetry_context", default=MappingProxyType({})
)


def safe_name(value: object, fallback: str = "unknown") -> str:
    """Accept an internal schema name, never stringify caller-owned objects."""
    return value if isinstance(value, str) and _NAME.fullmatch(value) else fallback


def sanitize(attributes: Mapping[str, object]) -> dict[str, SafeValue]:  # noqa: C901
    """Drop unknown or unsafe attributes without evaluating arbitrary objects."""
    result: dict[str, SafeValue] = {}
    for key, value in attributes.items():
        if not isinstance(key, str):
            continue
        if value is None:
            if (
                key
                in _ID_KEYS
                | _TOKEN_KEYS
                | _NUMBER_KEYS
                | _BOOL_KEYS
                | _TIME_KEYS
                | {"source"}
            ):
                result[key] = None
        elif key == "source":
            if isinstance(value, str) and value in _SOURCES:
                result[key] = value
        elif key == "warehouse_name":
            if isinstance(value, str) and value.strip() and _WAREHOUSE.fullmatch(value):
                result[key] = value
        elif key in _BOOL_KEYS:
            if type(value) is bool:
                result[key] = value
        elif key in _ID_KEYS | _TOKEN_KEYS:
            if (
                type(value) is int
                and (key in _ID_KEYS or key == "error_code")
                and 0 <= value <= 2**63 - 1
            ):
                result[key] = value
            elif isinstance(value, str) and _TOKEN.fullmatch(value):
                result[key] = value
        elif key in _TIME_KEYS and isinstance(value, str):
            if len(value) <= 40 and re.fullmatch(r"[0-9TtZz.:+ -]+", value):
                result[key] = value
        elif key in _NUMBER_KEYS:
            if (
                type(value) in (int, float)
                and abs(value) <= 1e18
                and math.isfinite(value)
            ):
                result[key] = value
        elif key in _LIST_KEYS and type(value) in (list, tuple):
            safe_ids = tuple(
                item
                for item in value[:64]
                if (type(item) is int and 0 <= item <= 2**63 - 1)
                or (isinstance(item, str) and _TOKEN.fullmatch(item))
            )
            result[key] = safe_ids
    return result


def _loaded_request_identity() -> dict[str, SafeValue]:
    """Read authenticated state only after existing auth loaded it; never ORM."""
    from flask import current_app, g, has_request_context
    from werkzeug.local import LocalProxy

    if not has_request_context():
        return {}
    try:
        observed_pid = g.get("observability_identity_pid")
        if observed_pid is not None and observed_pid != os.getpid():
            return {}
        g.observability_identity_pid = os.getpid()
        user = g.get("user")
        # FAB stores a lazy Flask-Login proxy. Resolving it here would trigger
        # request loaders (and potentially401s) on unrelated public endpoints.
        if type(user) is LocalProxy:
            user = g.get("_login_user")
        if user is None:
            return {}
        values = vars(user)
        if getattr(type(user), "is_guest_user", False):
            name = values.get("username")
            secret = current_app.config.get("SECRET_KEY")
            if isinstance(name, str) and isinstance(secret, (str, bytes)):
                cached = g.get("observability_guest_identity")
                if cached is None:
                    key = secret.encode() if isinstance(secret, str) else secret
                    cached = hmac.new(
                        key,
                        b"superset-observability-guest:" + name.encode(),
                        hashlib.sha256,
                    ).hexdigest()
                    g.observability_guest_identity = cached
                return {"guest_actor_id": cached}
            return {}
        user_id = values.get("id")
        if type(user_id) is int and user_id > 0:
            return {"request_actor_user_id": user_id, "effective_user_id": user_id}
    except Exception:  # pylint: disable=broad-except
        record_failure()
    return {}


def get_context() -> dict[str, SafeValue]:
    """Return safe context plus loaded server identity, without proxy/ORM reads."""
    snapshot = dict(_context.get())
    identity = _loaded_request_identity()
    if "effective_user_id" in identity:
        snapshot["effective_user_id"] = identity["effective_user_id"]
    for key in ("request_actor_user_id", "guest_actor_id"):
        if key in identity:
            snapshot.setdefault(key, identity[key])
    return snapshot


@contextmanager
def telemetry_context(**attributes: object) -> Iterator[None]:
    """Merge safe attributes for one scope and restore even on BaseException."""
    pid = os.getpid()
    token = _context.set({**_context.get(), **sanitize(attributes)})
    try:
        yield
    finally:
        if pid == os.getpid():
            _context.reset(token)


def wire_context() -> dict[str, TelemetryValue]:
    """Serialize primitive context and W3C parent for a trusted broker header."""
    from opentelemetry.trace.propagation.tracecontext import (
        TraceContextTextMapPropagator,
    )

    result = {
        key: value
        for key, value in get_context().items()
        if value is not None and not isinstance(value, tuple)
    }
    carrier: dict[str, str] = {}
    TraceContextTextMapPropagator().inject(carrier)
    if traceparent := carrier.get("traceparent"):
        result["traceparent"] = traceparent
    return result


@contextmanager
def restore_context(context: dict[str, object] | None) -> Iterator[None]:
    """Replace (not merge) a trusted task context and restore its trace parent."""
    from opentelemetry import context as otel_context
    from opentelemetry.trace.propagation.tracecontext import (
        TraceContextTextMapPropagator,
    )

    pid = os.getpid()
    incoming = context if isinstance(context, dict) else {}
    token = _context.set(sanitize(incoming))
    parent = incoming.get("traceparent")
    carrier = (
        {"traceparent": parent}
        if isinstance(parent, str) and _TRACEPARENT.fullmatch(parent)
        else {}
    )
    otel_token = otel_context.attach(TraceContextTextMapPropagator().extract(carrier))
    try:
        yield
    finally:
        if pid == os.getpid():
            otel_context.detach(otel_token)
            _context.reset(token)


def reset_after_fork() -> None:
    """A child must not inherit an in-flight request or task's identity."""
    _context.set({})
    from opentelemetry import context as otel_context

    otel_context.attach(otel_context.Context())
