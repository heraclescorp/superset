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
"""Unsampled, payload-free filelog events and precisely-once operation scopes."""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timezone
from types import TracebackType
from typing import Any
from uuid import uuid4

from superset.observability.context import (
    get_context,
    safe_name,
    sanitize,
    telemetry_context,
)
from superset.observability.diagnostics import record_failure
from superset.observability.event_writer import EventQueueHandler
from superset.observability.metrics import (
    _begin_operation_scope,
    _ScopeMetrics,
    observe_operation,
)
from superset.observability.runtime import enabled, get_runtime, resource_identity
from superset.utils import json

_LOGGER = logging.getLogger("superset.observability.events")


def configure_event_logger() -> None:
    """Own a queue-only logger, independent of worker root logging and stdout."""
    _LOGGER.disabled = False
    if _LOGGER.level != logging.INFO:
        _LOGGER.setLevel(logging.INFO)
    _LOGGER.propagate = False
    if len(_LOGGER.handlers) != 1 or not isinstance(
        _LOGGER.handlers[0], EventQueueHandler
    ):
        _LOGGER.handlers = [EventQueueHandler()]


def emit_event(event_name: str, **attributes: object) -> None:
    """Emit a bounded JSON envelope without exception strings, payloads or SQL."""
    if not enabled():
        return
    try:
        from opentelemetry import trace

        configure_event_logger()
        metadata: dict[str, Any] = {**get_context(), **sanitize(attributes)}
        state = get_runtime()
        identity = state.resource if state else resource_identity()
        metadata.update(
            {
                "schema_version": 1,
                "event_id": metadata.get("event_id") or uuid4().hex,
                "service_name": identity["service.name"],
                "service_instance_id": identity["service.instance.id"],
                "worker_pool": identity["superset.worker_pool"],
                "process_pid": os.getpid(),
            }
        )
        span_context = trace.get_current_span().get_span_context()
        if span_context.is_valid:
            metadata["traceId"] = format(span_context.trace_id, "032x")
            metadata["spanId"] = format(span_context.span_id, "016x")
        envelope = {
            "time": datetime.now(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "levelLabel": "INFO",
            "message": safe_name(event_name),
            "metadata": metadata,
        }
        _LOGGER.info(
            json.dumps(
                envelope, separators=(",", ":"), ensure_ascii=True, allow_nan=False
            )
        )
    except Exception:  # pylint: disable=broad-except
        record_failure()


def exception_attributes(error: BaseException) -> dict[str, object]:
    """Read safe driver codes, not str(error), args, stack traces or SQLAlchemy SQL."""
    attrs: dict[str, object] = {"error_type": type(error).__name__}
    error_name = type(error).__name__
    if error_name == "Retry":
        attrs.update(outcome="retry", error_category="operational")
    elif error_name in {"TimeoutError", "SoftTimeLimitExceeded", "TimeLimitExceeded"}:
        attrs.update(outcome="timeout", error_category="timeout")
    elif error_name in {"CancelledError", "TaskRevokedError", "TaskAbortedError"}:
        attrs.update(outcome="cancelled", error_category="cancelled")
    else:
        attrs.update(outcome="error", error_category="internal")
    for key in ("sqlstate", "pgcode", "errno"):
        try:
            value = getattr(error, key, None)
            if type(value) is int or (
                isinstance(value, str) and 0 < len(value) <= 16 and value.isalnum()
            ):
                attrs["error_code"] = str(value)
                break
        except Exception:  # pylint: disable=broad-except
            record_failure()
            continue
    return sanitize(attrs)


class Operation:
    """An operation is also its context manager; finish is idempotent and fail-open."""

    def __init__(self, name: str, **attributes: object) -> None:
        self.name = safe_name(name)
        self.attributes = sanitize(attributes)
        self.started = 0.0
        self.pid = os.getpid()
        self.finished = False
        self.span: Any = None
        self.span_scope: Any = None
        self.context_scope: Any = None
        self.active = False
        self._entered = False
        self._finish_lock = threading.RLock()
        self._scope_metrics: _ScopeMetrics | None = None

    def set_attribute(self, key: str, value: object) -> None:
        """Attach an allowlisted value; arbitrary nested objects are dropped."""
        self.attributes.update(sanitize({key: value}))

    def __enter__(self) -> Operation:
        pid = os.getpid()
        if self.pid != pid:
            if self._entered:
                return self
            self.pid = pid
            self._finish_lock = threading.RLock()
        with self._finish_lock:
            if self._entered or self.finished:
                return self
            self._entered = True
            self.started = time.monotonic()
            self.active = enabled()
            if not self.active:
                return self
            try:
                from opentelemetry import trace

                self.context_scope = telemetry_context(
                    **{**self.attributes, "operation": self.name}
                )
                self.context_scope.__enter__()
                start_attributes = {
                    **get_context(),
                    **self.attributes,
                    "operation": self.name,
                }
                self._scope_metrics = _begin_operation_scope(
                    self.name, start_attributes
                )
                state = get_runtime()
                if state is not None:
                    self.span = state.tracer_provider.get_tracer(
                        "superset.observability", "1"
                    ).start_span(
                        self.name,
                        attributes={
                            key: value
                            for key, value in start_attributes.items()
                            if value is not None
                        },
                    )
                    self.span_scope = trace.use_span(
                        self.span,
                        end_on_exit=False,
                        record_exception=False,
                        set_status_on_exception=False,
                    )
                    self.span_scope.__enter__()
                emit_event(
                    f"{self.name}.started",
                    **{**self.attributes, "operation": self.name},
                )
            except Exception:  # pylint: disable=broad-except
                record_failure()
            return self

    def __exit__(  # noqa: C901
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        # Do not acquire parent-owned locks or detach inherited ContextVar tokens.
        if self.pid != os.getpid():
            return False
        with self._finish_lock:
            if self.finished:
                return False
            self.finished = True
            if not self.active:
                return False
        try:
            from opentelemetry.trace import Status, StatusCode

            duration = max(0, time.monotonic() - self.started)
            if exc is not None:
                self.attributes.update(exception_attributes(exc))
            else:
                self.attributes.setdefault("outcome", "success")
            self.attributes["duration_seconds"] = duration
            attributes = {**get_context(), **self.attributes, "operation": self.name}
            emit_event(f"{self.name}.finished", **attributes)
            observe_operation(self.name, duration, attributes)
            if self.span is not None:
                for key, value in attributes.items():
                    if value is not None:
                        self.span.set_attribute(key, value)
                failed = self.attributes.get("outcome") in {
                    "error",
                    "failure",
                    "failed",
                    "timeout",
                }
                self.span.set_status(
                    Status(StatusCode.ERROR if failed else StatusCode.OK)
                )
        except Exception:  # pylint: disable=broad-except
            record_failure()
        finally:
            try:
                if self._scope_metrics is not None:
                    self._scope_metrics.finish()
            except Exception:  # pylint: disable=broad-except
                record_failure()
            for scope in (self.span_scope, self.context_scope):
                try:
                    if scope is not None:
                        scope.__exit__(None, None, None)
                except Exception:  # pylint: disable=broad-except
                    record_failure()
            try:
                if self.span is not None:
                    self.span.end()
            except Exception:  # pylint: disable=broad-except
                record_failure()
        return False


def operation(name: str, **attributes: object) -> Operation:
    """Return a non-suppressing monotonic operation scope, including on failure."""
    return Operation(name, **attributes)
