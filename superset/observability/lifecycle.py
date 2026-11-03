# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
"""Trusted broker context and task/report lifecycle observations.

Only primitive observability headers cross the broker boundary. Task arguments,
results, authentication tokens and exception messages are never serialized here.
Registration only attaches signals; it does not initialize an SDK before fork.

Enable OBSERVABILITY_ENABLED on producers and consumers together. Brokers are a
trusted server boundary; do not expose publish credentials to browsers. Internal
report/warmup HTTP requests use Foundation's short-lived purpose-signed header,
the existing shared SECRET_KEY, and the exact WEBDRIVER_BASEURL origin. Redirects
are checked per hop. Original signed initiator and authenticated effective user
remain distinct; no guest tokens, query payloads or task arguments carry context.

Each broker attempt has fresh published_at/received_at/started_at/finished_at
timestamps; submitted_at retains the original submission across retries. Parent
task_received modifies only the trusted custom header in Request.request_dict,
which Celery serializes to its child. Queue histograms use valid known UTC
intervals, never synthetic zeros for missing or skewed clocks. Future ETA holds
remain visible in scheduled dispatch latency but are excluded from ready-broker
and reserved-worker backlog intervals.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable, TYPE_CHECKING
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request

from celery import current_task, signals
from flask import current_app, has_app_context, has_request_context, request

from superset.extensions import celery_app
from superset.observability import (
    emit_event,
    get_context,
    increment,
    is_enabled,
    observe_duration,
    operation,
    restore_context,
    signed_execution_headers,
    telemetry_context,
    wire_context,
)

if TYPE_CHECKING:
    from flask import Flask

HEADER = "superset_observability"
_registered = False
_OUTCOMES = {
    "SUCCESS": "success",
    "FAILURE": "error",
    "RETRY": "retry",
    "REVOKED": "cancelled",
    "ABORTED": "cancelled",
    "TIMED_OUT": "timeout",
    "PENDING": "deferred",
    "IN_PROGRESS": "running",
    "ABORTING": "cancelled",
    "success": "success",
    "failure": "error",
    "pending": "deferred",
    "in_progress": "running",
    "aborting": "cancelled",
    "aborted": "cancelled",
    "timed_out": "timeout",
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: Any) -> datetime | None:
    """Parse only an explicit aware timestamp; unknown clocks stay unknown."""
    try:
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, str) and len(value) <= 40:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        elif type(value) in (int, float) and math.isfinite(value):
            parsed = datetime.fromtimestamp(value, timezone.utc)
        else:
            return None
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return None
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _delivery_queue(routing_key: Any) -> str | None:
    """Resolve only a unique configured binding, never guess from arbitrary text."""
    if not isinstance(routing_key, str):
        return None
    matches = [
        name
        for name, queue in celery_app.amqp.queues.items()
        if (queue.routing_key or name) == routing_key
    ]
    return matches[0] if len(matches) == 1 else None


def _observe_wait(
    name: str, beginning: datetime | None, ending: datetime | None, phase: str
) -> None:
    if beginning is None or ending is None or ending < beginning:
        return
    elapsed = (ending - beginning).total_seconds()
    observe_duration(name, elapsed, phase=phase, outcome="success")
    emit_event(
        f"{name}.observed",
        operation=name,
        phase=phase,
        duration_seconds=elapsed,
        outcome="success",
    )


def _queue_waits(request: Any) -> None:
    """Separate broker residency, worker reservation and intentional ETA holds."""
    if getattr(request, "is_eager", False) or getattr(
        request, "called_directly", False
    ):
        return
    attributes = get_context()
    published = _as_utc(attributes.get("published_at"))
    received = _as_utc(attributes.get("received_at"))
    started = _as_utc(attributes.get("started_at"))
    scheduled_value = attributes.get("scheduled_at") or getattr(request, "eta", None)
    scheduled = _as_utc(scheduled_value)
    deferred = scheduled_value is not None and (
        scheduled is None or published is None or scheduled > published
    )
    delivery = getattr(request, "delivery_info", None) or {}
    redelivered = delivery.get("redelivered") is True
    # An inconsistent parent/publisher/child sequence is not a zero wait. Keep
    # valid same-worker intervals, but omit an invalid cross-clock total.
    if not redelivered and (
        received is None
        or (
            (published is None or published <= received)
            and (started is None or received <= started)
        )
    ):
        _observe_wait(
            "celery.dispatch_wait",
            published,
            started,
            "scheduled" if deferred else "queue",
        )
    if scheduled_value is not None and scheduled is None:
        return
    broker_beginning = (
        max(published, scheduled)
        if published is not None and scheduled is not None
        else published
    )
    worker_beginning = (
        max(received, scheduled)
        if received is not None and scheduled is not None
        else received
    )
    # Broker redelivery has no new producer timestamp or known requeue time.
    # Its original publish cannot be misreported as this delivery's ready wait.
    if not redelivered:
        _observe_wait("celery.broker_wait", broker_beginning, received, "broker")
    _observe_wait("celery.worker_wait", worker_beginning, started, "worker")


def execution_headers_for_url(url: str) -> dict[str, str]:
    """Sign only internal report/warmup HTTP traffic to the configured origin."""
    if (
        not is_enabled()
        or not has_app_context()
        or get_context().get("source") not in {"report", "warmup"}
    ):
        return {}
    base = current_app.config.get("WEBDRIVER_BASEURL") or ""
    try:
        target, trusted = urlsplit(url), urlsplit(base)

        def origin(parts: Any) -> tuple[str, str | None, int | None]:
            scheme, port = parts.scheme.lower(), parts.port
            return (
                scheme,
                parts.hostname,
                port if port is not None else (443 if scheme == "https" else 80),
            )

        if target.scheme not in {"http", "https"} or target.username or target.password:
            return {}
        if not trusted.hostname or origin(target) != origin(trusted):
            return {}
    except ValueError:
        return {}
    return signed_execution_headers()


def sign_execution_request(request: Request) -> None:
    """Unredirected headers cannot be copied automatically to another origin."""
    for key, value in execution_headers_for_url(request.full_url).items():
        request.add_unredirected_header(key, value)


class ExecutionContextRedirectHandler(HTTPRedirectHandler):
    """Re-sign same-origin redirects, never forward execution identity externally."""

    def redirect_request(
        self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str
    ) -> Request | None:
        request = super().redirect_request(req, fp, code, msg, headers, newurl)
        if request is not None:
            sign_execution_request(request)
        return request


def set_task_outcome(outcome: str) -> None:
    """Record swallowed/report retry outcomes without changing Celery semantics."""
    if (
        is_enabled()
        and current_task
        and outcome in {"success", "error", "retry", "skipped", "cancelled", "timeout"}
    ):
        current_task.request._superset_outcome = outcome


def observed(name: str, **attributes: Any) -> Callable:
    """Observe an existing phase without changing its result or exceptions."""

    def decorate(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            if not is_enabled():
                return fn(*args, **kwargs)
            with operation(name, **attributes):
                return fn(*args, **kwargs)

        return wrapped

    return decorate


def task_relationship(task_id: str, created: bool, task_type: str) -> None:
    """Emit one request-to-native-task edge, not another physical execution."""
    if not is_enabled():
        return
    if has_request_context():
        seen = request.environ.setdefault(
            "superset.observability.task_relationships", set()
        )
        if task_id in seen:
            return
        seen.add(task_id)
    action = "create" if created else "join"
    parent = get_context().get("task_id")
    if parent == task_id:
        parent = get_context().get("parent_task_id")
    emit_event(
        "task.relationship",
        task_id=task_id,
        task_type=task_type,
        consumer_action=action,
        parent_task_id=parent,
    )
    increment("task.relationships", consumer_action=action)


def task_transition(task_id: str, status: str, **attributes: Any) -> None:
    """Call after a successful committed CAS; failed transitions emit nothing."""
    outcome = _OUTCOMES.get(status, "unknown")
    emit_event(
        "task.transition",
        task_id=task_id,
        task_status=status,
        outcome=outcome,
        **attributes,
    )
    increment("task.transitions", task_status=status, outcome=outcome)


def _is_enabled() -> bool:
    return is_enabled()


def _before_publish(
    headers: dict | None = None,
    sender: str | None = None,
    routing_key: str | None = None,
    **_: Any,
) -> None:
    if not _is_enabled() or headers is None:
        return
    # Overwrite, rather than merge a caller-provided custom header. Producer
    # identity comes only from the server's current authenticated execution.
    context = wire_context()
    published_at = _utc_now().isoformat()
    context.setdefault("submitted_at", published_at)
    context["published_at"] = published_at
    context["retry_count"] = headers.get("retries", 0)
    for key in (
        "received_at",
        "started_at",
        "finished_at",
        "ended_at",
        "queue_name",
        "scheduled_at",
    ):
        context.pop(key, None)
    scheduled = _as_utc(headers.get("eta"))
    if scheduled is not None:
        context["scheduled_at"] = scheduled.isoformat()
    queue = _delivery_queue(routing_key)
    if queue is not None:
        context["queue_name"] = queue
    if sender == "reports.execute":
        context.update(source="report", report_execution_id=headers.get("id"))
    elif sender == "cache-warmup":
        context.update(source="warmup", warmup_run_id=headers.get("id"))
    if sender in {"reports.execute", "cache-warmup"}:
        context.setdefault("request_actor_user_id", None)
    headers[HEADER] = context


def _after_publish(
    headers: dict | None = None, sender: str | None = None, **_: Any
) -> None:
    if _is_enabled() and headers is not None:
        with restore_context(headers.get(HEADER)):
            emit_event(
                "celery.task.published",
                celery_task_id=headers.get("id"),
                celery_task_name=sender,
            )
            increment("celery.task.published")


def _received(request: Any = None, **_: Any) -> None:
    """Stamp the parent's Request dictionary which Celery serializes to its pool."""
    if not _is_enabled() or request is None:
        return
    request_dict = getattr(request, "request_dict", None)
    if not isinstance(request_dict, dict):
        return
    received_at = _utc_now().isoformat()
    delivery = getattr(request, "delivery_info", None) or {}
    scheduled = _as_utc(getattr(request, "eta", None))
    with (
        restore_context(request_dict.get(HEADER)),
        telemetry_context(
            celery_task_id=getattr(request, "id", None),
            celery_task_name=getattr(request, "name", None),
            retry_count=request_dict.get("retries", 0),
            received_at=received_at,
            queue_name=_delivery_queue(delivery.get("routing_key")),
            scheduled_at=scheduled.isoformat() if scheduled is not None else None,
        ),
    ):
        serialized = wire_context()
        # Celery 5.6 Request.execute_using_pool passes request_dict, not payload
        # metadata, to trace_task. Do not touch args/kwargs/body or dedup keys.
        request_dict[HEADER] = serialized
        message = getattr(request, "message", None)
        message_headers = getattr(message, "headers", None)
        if isinstance(message_headers, dict):
            message_headers[HEADER] = dict(serialized)
        emit_event(
            "celery.task.received",
            state="redelivered" if delivery.get("redelivered") is True else "received",
        )
        increment("celery.task.received")


def _prerun(task: Any = None, task_id: str | None = None, **_: Any) -> None:
    if not _is_enabled() or task is None:
        return
    request = task.request
    headers = getattr(request, "headers", None) or {}
    serialized = headers.get(HEADER)
    if serialized is None and getattr(request, "is_eager", False):
        serialized = wire_context()
    scope = restore_context(serialized)
    scope.__enter__()
    context = telemetry_context(
        celery_task_id=task_id,
        celery_task_name=task.name,
        retry_count=getattr(request, "retries", 0),
        started_at=_utc_now().isoformat(),
    )
    context.__enter__()
    _queue_waits(request)
    execution = operation("celery.task", celery_task_name=task.name)
    observation = execution.__enter__()
    request._superset_observation = (scope, context, execution, observation)


def _retry(request: Any = None, reason: Any = None, **_: Any) -> None:
    if not _is_enabled():
        return
    emit_event(
        "celery.task.retry",
        celery_task_id=getattr(request, "id", None),
        retry_count=getattr(request, "retries", 0),
        outcome="retry",
        error_type=type(reason).__name__ if reason is not None else None,
    )
    increment("celery.task.retries", outcome="retry")


def _failure(
    exception: BaseException | None = None,
    task_id: str | None = None,
    sender: Any = None,
    **_: Any,
) -> None:
    if _is_enabled():
        emit_event(
            "celery.task.error",
            outcome="error",
            celery_task_id=task_id,
            celery_task_name=getattr(sender, "name", None),
            error_type=type(exception).__name__ if exception is not None else None,
        )


def _revoked(
    request: Any = None, terminated: bool = False, expired: bool = False, **_: Any
) -> None:
    if not _is_enabled():
        return
    # task_revoked runs in the parent with a worker Request (request_dict),
    # whereas task_prerun receives the task's Context (headers).
    headers = (
        getattr(request, "headers", None)
        or getattr(request, "request_dict", None)
        or {}
    )
    with restore_context(headers.get(HEADER)):
        emit_event(
            "celery.task.cancelled",
            celery_task_id=getattr(request, "id", None),
            celery_task_name=getattr(request, "name", None),
            cancelled=terminated,
            outcome="expired" if expired else "cancelled",
        )
        increment(
            "celery.task.cancellations", outcome="expired" if expired else "cancelled"
        )


def _postrun(
    task: Any = None, state: str | None = None, retval: Any = None, **_: Any
) -> None:
    if task is None:
        return
    observation = getattr(task.request, "_superset_observation", None)
    if observation is None:
        return
    del task.request._superset_observation
    scope, context, execution, observation = observation
    try:
        outcome = _OUTCOMES.get(state or "", "unknown")
        # GTF failures are deliberately returned rather than raised. Read only
        # the finite status protocol, never arbitrary task results or messages.
        if task.name == "tasks.execute" and isinstance(retval, dict):
            native_status = retval.get("status")
            if isinstance(native_status, str) and native_status in _OUTCOMES:
                outcome = _OUTCOMES[native_status]
        outcome = getattr(task.request, "_superset_outcome", outcome)
        if hasattr(task.request, "_superset_outcome"):
            del task.request._superset_outcome
        observation.set_attribute("outcome", outcome)
        observation.set_attribute("finished_at", _utc_now().isoformat())
        execution.__exit__(None, None, None)
    finally:
        try:
            context.__exit__(None, None, None)
        finally:
            scope.__exit__(None, None, None)


def register_lifecycle(app: Flask) -> None:
    """Register once in the app parent; execution/export remains process-lazy."""
    global _registered  # pylint: disable=global-statement
    if not app.config.get("OBSERVABILITY_ENABLED"):
        return
    if _registered:
        return
    signals.before_task_publish.connect(_before_publish, weak=False)
    signals.after_task_publish.connect(_after_publish, weak=False)
    signals.task_received.connect(_received, weak=False)
    signals.task_prerun.connect(_prerun, weak=False)
    signals.task_retry.connect(_retry, weak=False)
    signals.task_failure.connect(_failure, weak=False)
    signals.task_revoked.connect(_revoked, weak=False)
    signals.task_postrun.connect(_postrun, weak=False)
    _registered = True
