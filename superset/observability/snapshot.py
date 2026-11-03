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
"""Finite, single-writer control-plane backlog collection.

Run ``observability.snapshot`` on the control queue every 30 seconds (Beat).
``collect_snapshot`` is the synchronous smoke/administration entry point. A
30-second coordination lease, deliberately retained until expiry, suppresses
both overlapping collectors and duplicate deliveries within one collection
interval. Never attach this collector to worker-process or request hooks.
Control workers retain their existing concurrency. The shared lease admits one
collector per interval across live children and rolling pods; each process keeps
its own writer identity. Availability must select the newest phase attempt,
including failed/interrupted children. State gauges select the newest complete
successful snapshot per phase, never a sum of retained per-PID observations.
Worker state counts describe replying workers, not an inference about missing
workers. Collection is staged before publication, so a later queue/RPC failure
cannot overwrite the last complete successful phase with partial state.
Broker collection supports at most 32 configured queues and fewer than 128 worker
replies per state; exceeding a bound reports that source unavailable, never
silently truncated. Socket/reply waits use two-second deadlines and publishing
never retries. The scheduled task additionally has 20s soft/25s hard limits.

Native counts are authoritative metastore aggregates, not Celery result-backend
estimates. Pending age excludes prerequisite runtime: runnable queue age starts
at the later of creation and latest prerequisite completion, matching pending
expiry. Dependency-waiting and failed-prerequisite work are separate populations.
Broker ready excludes active, prefetched/reserved and ETA-scheduled deliveries.
Missing/unreachable broker or worker observations are unavailable, never zeros.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import datetime
from typing import Any
from uuid import uuid4

import sqlalchemy as sa
from celery.app.control import flatten_reply
from celery.exceptions import SoftTimeLimitExceeded
from flask import current_app
from kombu import Producer
from sqlalchemy.orm import aliased
from superset_core.tasks.types import TaskStatus

from superset.commands.distributed_lock.acquire import AcquireDistributedLock
from superset.daos.tasks import TaskDAO
from superset.exceptions import LockAlreadyHeldException
from superset.extensions import celery_app, db
from superset.models.task_dependencies import TaskDependency
from superset.models.tasks import Task
from superset.observability import emit_event, gauge, operation, telemetry_context
from superset.tasks.constants import ACTIVE_STATES, TERMINAL_STATES

REMOTE_TIMEOUT_SECONDS = 2.0
SNAPSHOT_INTERVAL_SECONDS = 30
MAX_CONFIGURED_QUEUES = 32
MAX_WORKER_REPLIES = 128
_PENDING_STATES = ("runnable", "dependency_waiting", "dependency_failed", "claimed")


class _SnapshotProducer(Producer):
    """Pidbox hardcodes retry=True; observation must not retry indefinitely."""

    def publish(self, *args: Any, **kwargs: Any) -> Any:
        kwargs["retry"] = False
        return super().publish(*args, **kwargs)


def _native_snapshot() -> list[dict[str, Any]]:
    prerequisite = aliased(Task)
    dependencies = (
        db.session.query(
            TaskDependency.task_id.label("task_id"),
            sa.func.sum(
                sa.case((prerequisite.status.notin_(TERMINAL_STATES), 1), else_=0)
            ).label("waiting"),
            sa.func.sum(
                sa.case(
                    (
                        sa.and_(
                            prerequisite.status.in_(TERMINAL_STATES),
                            prerequisite.status != TaskStatus.SUCCESS.value,
                        ),
                        1,
                    ),
                    else_=0,
                )
            ).label("failed"),
            sa.func.max(
                sa.func.coalesce(
                    prerequisite.ended_at,
                    prerequisite.changed_on,
                    prerequisite.created_on,
                )
            ).label("last_end"),
        )
        .join(prerequisite, prerequisite.id == TaskDependency.depends_on_task_id)
        .group_by(TaskDependency.task_id)
        .subquery()
    )
    pending_state = sa.case(
        (
            sa.or_(Task.started_at.isnot(None), Task.last_heartbeat.isnot(None)),
            "claimed",
        ),
        (dependencies.c.waiting > 0, "dependency_waiting"),
        (dependencies.c.failed > 0, "dependency_failed"),
        else_="runnable",
    )
    state = sa.case(
        (Task.status == TaskStatus.PENDING.value, pending_state), else_="running"
    )
    queue_since = sa.case(
        (dependencies.c.last_end > Task.created_on, dependencies.c.last_end),
        else_=Task.created_on,
    )
    since = sa.case(
        (
            sa.and_(
                Task.status == TaskStatus.PENDING.value, pending_state == "runnable"
            ),
            queue_since,
        ),
        (Task.status == TaskStatus.PENDING.value, Task.created_on),
        else_=sa.func.coalesce(Task.started_at, Task.created_on),
    )
    now = (
        TaskDAO._db_utcnow()
    )  # Same DB clock as pending-expiry and heartbeat recovery.
    rows = (
        db.session.query(Task.status, state, sa.func.count(Task.id), sa.func.min(since))
        .outerjoin(dependencies, dependencies.c.task_id == Task.id)
        .filter(Task.status.in_(ACTIVE_STATES))
        .group_by(Task.status, state)
        .all()
    )
    values = {
        (status, dependency): (count, oldest)
        for status, dependency, count, oldest in rows
    }
    result = []
    # Always publish zeros for populations that drained since the last sample.
    populations = [
        (TaskStatus.PENDING.value, dependency) for dependency in _PENDING_STATES
    ]
    populations += [
        (status, "running")
        for status in ACTIVE_STATES
        if status != TaskStatus.PENDING.value
    ]
    for status, dependency in populations:
        count, oldest = values.get((status, dependency), (0, None))
        age = (
            max(0.0, (now - oldest).total_seconds())
            if isinstance(oldest, datetime)
            else 0.0
        )
        attributes = {"task_status": status, "dependency_state": dependency}
        result.append({**attributes, "count": count, "oldest_age_seconds": age})
    return result


def _broker_snapshot() -> dict[str, int]:
    queues = celery_app.amqp.queues
    names = tuple(queues) or (celery_app.conf.task_default_queue,)
    if len(names) > MAX_CONFIGURED_QUEUES:
        raise ValueError("Configured queue count exceeds the snapshot bound")
    transport_options = dict(celery_app.conf.broker_transport_options or {})
    transport_options.update(
        socket_timeout=REMOTE_TIMEOUT_SECONDS,
        socket_connect_timeout=REMOTE_TIMEOUT_SECONDS,
        retry_on_timeout=False,
        max_retries=0,
    )
    ready = {}
    with celery_app.connection_for_read(
        connect_timeout=REMOTE_TIMEOUT_SECONDS, transport_options=transport_options
    ) as connection:
        connection.ensure_connection(max_retries=0)
        with connection.channel() as channel:
            for name in names:
                if connection.transport.driver_type == "redis":
                    # Pinned Kombu Redis passive-declare reports an empty list
                    # as NOT_FOUND. Its readonly _size sums all configured
                    # priority lists and respects the transport key prefix.
                    count = channel._size(name)  # pylint: disable=protected-access
                else:
                    count = channel.queue_declare(
                        queue=name, passive=True
                    ).message_count
                ready[name] = count
    return ready


def _worker_snapshot() -> dict[str, dict[str, int]]:
    transport_options = dict(celery_app.conf.broker_transport_options or {})
    transport_options.update(
        socket_timeout=REMOTE_TIMEOUT_SECONDS,
        socket_connect_timeout=REMOTE_TIMEOUT_SECONDS,
        retry_on_timeout=False,
        max_retries=0,
    )
    with celery_app.connection_for_write(
        connect_timeout=REMOTE_TIMEOUT_SECONDS, transport_options=transport_options
    ) as connection:
        connection.ensure_connection(max_retries=0)
        with connection.channel() as channel:
            mailbox = celery_app.control.mailbox(connection)
            producer = _SnapshotProducer(channel, auto_declare=False)

            def read_state(state: str) -> dict[str, Any] | None:
                ticket = uuid4().hex
                deadline = time.monotonic() + REMOTE_TIMEOUT_SECONDS

                def check_deadline(_: Any) -> None:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Worker reply deadline exceeded")

                # Pinned pidbox APIs support an explicit producer. This keeps
                # both the connection and publish retries local and bounded;
                # Celery Inspect otherwise borrows an unbounded shared producer.
                mailbox._publish(  # pylint: disable=protected-access
                    state,
                    {"safe": True},
                    reply_ticket=ticket,
                    channel=channel,
                    timeout=REMOTE_TIMEOUT_SECONDS,
                    producer=producer,
                )
                replies = mailbox._collect(  # pylint: disable=protected-access
                    ticket,
                    limit=MAX_WORKER_REPLIES,
                    channel=channel,
                    timeout=REMOTE_TIMEOUT_SECONDS,
                    callback=check_deadline,
                )
                if len(replies) >= MAX_WORKER_REPLIES:
                    raise OverflowError("Worker reply bound reached")
                return flatten_reply(replies) if replies else None

            return _worker_states(read_state)


def _worker_states(
    read_state: Callable[[str], dict[str, Any] | None],
) -> dict[str, dict[str, int]]:
    queues = celery_app.amqp.queues
    if len(queues) > MAX_CONFIGURED_QUEUES:
        raise ValueError("Configured queue count exceeds the snapshot bound")
    bindings = {str(queue.routing_key or name): name for name, queue in queues.items()}
    result = {}
    for state in ("active", "reserved", "scheduled"):
        replies = read_state(state)
        if replies is None:
            emit_event(
                "observability.snapshot.unavailable", phase=state, outcome="unavailable"
            )
            raise TimeoutError("A worker state inspection received no replies")
        counts = {**{name: 0 for name in queues}, "unknown": 0}
        for tasks in replies.values():
            for task in tasks:
                request = task.get("request", {}) if state == "scheduled" else task
                delivery = request.get("delivery_info") or {}
                routing_key = str(delivery.get("routing_key") or "")
                queue = bindings.get(routing_key, "unknown")
                counts[queue] = counts.get(queue, 0) + 1
        result[state] = counts
    return result


def _publish_snapshot(phase: str, values: Any) -> None:
    """Publish only a fully collected phase, retaining zeros for drained states."""
    if phase == "metastore":
        for row in values:
            attributes = {
                "phase": phase,
                "task_status": row["task_status"],
                "dependency_state": row["dependency_state"],
            }
            gauge("task.backlog", row["count"], **attributes)
            gauge("task.oldest_age", row["oldest_age_seconds"], **attributes)
    elif phase == "broker":
        for queue, count in values.items():
            gauge(
                "celery.queue.depth",
                count,
                phase=phase,
                queue_name=queue,
                state="ready",
            )
    else:
        for state, counts in values.items():
            for queue, count in counts.items():
                gauge(
                    "celery.queue.depth",
                    count,
                    phase=phase,
                    queue_name=queue,
                    state=state,
                )


def collect_snapshot() -> dict[str, Any]:  # noqa: C901
    """Collect once, returning safe aggregates; failures never poison control work."""
    if not current_app.config.get("OBSERVABILITY_ENABLED"):
        return {"outcome": "disabled"}
    lease = AcquireDistributedLock(
        "observability:snapshot", ttl_seconds=SNAPSHOT_INTERVAL_SECONDS
    )
    try:
        lease.run()
    except LockAlreadyHeldException:
        return {"outcome": "skipped"}
    except SoftTimeLimitExceeded:
        raise
    except Exception as ex:  # noqa: BLE001
        emit_event(
            "observability.snapshot.unavailable",
            phase="coordination",
            outcome="unavailable",
            error_type=type(ex).__name__,
        )
        return {"outcome": "unavailable"}
    result: dict[str, Any] = {"outcome": "success"}
    with (
        telemetry_context(source="observability"),
        operation("observability.snapshot") as observation,
    ):
        for phase, collect in (
            ("metastore", _native_snapshot),
            ("broker", _broker_snapshot),
            ("workers", _worker_snapshot),
        ):
            # Announce the attempt before any blocking call. A killed child must
            # not leave its inherited previous availability looking successful.
            gauge("observability.snapshot.available", 0, phase=phase)
            gauge("observability.snapshot.attempt_timestamp", time.time(), phase=phase)
            try:
                result[phase] = collect()
                _publish_snapshot(phase, result[phase])
                completed_at = time.time()
                gauge("observability.snapshot.available", 1, phase=phase)
                gauge("observability.snapshot.timestamp", completed_at, phase=phase)
                gauge(
                    "observability.snapshot.attempt_timestamp",
                    completed_at,
                    phase=phase,
                )
            except SoftTimeLimitExceeded:
                gauge(
                    "observability.snapshot.attempt_timestamp", time.time(), phase=phase
                )
                raise
            except Exception as ex:  # noqa: BLE001
                if phase == "metastore":
                    try:
                        db.session.rollback()  # pylint: disable=consider-using-transaction
                    except SoftTimeLimitExceeded:
                        raise
                    except Exception as cleanup_error:  # noqa: BLE001
                        emit_event(
                            "observability.snapshot.cleanup_error",
                            phase=phase,
                            outcome="error",
                            error_type=type(cleanup_error).__name__,
                        )
                result["outcome"] = "partial"
                gauge("observability.snapshot.available", 0, phase=phase)
                gauge(
                    "observability.snapshot.attempt_timestamp", time.time(), phase=phase
                )
                emit_event(
                    "observability.snapshot.unavailable",
                    phase=phase,
                    outcome="unavailable",
                    error_type=type(ex).__name__,
                )
        observation.set_attribute("outcome", result["outcome"])
        emit_event("observability.snapshot.collected", outcome=result["outcome"])
    return result


@celery_app.task(
    name="observability.snapshot",
    soft_time_limit=20,
    time_limit=25,
    expires=SNAPSHOT_INTERVAL_SECONDS,
    ignore_result=True,
)
def snapshot() -> None:
    """Beat entry point. No loop, no per-worker DB scanner, no result payload."""
    collect_snapshot()
