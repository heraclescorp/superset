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
"""Explicit units, finite instruments/dimensions, cumulative native SDK metrics.

The gateway has add_metric_suffixes disabled. Therefore counter names explicitly
end in _total and duration/size names explicitly say _seconds/_bytes. User/query
IDs are event/span-only. Warehouse names and database IDs are bounded asset
dimensions. Each instrument retains256 detailed series plus one explicit
otel.metric.overflow series; counters/histograms retain every observation.
"""

from __future__ import annotations

import math
import os
import re
import threading
from collections.abc import Iterable, Mapping
from types import MappingProxyType
from typing import Any

from superset.observability.context import get_context, sanitize
from superset.observability.diagnostics import record_failure
from superset.observability.legacy import canonical_legacy_key
from superset.observability.runtime import get_runtime

BROWSER_OPERATIONS = frozenset(
    {
        "browser.chart.load",
        "browser.chart.data_ready",
        "browser.chart.render",
        "browser.chart.request",
        "browser.dashboard.load",
    }
)
OPERATIONS = frozenset(
    (
        "http.request",
        "db.connection.acquire",
        "sql.statement",
        "sql.execute",
        "sql.fetch",
        "sql.dataframe",
        "sql.planning",
        "sql.assembly",
        "sql.resultset",
        "sql.cache.get",
        "sql.cache.set",
        "chart.query",
        "chart.data",
        "chart.execute",
        "chart.planning",
        "chart.assembly",
        "chart.cache.get",
        "chart.cache.set",
        "chart.result",
        "chart.data.acquire",
        "chart.query.plan",
        "chart.cache.resolve",
        "chart.payload.assemble",
        "chart.data.serialize",
        "chart.response.serialize",
        "cache.get",
        "cache.set",
        "cache.lookup",
        "cache.write",
        "celery.task",
        "task.dispatch",
        "task.cancel",
        "screenshot.capture",
        "observability.snapshot",
        "report.execute",
        "report.schedule",
        "report.evaluate",
        "report.capture",
        "report.export",
        "report.delivery",
        "report.notification",
        "warmup.execute",
        "warmup.query",
        "warmup.capture",
        "export.execute",
    )
)
OPERATIONS = (
    OPERATIONS
    | BROWSER_OPERATIONS
    | frozenset({"celery.broker_wait", "celery.worker_wait", "celery.dispatch_wait"})
)
COUNTERS = {
    "task.relationships": "superset_task_relationships_total",
    "task.transitions": "superset_task_transitions_total",
    "celery.task.retries": "superset_celery_task_retries_total",
    "celery.task.cancellations": "superset_celery_task_cancellations_total",
    "celery.task.published": "superset_celery_task_published_total",
    "celery.task.received": "superset_celery_task_received_total",
    "task.cancellations": "superset_task_cancellations_total",
    "report.transitions": "superset_report_transitions_total",
    "report.retries": "superset_report_retries_total",
    "db.connections": "superset_db_connections_total",
    "db.invalidations": "superset_db_invalidations_total",
    "db.errors": "superset_db_errors_total",
    "db.checkouts": "superset_db_checkouts_total",
    "db.checkins": "superset_db_checkins_total",
    "db.reconnects": "superset_db_reconnects_total",
}
GAUGES = {
    "task.backlog": "superset_task_backlog",
    "task.oldest_age": "superset_task_oldest_age_seconds",
    "celery.queue.depth": "superset_celery_queue_depth",
    "observability.snapshot.available": "superset_observability_snapshot_available",
    "observability.snapshot.timestamp": (
        "superset_observability_snapshot_timestamp_seconds"
    ),
    "observability.snapshot.attempt_timestamp": (
        "superset_observability_snapshot_attempt_timestamp_seconds"
    ),
    "db.pool.checked_out": "superset_db_pool_checked_out",
    "db.pool.size": "superset_db_pool_size",
    "db.pool.overflow": "superset_db_pool_overflow",
}
_DIMENSIONS: dict[str, frozenset[str]] = {
    "operation": OPERATIONS,
    "source": frozenset(
        (
            "chart",
            "sql_lab",
            "report",
            "warmup",
            "export",
            "metadata",
            "system",
            "browser",
            "observability",
        )
    ),
    "outcome": frozenset(
        (
            "success",
            "error",
            "failed",
            "failure",
            "cancelled",
            "canceled",
            "timeout",
            "retry",
            "started",
            "finished",
            "hit",
            "miss",
            "created",
            "joined",
            "rejected",
            "unavailable",
            "available",
            "skipped",
            "invalidated",
            "connected",
            "disconnected",
            "acquired",
            "released",
            "deferred",
            "running",
            "partial",
            "expired",
            "not_persisted",
            "abandoned",
        )
    ),
    "phase": frozenset(
        (
            "execute",
            "fetch",
            "dataframe",
            "planning",
            "assembly",
            "result",
            "request",
            "render",
            "capture",
            "export",
            "delivery",
            "notification",
            "evaluate",
            "schedule",
            "cache_get",
            "cache_set",
            "metastore",
            "broker",
            "worker",
            "queue",
            "workers",
            "active",
            "reserved",
            "scheduled",
            "browser",
            "pdf",
            "tabular",
            "data_frame",
            "evaluation",
            "notification_delivery",
            "parse",
            "statement_blocks",
            "mutation",
            "payload_serialization",
            "arrow_serialization",
            "records",
            "payload",
            "compression",
            "cache_serialization",
            "load",
            "data",
            "data_ready",
            "dashboard",
        )
    ),
    "db_system": frozenset(
        (
            "snowflake",
            "postgresql",
            "postgres",
            "mysql",
            "sqlite",
            "mssql",
            "oracle",
            "trino",
            "presto",
            "bigquery",
            "clickhouse",
            "redshift",
            "duckdb",
        )
    ),
    "role": frozenset({"warehouse", "metastore"}),
    "worker_pool": frozenset(
        (
            "web",
            "queries",
            "reports",
            "control",
            "beat",
        )
    ),
    "error_category": frozenset(
        (
            "database",
            "timeout",
            "cancelled",
            "canceled",
            "connection",
            "authentication",
            "authorization",
            "validation",
            "internal",
            "unavailable",
            "operational",
            "unknown",
        )
    ),
    "status_class": frozenset({"1xx", "2xx", "3xx", "4xx", "5xx"}),
    "task_status": frozenset(
        (
            "pending",
            "queued",
            "running",
            "in_progress",
            "aborting",
            "success",
            "failure",
            "failed",
            "aborted",
            "cancelling",
            "canceled",
            "cancelled",
            "timed_out",
            "PENDING",
            "RUNNING",
            "SUCCESS",
            "FAILURE",
            "ABORTED",
        )
    ),
    "state": frozenset(
        (
            "pending",
            "queued",
            "running",
            "in_progress",
            "aborting",
            "success",
            "failed",
            "failure",
            "aborted",
            "ready",
            "active",
            "reserved",
            "scheduled",
            "PENDING",
            "RUNNING",
            "SUCCESS",
            "FAILURE",
            "ABORTED",
        )
    ),
    "dependency_state": frozenset(
        (
            "runnable",
            "dependency_waiting",
            "dependency_failed",
            "claimed",
            "running",
        )
    ),
    "task_type": frozenset(
        (
            "chart_data",
            "query",
            "sql_lab",
            "export",
            "report",
            "screenshot",
            "thumbnail",
            "metadata",
        )
    ),
    "celery_task_name": frozenset(
        (
            "sql_lab.get_sql_results",
            "sql_lab.get_sql_results_task",
            "reports.scheduler",
            "reports.execute",
            "reports.prune_log",
            "cache-warmup",
            "observability.snapshot",
            "tasks.execute",
            "execute_task",
            "superset.tasks.scheduler.execute_task",
            "thumbnails.cache_chart_thumbnail",
            "thumbnails.cache_dashboard_thumbnail",
        )
    ),
    "queue_name": frozenset(
        (
            "celery",
            "default",
            "queries",
            "reports",
            "control",
            "sql_lab",
            "superset-queries",
            "superset-reports",
            "superset-control",
        )
    ),
    "consumer_action": frozenset(
        {
            "create",
            "join",
            "detach",
            "unsubscribe",
            "cancel",
            "aborted",
            "unsubscribed",
            "detached",
            "cancelled",
        }
    ),
    "snapshot_kind": frozenset({"tasks", "broker", "workers", "metastore"}),
    "statement_type": frozenset(
        {"select", "ddl", "dml", "session", "metadata", "other"}
    ),
    "cache_backend": frozenset({"data", "results"}),
    "trust": frozenset({"client_untrusted", "server"}),
}
_BOOL_DIMENSIONS = frozenset(
    {"cache_hit", "async_execution", "force_refresh", "automation"}
)


def configure_metric_queues(names: Iterable[str]) -> None:
    """Preserve configured Celery queue names, with a finite bootstrap bound."""
    configured = sorted(
        {
            name
            for name in names
            if isinstance(name, str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,160}", name)
        }
    )[:32]
    _DIMENSIONS["queue_name"] = frozenset(configured) | frozenset(
        (
            "celery",
            "default",
            "queries",
            "reports",
            "control",
            "sql_lab",
            "superset-queries",
            "superset-reports",
            "superset-control",
        )
    )


def metric_attributes(attributes: Mapping[str, object]) -> dict[str, str | bool | int]:  # noqa: C901
    """Project onto finite enums; never use IDs, paths, arbitrary task names."""
    result: dict[str, str | bool | int] = {}
    for key, allowed in _DIMENSIONS.items():
        value = attributes.get(key)
        if isinstance(value, str):
            if key == "queue_name" and value not in allowed:
                result["otel.metric.overflow"] = True
            else:
                result[key] = value if value in allowed else "other"
    for key in _BOOL_DIMENSIONS:
        if type(attributes.get(key)) is bool:
            result[key] = attributes[key]  # type: ignore[assignment]
    if isinstance(attributes.get("legacy_key"), str):
        result["legacy_key"] = canonical_legacy_key(attributes["legacy_key"])
    for key, value in sanitize(
        {
            "warehouse_name": attributes.get("warehouse_name"),
            "database_id": attributes.get("database_id"),
        }
    ).items():
        if key == "warehouse_name" and isinstance(value, str):
            result[key] = value
        elif key == "database_id" and type(value) is int:
            result[key] = value
    return result


class _ScopeMetrics:
    """One PID-owned scope balance with immutable start dimensions.

    These are process-local diagnostics, not authoritative global task backlog.
    A process that dies mid-scope retains its final balance in the backend; only
    live service.instance.id writers should contribute to in-flight displays.
    """

    def __init__(
        self, attributes: dict[str, str | bool | int], completed: Any, inflight: Any
    ) -> None:
        self.pid = os.getpid()
        self.attributes: Mapping[str, str | bool | int] = MappingProxyType(attributes)
        self.completed = completed
        self.inflight = inflight
        self._finished = False
        self._lock = threading.Lock()

    def finish(self) -> None:
        """Complete/decrement once, even on duplicate or concurrent teardown."""
        # Never acquire locks or record instruments inherited from another PID.
        if self.pid != os.getpid():
            return
        with self._lock:
            if self._finished:
                return
            self._finished = True
        try:
            self.completed.add(1, self.attributes)
        except Exception:  # pylint: disable=broad-except
            record_failure()
        finally:
            try:
                self.inflight.add(-1, self.attributes)
            except Exception:  # pylint: disable=broad-except
                record_failure()


def _begin_operation_scope(
    name: str, attributes: Mapping[str, object]
) -> _ScopeMetrics | None:
    """Count actual entered scopes; supplied durations must never call this.

    One shared series decision routes all three instruments. An overflow start
    must finish/decrement the same overflow series, regardless of completion
    ordering, dimension changes or later cardinality pressure.
    """
    try:
        state = get_runtime()
        if state is None:
            return None
        attrs = metric_attributes(
            {**attributes, "operation": name if name in OPERATIONS else "other"}
        )
        for key in (
            "outcome",
            "error_category",
            "status_class",
            "database_id",
            "warehouse_name",
            "legacy_key",
        ):
            attrs.pop(key, None)
        signature = tuple(sorted(attrs.items()))
        with state.instrument_lock:
            series = state.series.setdefault("superset_operation_scopes", set())
            if attrs.get("otel.metric.overflow") or (
                signature not in series and len(series) >= 256
            ):
                attrs = {"otel.metric.overflow": True}
                state.overflowed_metric_observations += 1
            else:
                series.add(signature)
            meter = None
            instruments = []
            for kind, metric_name in (
                ("counter", "superset_operation_scopes_started_total"),
                ("counter", "superset_operation_scopes_completed_total"),
                ("updown", "superset_operation_scopes_inflight"),
            ):
                instrument = state.instruments.get((kind, metric_name))
                if instrument is None:
                    if meter is None:
                        meter = state.meter_provider.get_meter(
                            "superset.observability", "1"
                        )
                    create = (
                        meter.create_up_down_counter
                        if kind == "updown"
                        else meter.create_counter
                    )
                    instrument = create(metric_name, unit="1")
                    state.instruments[(kind, metric_name)] = instrument
                instruments.append(instrument)
        started, completed, inflight = instruments
        scope = _ScopeMetrics(attrs, completed, inflight)
        inflight.add(1, scope.attributes)
        try:
            started.add(1, scope.attributes)
        except Exception:  # pylint: disable=broad-except
            # Compensate a successfully recorded balance if SDK start recording
            # fails. Collector failures occur asynchronously, not on this path.
            inflight.add(-1, scope.attributes)
            return None
        return scope
    except Exception:  # pylint: disable=broad-except
        return None


def _record(
    kind: str,
    name: str,
    value: float,
    attributes: Mapping[str, object],
    *,
    inherit_context: bool = True,
) -> None:
    try:
        if (
            type(value) not in (int, float)
            or abs(value) > 1e18
            or not math.isfinite(value)
        ):
            return
        state = get_runtime()
        if state is None:
            return
        attrs = metric_attributes(
            {**(get_context() if inherit_context else {}), **attributes}
        )
        signature = tuple(sorted(attrs.items()))
        with state.instrument_lock:
            series = state.series.setdefault(name, set())
            if attrs.get("otel.metric.overflow") or (
                signature not in series and len(series) >= 256
            ):
                state.overflowed_metric_observations += 1
                attrs = {"otel.metric.overflow": True}
            else:
                series.add(signature)
            instrument = state.instruments.get((kind, name))
            if instrument is None:
                meter = state.meter_provider.get_meter("superset.observability", "1")
                unit = "s" if "_seconds" in name else "By" if "_bytes" in name else "1"
                create = {
                    "counter": meter.create_counter,
                    "updown": meter.create_up_down_counter,
                    "histogram": meter.create_histogram,
                    "gauge": meter.create_gauge,
                }[kind]
                instrument = create(name, unit=unit)
                state.instruments[(kind, name)] = instrument
        if kind in {"counter", "updown"}:
            instrument.add(value, attrs)
        else:
            instrument.record(value, attrs) if kind == "histogram" else instrument.set(
                value, attrs
            )
    except Exception:  # pylint: disable=broad-except
        record_failure()


def increment(name: str, value: int = 1, **attributes: object) -> None:
    """Record only registered monotonic counters; unknown names are discarded."""
    if isinstance(name, str) and name in COUNTERS and type(value) is int and value >= 0:
        _record("counter", COUNTERS[name], value, attributes)


def gauge(name: str, value: float, **attributes: object) -> None:
    """Record a registered gauge with bounded database/warehouse asset labels."""
    if isinstance(name, str) and name in GAUGES:
        _record("gauge", GAUGES[name], value, attributes)


def observe_operation(
    name: str, duration: float, attributes: Mapping[str, object]
) -> None:
    """Count every completed scope exactly once, with monotonic seconds."""
    attrs = {**attributes, "operation": name if name in OPERATIONS else "other"}
    _record("counter", "superset_operations_total", 1, attrs)
    _record("histogram", "superset_operation_duration_seconds", max(0, duration), attrs)


def observe_duration(name: str, duration_seconds: float, **attributes: object) -> None:
    """Observe validated caller-supplied seconds, without events/spans/timing.

    Browser callers must authenticate and authorize assets before calling this
    API. Their durations are client-untrusted, terminal only, and intentionally
    isolated from all ambient signed/report/actor/asset execution context.
    """
    if not isinstance(name, str) or name not in OPERATIONS:
        return
    if (
        type(duration_seconds) not in (int, float)
        or not 0 <= duration_seconds <= 1e18
        or not math.isfinite(duration_seconds)
    ):
        return
    if name in BROWSER_OPERATIONS:
        if (
            duration_seconds > 86400
            or not isinstance(attributes.get("outcome"), str)
            or attributes["outcome"] not in {"success", "error", "abandoned"}
        ):
            return
        safe = {
            key: value
            for key, value in attributes.items()
            if key
            in {
                "outcome",
                "phase",
                "cache_hit",
                "async_execution",
                "force_refresh",
                "automation",
            }
        }
        safe.update(operation=name, source="browser", trust="client_untrusted")
        _record("counter", "superset_operations_total", 1, safe, inherit_context=False)
        _record(
            "histogram",
            "superset_operation_duration_seconds",
            duration_seconds,
            safe,
            inherit_context=False,
        )
    else:
        observe_operation(name, duration_seconds, attributes)


def record_legacy(kind: str, key: str, value: float) -> None:
    """Use a finite legacy-key label, including decrement up/down semantics."""
    safe_key = canonical_legacy_key(key)
    name = {
        "counter": "superset_legacy_events_total",
        "updown": "superset_legacy_balance",
        "histogram": "superset_legacy_duration_seconds",
        "gauge": "superset_legacy_value",
    }[kind]
    _record(kind, name, value, {"legacy_key": safe_key})
