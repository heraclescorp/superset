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
"""PID-owned native OTLP SDK providers; never register process-global providers.

Flask bootstrap only stores configuration. The first operation (or explicit
init_process in a Celery child/beat/standalone CLI) starts exporters. One daemon
owns native OTLP I/O: business threads enqueue completed SDK spans and record SDK
metrics only. The SDK periodic reader has interval infinity and no SDK batch
processor is used: their automatic at-fork callbacks would restart inherited
gRPC exporters. Our child hook discards providers/queues/locks without flushing
parent telemetry or calling inherited exporter locks. Children build fresh
resources, channels and cumulative streams. No exporter runs in a preload master.
"""

from __future__ import annotations

import atexit
import gc
import os
import re
import socket
import threading
import time
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import unquote
from uuid import uuid4

from superset.observability.context import reset_after_fork
from superset.observability.diagnostics import (
    failures as observation_failures,
    record_failure,
)
from superset.observability.event_writer import (
    configure as configure_event_writer,
    diagnostics as event_writer_diagnostics,
    shutdown as shutdown_event_writer,
)

_LOCK = threading.RLock()
_CONFIG: dict[str, object] = {}
_STATE: Runtime | None = None
_ATTEMPTED_PID: int | None = None
_EVENT_RESOURCE: dict[str, str | int] | None = None
# Retain inherited channels until process exit: collecting their Python cycles
# in a child could invoke gRPC finalizers against parent-owned locks. Never use,
# flush or close these objects in the child; fresh runtime channels own all I/O.
_INHERITED_RUNTIMES: list[Runtime] = []


def enabled() -> bool:
    """Use app configuration when initialized, environment for standalone tools."""
    value = _CONFIG.get(
        "OBSERVABILITY_ENABLED", os.getenv("SUPERSET_OBSERVABILITY_ENABLED", "false")
    )
    from flask import current_app, has_app_context

    if has_app_context():
        value = current_app.config.get("OBSERVABILITY_ENABLED", value)
    return value is True or (
        isinstance(value, str) and value.lower() in {"1", "true", "yes", "on"}
    )


def configure(config: Mapping[str, object]) -> None:
    """Store only telemetry settings; do not retain app/DB/secret configuration."""
    global _CONFIG
    _CONFIG = {
        key: config[key]
        for key in (
            "OBSERVABILITY_ENABLED",
            "OBSERVABILITY_METRIC_EXPORT_INTERVAL_SECONDS",
            "OBSERVABILITY_EXPORT_TIMEOUT_SECONDS",
            "OBSERVABILITY_TRACE_SAMPLE_RATIO",
            "OBSERVABILITY_MAX_QUEUE_SIZE",
        )
        if key in config
    }
    configure_event_writer(
        max_queue_size=config.get("OBSERVABILITY_EVENT_MAX_QUEUE_SIZE", 2048),
        shutdown_timeout=config.get(
            "OBSERVABILITY_EVENT_SHUTDOWN_TIMEOUT_SECONDS", 0.25
        ),
    )


def _number(key: str, default: float, minimum: float, maximum: float) -> float:
    try:
        value = float(_CONFIG.get(key, os.getenv(f"SUPERSET_{key}", str(default))))
        return min(maximum, max(minimum, value))
    except (TypeError, ValueError):
        return default


def _resource(role: str | None = None) -> dict[str, str | int]:
    from superset.observability.context import safe_name

    pool = safe_name(role or os.getenv("SUPERSET_OBSERVABILITY_POOL", "web"), "other")
    attributes: dict[str, str | int] = {
        "service.name": os.getenv("OTEL_SERVICE_NAME", "superset")[:80],
        "service.instance.id": (
            f"{os.getenv('POD_UID') or socket.gethostname()}:"
            f"{os.getpid()}:{uuid4().hex[:12]}"
        ),
        "superset.worker_pool": pool,
        "process.pid": os.getpid(),
    }
    for env_key, key in (
        ("POD_NAME", "k8s.pod.name"),
        ("POD_UID", "k8s.pod.uid"),
        ("POD_NAMESPACE", "k8s.namespace.name"),
        ("SUPERSET_VERSION", "service.version"),
    ):
        if value := os.getenv(env_key):
            attributes[key] = value[:160]
    # Standard operator-provided resources are bounded, percent-decoded and may
    # not replace the actual PID/pool/pod writer identity. Collector promotion
    # remains separately allowlisted; these are not application metric labels.
    configured: dict[str, str | int] = {}
    for entry in os.getenv("OTEL_RESOURCE_ATTRIBUTES", "").split(",")[:32]:
        key, separator, value = entry.partition("=")
        key, value = unquote(key.strip()), unquote(value.strip())
        if (
            separator
            and re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", key)
            and len(value) <= 160
            and not any(ord(char) < 32 for char in value)
        ):
            configured[key] = value
    if "OTEL_SERVICE_NAME" not in os.environ and "service.name" in configured:
        attributes["service.name"] = configured["service.name"]
    if "service.version" in configured:
        attributes["service.version"] = configured["service.version"]
    attributes = {**configured, **attributes}
    return attributes


def resource_identity() -> dict[str, str | int]:
    """Keep event writers unique even when SDK construction is unavailable."""
    global _EVENT_RESOURCE
    if _STATE is not None and _STATE.pid == os.getpid():
        return _STATE.resource
    with _LOCK:
        if _EVENT_RESOURCE is None or _EVENT_RESOURCE.get("process.pid") != os.getpid():
            _EVENT_RESOURCE = _resource()
        return _EVENT_RESOURCE


@dataclass
class Runtime:
    """All live SDK objects belong exclusively to pid; never reuse in a child."""

    pid: int
    resource: dict[str, str | int]
    meter_provider: Any
    tracer_provider: Any
    reader: Any
    span_exporter: Any
    timeout: float
    interval: float
    queue_limit: int
    spans: deque[Any] = field(default_factory=deque)
    stop: threading.Event = field(default_factory=threading.Event)
    instruments: dict[tuple[str, str], Any] = field(default_factory=dict)
    series: dict[str, set[tuple[Any, ...]]] = field(default_factory=dict)
    overflowed_metric_observations: int = 0
    instrument_lock: threading.RLock = field(default_factory=threading.RLock)
    thread: threading.Thread | None = None
    dropped_spans: int = 0
    export_failures: int = 0
    export_successes: int = 0
    last_export: float = 0

    def enqueue(self, spans: Sequence[Any]) -> None:
        """Bound memory and never block a request on network I/O."""
        if self.pid != os.getpid() or self.stop.is_set():
            return
        with self.instrument_lock:
            for span in spans:
                if len(self.spans) >= self.queue_limit:
                    self.dropped_spans += 1
                else:
                    self.spans.append(span)

    def export_once(self, *, metrics: bool = True) -> None:
        """Export a bounded batch; failures are observable but never rethrown."""
        if self.pid != os.getpid():
            return
        try:
            with self.instrument_lock:
                spans = [self.spans.popleft() for _ in range(min(256, len(self.spans)))]
            if spans:
                from opentelemetry.sdk.trace.export import SpanExportResult

                if self.span_exporter.export(spans) != SpanExportResult.SUCCESS:
                    self.export_failures += 1
            if metrics:
                self.reader.collect(timeout_millis=self.timeout * 1000)
        except Exception:  # pylint: disable=broad-except
            self.export_failures += 1

    def run(self) -> None:
        """Serialize exports and collect process observations without extra threads."""
        deadline = time.monotonic() + self.interval
        while not self.stop.wait(1):
            collect = time.monotonic() >= deadline
            self.export_once(metrics=collect)
            if collect:
                deadline = time.monotonic() + self.interval


def _build(role: str | None) -> Runtime:  # noqa: C901
    import psutil
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
        OTLPMetricExporter,
    )
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.metrics import Observation
    from opentelemetry.sdk.metrics import (
        Counter,
        Histogram,
        MeterProvider,
        ObservableCounter,
        ObservableUpDownCounter,
        UpDownCounter,
    )
    from opentelemetry.sdk.metrics.export import (
        AggregationTemporality,
        MetricExportResult,
        PeriodicExportingMetricReader,
    )
    from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation, View
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import SpanLimits, TracerProvider
    from opentelemetry.sdk.trace.export import (
        SimpleSpanProcessor,
        SpanExporter,
        SpanExportResult,
    )
    from opentelemetry.sdk.trace.sampling import TraceIdRatioBased

    if os.getenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc") != "grpc":
        raise ValueError("Superset observability requires OTLP/gRPC")
    resource_attributes = _resource(role)
    resource = Resource(resource_attributes)
    timeout = _number("OBSERVABILITY_EXPORT_TIMEOUT_SECONDS", 2, 0.1, 10)
    interval = _number("OBSERVABILITY_METRIC_EXPORT_INTERVAL_SECONDS", 15, 1, 300)
    state_holder: list[Runtime] = []

    class HealthMetricExporter(OTLPMetricExporter):
        def export(self, *args: Any, **kwargs: Any) -> Any:
            result = super().export(*args, **kwargs)
            if state_holder:
                current = state_holder[0]
                if result == MetricExportResult.SUCCESS:
                    current.export_successes += 1
                    current.last_export = time.time()
                else:
                    current.export_failures += 1
            return result

    exporter = HealthMetricExporter(
        timeout=timeout,
        preferred_temporality={
            kind: AggregationTemporality.CUMULATIVE
            for kind in (
                Counter,
                Histogram,
                UpDownCounter,
                ObservableCounter,
                ObservableUpDownCounter,
            )
        },
    )
    reader = PeriodicExportingMetricReader(
        exporter,
        export_interval_millis=float("inf"),
        export_timeout_millis=timeout * 1000,
    )
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[reader],
        shutdown_on_exit=False,
        views=[
            View(
                instrument_type=Histogram,
                aggregation=ExplicitBucketHistogramAggregation(
                    boundaries=(
                        0.001,
                        0.005,
                        0.01,
                        0.025,
                        0.05,
                        0.1,
                        0.25,
                        0.5,
                        1,
                        2.5,
                        5,
                        10,
                        15,
                        30,
                        60,
                        120,
                        180,
                        300,
                        360,
                        600,
                        900,
                        1800,
                        3600,
                    )
                ),
            )
        ],
    )
    # The default ratio1 records all first-party spans, even when an external
    # caller supplies an unsampled traceparent. Collector-side selection can
    # change without a Superset rollout; events/metrics are always unsampled.
    tracer_provider = TracerProvider(
        resource=resource,
        sampler=TraceIdRatioBased(_number("OBSERVABILITY_TRACE_SAMPLE_RATIO", 1, 0, 1)),
        shutdown_on_exit=False,
        span_limits=SpanLimits(
            max_attributes=64, max_events=0, max_links=0, max_attribute_length=160
        ),
    )
    state = Runtime(
        os.getpid(),
        resource_attributes,
        meter_provider,
        tracer_provider,
        reader,
        OTLPSpanExporter(timeout=timeout),
        timeout,
        interval,
        int(_number("OBSERVABILITY_MAX_QUEUE_SIZE", 2048, 64, 16384)),
    )
    state_holder.append(state)

    class QueueSpanExporter(SpanExporter):
        def export(self, spans: Sequence[Any]) -> Any:
            state.enqueue(spans)
            return SpanExportResult.SUCCESS

        def shutdown(self) -> None:
            """The PID-owned runtime, not SimpleSpanProcessor, owns OTLP shutdown."""

    tracer_provider.add_span_processor(SimpleSpanProcessor(QueueSpanExporter()))
    meter = meter_provider.get_meter("superset.observability", "1")
    process = psutil.Process(os.getpid())

    def observe(getter: Any) -> Any:
        def callback(options: Any) -> list[Any]:
            if state.pid != os.getpid():
                return []
            try:
                return [Observation(getter())]
            except Exception:  # pylint: disable=broad-except
                return []

        return callback

    for name, unit, getter in (
        (
            "superset_process_resident_memory_bytes",
            "By",
            lambda: process.memory_info().rss,
        ),
        (
            "superset_process_virtual_memory_bytes",
            "By",
            lambda: process.memory_info().vms,
        ),
        ("superset_process_threads", "1", process.num_threads),
        ("superset_process_open_file_descriptors", "1", process.num_fds),
        (
            "superset_process_uptime_seconds",
            "s",
            lambda: max(0, time.time() - process.create_time()),
        ),
        ("superset_observability_span_queue_size", "1", lambda: len(state.spans)),
        (
            "superset_observability_last_export_timestamp_seconds",
            "s",
            lambda: state.last_export,
        ),
        (
            "superset_observability_event_queue_size",
            "1",
            lambda: event_writer_diagnostics()["queue_size"],
        ),
        (
            "superset_observability_event_writer_inflight",
            "1",
            lambda: event_writer_diagnostics()["inflight"],
        ),
    ):
        meter.create_observable_gauge(name, callbacks=[observe(getter)], unit=unit)
    for name, unit, getter in (
        (
            "superset_process_cpu_seconds_total",
            "s",
            lambda: sum(process.cpu_times()[:2]),
        ),
        (
            "superset_python_gc_collections_total",
            "1",
            lambda: sum(item["collections"] for item in gc.get_stats()),
        ),
        (
            "superset_observability_dropped_spans_total",
            "1",
            lambda: state.dropped_spans,
        ),
        (
            "superset_observability_metric_overflow_observations_total",
            "1",
            lambda: state.overflowed_metric_observations,
        ),
        (
            "superset_observability_export_failures_total",
            "1",
            lambda: state.export_failures,
        ),
        (
            "superset_observability_export_successes_total",
            "1",
            lambda: state.export_successes,
        ),
        (
            "superset_observability_dropped_events_total",
            "1",
            lambda: event_writer_diagnostics()["dropped"],
        ),
        (
            "superset_observability_event_write_failures_total",
            "1",
            lambda: event_writer_diagnostics()["failures"],
        ),
        (
            "superset_observability_observation_failures_total",
            "1",
            observation_failures,
        ),
    ):
        meter.create_observable_counter(name, callbacks=[observe(getter)], unit=unit)
    return state


def get_runtime(role: str | None = None) -> Runtime | None:
    """Lazily initialize once per PID; disabled/broken SDK is always fail-open."""
    global _STATE, _ATTEMPTED_PID
    if not enabled():
        return None
    pid = os.getpid()
    if _STATE is not None and _STATE.pid == pid:
        return _STATE
    with _LOCK:
        if _STATE is not None and _STATE.pid == pid:
            return _STATE
        if _ATTEMPTED_PID == pid:
            return None
        _ATTEMPTED_PID = pid
        try:
            _STATE = _build(role)
            _STATE.thread = threading.Thread(
                target=_STATE.run, name="SupersetOtelExport", daemon=True
            )
            _STATE.thread.start()
            return _STATE
        except Exception:  # pylint: disable=broad-except
            # Never print configuration, endpoint credentials or exception messages.
            return None


def init_process(role: str | None = None) -> None:
    """Start a child/beat/standalone writer without create_app or DB access."""
    get_runtime(role)


def shutdown() -> None:
    """Bounded graceful export only in the owning PID, including CLI completion."""
    global _STATE
    shutdown_event_writer()
    state = _STATE
    if state is None or state.pid != os.getpid():
        return
    state.stop.set()
    if state.thread is not None:
        state.thread.join(timeout=state.timeout + 0.1)
        if state.thread.is_alive():
            return
    with state.instrument_lock:
        while len(state.spans) > 256:
            state.spans.pop()
            state.dropped_spans += 1
    state.export_once()
    try:
        state.tracer_provider.shutdown()
        state.meter_provider.shutdown(timeout_millis=state.timeout * 1000)
        state.span_exporter.shutdown()
    except Exception:  # pylint: disable=broad-except
        record_failure()
    _STATE = None


def _after_fork() -> None:
    global _STATE, _ATTEMPTED_PID, _LOCK
    if _STATE is not None:
        _INHERITED_RUNTIMES.append(_STATE)
    _STATE = None
    _ATTEMPTED_PID = None
    _LOCK = threading.RLock()
    reset_after_fork()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork)
atexit.register(shutdown)
