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
"""Behavior/privacy tests with real SDK aggregation and local native OTLP/gRPC.

No network except loopback, no Superset app construction or metadata DB needed.
The fork test deliberately starts a writer before fork, rather than proving only
that a previously uninitialized child can start. Run via the unit pytest runner.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import weakref
from concurrent.futures import ThreadPoolExecutor
from io import StringIO
from types import SimpleNamespace
from typing import Any

import grpc
import pytest
from flask import current_app, Flask, g, has_app_context
from opentelemetry.proto.collector.metrics.v1 import (
    metrics_service_pb2,
    metrics_service_pb2_grpc,
)
from opentelemetry.proto.collector.trace.v1 import (
    trace_service_pb2,
    trace_service_pb2_grpc,
)
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool, QueuePool

from superset.observability import (
    emit_event,
    event_writer,
    events,
    gauge,
    get_context,
    increment,
    instrument_engine,
    is_enabled,
    operation,
    restore_context,
    runtime,
    signed_execution_headers,
    telemetry_context,
    wire_context,
)
from superset.observability.context import sanitize
from superset.observability.http import EXECUTION_HEADER, register_http
from superset.observability.metrics import metric_attributes
from superset.observability.stats import ObservabilityStatsLogger
from superset.stats_logger import DummyStatsLogger
from superset.utils import json


class CapturedEventStream(StringIO):
    """Observe asynchronously delivered JSON only after a bounded barrier."""

    def getvalue(self) -> str:
        assert event_writer.flush(timeout=2), "event writer did not drain"
        return super().getvalue()


class Receiver(
    metrics_service_pb2_grpc.MetricsServiceServicer,
    trace_service_pb2_grpc.TraceServiceServicer,
):
    def __init__(self) -> None:
        self.metrics: list[Any] = []
        self.traces: list[Any] = []

    def Export(self, request: Any, context: Any) -> Any:  # noqa: N802
        if isinstance(request, metrics_service_pb2.ExportMetricsServiceRequest):
            self.metrics.append(request)
            return metrics_service_pb2.ExportMetricsServiceResponse()
        self.traces.append(request)
        return trace_service_pb2.ExportTraceServiceResponse()


@pytest.fixture
def collector() -> Any:
    receiver = Receiver()
    server = grpc.server(ThreadPoolExecutor(max_workers=2))
    metrics_service_pb2_grpc.add_MetricsServiceServicer_to_server(receiver, server)
    trace_service_pb2_grpc.add_TraceServiceServicer_to_server(receiver, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    yield receiver, port
    server.stop(0).wait(5)


@pytest.fixture
def telemetry(monkeypatch: Any, collector: Any) -> Any:
    receiver, port = collector
    if has_app_context():
        monkeypatch.setitem(current_app.config, "OBSERVABILITY_ENABLED", True)
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", f"http://127.0.0.1:{port}")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
    monkeypatch.setenv("SUPERSET_OBSERVABILITY_POOL", "queries")
    monkeypatch.setenv("POD_UID", "test-pod")
    monkeypatch.setattr(
        runtime,
        "_CONFIG",
        {
            "OBSERVABILITY_ENABLED": True,
            "OBSERVABILITY_EXPORT_TIMEOUT_SECONDS": 0.1,
            "OBSERVABILITY_METRIC_EXPORT_INTERVAL_SECONDS": 300,
        },
    )
    state = runtime._build("queries")
    monkeypatch.setattr(runtime, "_STATE", state)
    monkeypatch.setattr(runtime, "_ATTEMPTED_PID", os.getpid())
    event_writer.shutdown()
    stream = CapturedEventStream()
    monkeypatch.setattr(event_writer, "_WRITER", None)
    monkeypatch.setattr(event_writer, "_SINK", stream)
    monkeypatch.setattr(event_writer, "_MAX_QUEUE_SIZE", 2048)
    monkeypatch.setattr(event_writer, "_FAILED_ENQUEUES", 0)
    with restore_context(None):
        yield state, receiver, stream
    runtime.shutdown()


def metric_map(receiver: Receiver) -> dict[str, Any]:
    return {
        metric.name: metric
        for request in receiver.metrics
        for resource in request.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
    }


def attribute_dict(attributes: Any) -> dict[str, Any]:
    return {
        item.key: getattr(item.value, item.value.WhichOneof("value"))
        for item in attributes
    }


def test_context_restores_and_drops_payloads() -> None:
    class Private:
        def __str__(self) -> str:
            raise AssertionError("telemetry must not stringify objects")

    with restore_context(None):
        with telemetry_context(
            source="chart",
            chart_id=12,
            snowflake_query_id="old",
            sql="SELECT private",
            filters={"ssn": "private"},
            error_type=Private(),
        ):
            assert get_context() == {
                "source": "chart",
                "chart_id": 12,
                "snowflake_query_id": "old",
            }
            snapshot = get_context()
            snapshot["chart_id"] = 99
            with pytest.raises(ValueError, match="private failure"):  # noqa: PT012
                with telemetry_context(chart_id=13, snowflake_query_id=None):
                    assert get_context()["snowflake_query_id"] is None
                    raise ValueError("private failure")
            assert get_context()["chart_id"] == 12
        assert get_context() == {}
    assert sanitize(
        {
            "rows": float("nan"),
            "request_id": "https://private/token",
            "chart_ids": list(range(100)),
        }
    ) == {"chart_ids": tuple(range(64))}


def test_operation_exports_native_cumulative_metrics_and_trace_without_payloads(
    telemetry: Any,
) -> None:
    state, receiver, stream = telemetry
    error = ValueError("SELECT credit_card FROM secret WHERE token='private'")
    with telemetry_context(
        source="chart", request_id="request-1", request_actor_user_id=42, chart_id=7
    ):
        with pytest.raises(ValueError, match=re.escape(str(error))) as caught:  # noqa: PT012
            with operation(
                "sql.execute",
                db_system="snowflake",
                sql="SELECT secret",
                error_message="private",
            ) as scope:
                scope.set_attribute("snowflake_query_id", "sf-query-1")
                scope.set_attribute("rows", 3)
                raise error
        assert caught.value is error
        scope.__exit__(None, None, None)
    state.export_once()
    metrics = metric_map(receiver)
    counter = metrics["superset_operations_total"].sum
    histogram = metrics["superset_operation_duration_seconds"].histogram
    assert counter.aggregation_temporality == 2
    assert counter.is_monotonic
    assert sum(point.as_int or point.as_double for point in counter.data_points) == 1
    assert histogram.aggregation_temporality == 2
    assert sum(point.count for point in histogram.data_points) == 1
    assert histogram.data_points[0].explicit_bounds[0] == 0.001
    assert histogram.data_points[0].explicit_bounds[-1] == 3600
    for metric in (counter, histogram):
        for point in metric.data_points:
            assert (
                not {
                    "request_id",
                    "request_actor_user_id",
                    "chart_id",
                    "snowflake_query_id",
                    "sql",
                }
                & attribute_dict(point.attributes).keys()
            )
    spans = [
        span
        for request in receiver.traces
        for resource in request.resource_spans
        for scope in resource.scope_spans
        for span in scope.spans
    ]
    assert len(spans) == 1
    assert spans[0].name == "sql.execute"
    assert attribute_dict(spans[0].attributes)["snowflake_query_id"] == "sf-query-1"
    assert not spans[0].events  # No exception message/stacktrace auto-recording.
    records = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert [record["message"] for record in records] == [
        "sql.execute.started",
        "sql.execute.finished",
    ]
    assert records[-1]["metadata"]["outcome"] == "error"
    assert records[-1]["metadata"]["error_type"] == "ValueError"
    assert records[-1]["metadata"]["traceId"] == spans[0].trace_id.hex()
    encoded = stream.getvalue() + str(receiver.metrics) + str(receiver.traces)
    assert "SELECT" not in encoded
    assert "private" not in encoded
    assert "credit_card" not in encoded
    with operation("sql.execute", db_system="snowflake"):
        pass
    state.export_once()
    assert (
        sum(
            point.as_int or point.as_double
            for point in metric_map(receiver)[
                "superset_operations_total"
            ].sum.data_points
        )
        == 2
    )


def test_explicit_outcome_and_logger_ignores_root_error_level(
    telemetry: Any, monkeypatch: Any
) -> None:
    _, _, stream = telemetry
    monkeypatch.setattr(logging.getLogger(), "level", logging.ERROR)
    with operation("celery.task") as scope:
        scope.set_attribute("outcome", "retry")
    emit_event(
        "warehouse.query.enriched",
        event_id="warehouse-query-v1-stable",
        snowflake_query_id="native-id",
    )
    records = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert records[1]["metadata"]["outcome"] == "retry"
    assert records[-1]["metadata"]["event_id"] == "warehouse-query-v1-stable"
    assert events._LOGGER.propagate is False


def test_identifiers_do_not_grow_metric_series_and_unknown_instruments_are_rejected(
    telemetry: Any,
) -> None:
    state, receiver, _ = telemetry
    for identifier in range(500):
        increment(
            "task.relationships",
            consumer_action="join",
            request_actor_user_id=identifier,
            task_id=f"task-{identifier}",
            chart_id=identifier,
        )
        gauge(
            "warehouse.own_queries.window_count",
            identifier,
            source="observability",
            db_system="snowflake",
            metric_scope="own_user",
            window_seconds=900,
        )
        increment(f"user_{identifier}")
    state.export_once()
    metrics = metric_map(receiver)
    points = metrics["superset_task_relationships_total"].sum.data_points
    assert len(points) == 1
    assert points[0].as_int == 500
    assert not any(name.startswith("user_") for name in metrics)
    assert attribute_dict(points[0].attributes) == {"consumer_action": "join"}
    assert (
        attribute_dict(
            metrics["superset_warehouse_own_queries_window_count"]
            .gauge.data_points[0]
            .attributes
        )["window_seconds"]
        == 900
    )
    assert metric_attributes(
        {"queue_name": "private-customer-42", "task_type": "private-user"}
    ) == {"otel.metric.overflow": True, "task_type": "other"}
    assert len(state.series["superset_task_relationships_total"]) == 1


def test_series_and_span_queues_are_bounded(telemetry: Any) -> None:
    state, receiver, _ = telemetry
    state.queue_limit = 2
    with operation("sql.execute"):
        pass
    with operation("sql.execute"):
        pass
    with operation("sql.execute"):
        pass
    assert len(state.spans) == 2
    assert state.dropped_spans == 1
    from superset.observability.metrics import _DIMENSIONS

    for source in _DIMENSIONS["source"]:
        for outcome in _DIMENSIONS["outcome"]:
            for phase in ("execute", "fetch"):
                increment(
                    "task.transitions", source=source, outcome=outcome, phase=phase
                )
    assert len(state.series["superset_task_transitions_total"]) == 256
    assert state.overflowed_metric_observations > 0
    state.export_once()
    points = metric_map(receiver)["superset_task_transitions_total"].sum.data_points
    expected = len(_DIMENSIONS["source"]) * len(_DIMENSIONS["outcome"]) * 2
    assert sum(point.as_int or point.as_double for point in points) == expected
    assert any(
        attribute_dict(point.attributes) == {"otel.metric.overflow": True}
        for point in points
    )


def test_legacy_decrement_is_not_a_negative_monotonic_counter(telemetry: Any) -> None:
    state, receiver, _ = telemetry
    adapter = ObservabilityStatsLogger(DummyStatsLogger())
    adapter.incr("health")
    adapter.decr("health")
    adapter.timing("sqllab.query.time_pending", 125)
    adapter.gauge("health", 7)
    state.export_once()
    metrics = metric_map(receiver)
    assert metrics["superset_legacy_events_total"].sum.data_points[0].as_int == 1
    assert attribute_dict(
        metrics["superset_legacy_events_total"].sum.data_points[0].attributes
    ) == {"legacy_key": "health"}
    balance = metrics["superset_legacy_balance"].sum
    assert not balance.is_monotonic
    assert balance.data_points[0].as_int == 0
    assert (
        metrics["superset_legacy_duration_seconds"].histogram.data_points[0].sum
        == 0.125
    )
    assert metrics["superset_legacy_value"].gauge.data_points[0].as_int == 7


def test_http_signed_context_actor_and_error_response_correlation(
    telemetry: Any,
) -> None:
    app = Flask(__name__)
    app.secret_key = "independent-test-signing-key"  # noqa: S105

    @app.before_request
    def authenticated_user() -> None:
        g.user = SimpleNamespace(id=71)

    register_http(app)

    @app.get("/context")
    def context_route() -> dict[str, Any]:
        return get_context()

    @app.get("/failed")
    def failed_route() -> tuple[str, int]:
        return "failure", 500

    with (
        app.app_context(),
        telemetry_context(
            source="report",
            report_schedule_id=5,
            request_actor_user_id=999,
            effective_user_id=999,
        ),
    ):
        headers = signed_execution_headers()
    client = app.test_client()
    response = client.get(
        "/context",
        headers={
            **headers,
            "X-Superset-Actor-User-Id": "999",
            "X-Superset-Request-Id": "attacker",
        },
    )
    result = response.get_json()
    assert result["source"] == "report"
    assert result["report_schedule_id"] == 5
    assert result["request_actor_user_id"] == 999
    assert result["effective_user_id"] == 71
    assert (
        response.headers["X-Superset-Request-Id"] == result["request_id"] != "attacker"
    )
    rejected = client.get(
        "/context", headers={EXECUTION_HEADER: headers[EXECUTION_HEADER] + "forged"}
    ).get_json()
    assert rejected["source"] == "system"
    assert "report_schedule_id" not in rejected
    assert rejected["request_actor_user_id"] == rejected["effective_user_id"] == 71
    with (
        app.app_context(),
        telemetry_context(source="report", request_actor_user_id=None),
    ):
        scheduled_headers = signed_execution_headers()
    scheduled = client.get("/context", headers=scheduled_headers).get_json()
    assert scheduled["request_actor_user_id"] is None
    assert scheduled["effective_user_id"] == 71
    assert client.get("/failed").headers["X-Superset-Request-Id"]
    assert get_context() == {}


def test_trusted_wire_context_restores_parent_without_payload(telemetry: Any) -> None:
    from opentelemetry import trace

    with (
        operation("celery.task"),
        telemetry_context(source="warmup", warmup_run_id="warmup-1", sql="private"),
    ):
        parent = trace.get_current_span().get_span_context()
        serialized = wire_context()
    assert "sql" not in serialized
    assert "traceparent" in serialized
    with restore_context(serialized), operation("sql.execute"):
        current = trace.get_current_span().get_span_context()
        assert current.trace_id == parent.trace_id
        assert get_context()["warmup_run_id"] == "warmup-1"
    assert get_context() == {}


def test_engine_health_preserves_transaction_error_and_weak_ownership(
    telemetry: Any,
) -> None:
    state, receiver, stream = telemetry
    engine = create_engine("sqlite://", poolclass=QueuePool)
    instrument_engine(engine, role="warehouse", database_id=9)
    instrument_engine(engine, role="warehouse", database_id=9)
    with engine.begin() as connection:
        connection.execute(text("create table sensitive_table (value integer)"))
        connection.execute(text("insert into sensitive_table values (7)"))
    with engine.connect() as connection:
        with pytest.raises(OperationalError):
            connection.execute(
                text("SELECT private_secret FROM nonexistent_private_table")
            )
        connection.rollback()
        assert connection.scalar(text("select value from sensitive_table")) == 7
        connection.invalidate()
    state.export_once()
    metrics = metric_map(receiver)
    assert metrics["superset_db_connections_total"].sum.data_points[0].as_int == 1
    assert metrics["superset_db_invalidations_total"].sum.data_points[0].as_int == 1
    assert "private_secret" not in stream.getvalue()
    assert "nonexistent_private_table" not in stream.getvalue()
    reference = weakref.ref(engine)
    engine.dispose()
    del connection
    del engine
    import gc

    gc.collect()
    assert reference() is None


def test_null_pool_does_not_report_fake_utilization(telemetry: Any) -> None:
    state, receiver, _ = telemetry
    engine = create_engine("sqlite://", poolclass=NullPool)
    instrument_engine(engine, role="warehouse")
    with engine.connect() as connection:
        assert connection.scalar(text("select 1")) == 1
    state.export_once()
    assert not any(
        name.startswith("superset_db_pool_") for name in metric_map(receiver)
    )
    engine.dispose()


def test_disabled_sdk_never_initializes_or_changes_exception(monkeypatch: Any) -> None:
    monkeypatch.setattr(runtime, "_CONFIG", {"OBSERVABILITY_ENABLED": False})

    def forbidden_build(role: Any) -> Any:
        raise AssertionError("disabled instrumentation must never build an SDK")

    monkeypatch.setattr(runtime, "_build", forbidden_build)
    assert not is_enabled()
    expected = RuntimeError("private")
    with pytest.raises(RuntimeError) as caught:  # noqa: PT012
        with operation("sql.execute"):
            increment("task.relationships")
            emit_event("task.created")
            raise expected
    assert caught.value is expected
    assert runtime.get_runtime() is None


def test_broken_sdk_and_collector_preserve_results(
    monkeypatch: Any, telemetry: Any
) -> None:
    state, _, _ = telemetry

    def failed_export(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("collector unavailable private config")

    monkeypatch.setattr(state.span_exporter, "export", failed_export)
    with operation("sql.execute"):
        result = 42
    state.export_once()
    assert result == 42
    assert state.export_failures >= 1
    monkeypatch.setattr(runtime, "_STATE", None)
    monkeypatch.setattr(runtime, "_ATTEMPTED_PID", None)
    monkeypatch.setattr(runtime, "_build", failed_export)
    with operation("sql.execute"):
        result = 43
    assert result == 43
    assert runtime.get_runtime() is None
    # Restore fixture-owned state so its exporters are closed during teardown.
    monkeypatch.setattr(runtime, "_STATE", state)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="POSIX fork contract")
def test_fork_discards_initialized_parent_exporters_records_and_context(
    telemetry: Any,
) -> None:
    state, _, _ = telemetry
    state.thread = threading.Thread(
        target=state.run, name="SupersetOtelExport", daemon=True
    )
    state.thread.start()
    with (
        telemetry_context(request_id="parent-private-request"),
        operation("sql.execute"),
    ):
        pass
    parent_identity = state.resource["service.instance.id"]
    read_fd, write_fd = os.pipe()
    with (
        telemetry_context(parent_task_id="parent-outer-task"),
        telemetry_context(request_id="parent-in-flight-request"),
        operation("http.request"),
    ):
        pid = os.fork()
    if pid == 0:
        os.close(read_fd)
        try:
            from google.protobuf.json_format import MessageToDict
            from opentelemetry.exporter.otlp.proto.common.trace_encoder import (
                encode_spans,
            )

            clean = (
                runtime._STATE is None
                and event_writer._WRITER is None
                and get_context() == {}
            )
            child = runtime.get_runtime("queries")
            with child.instrument_lock:
                with operation("sql.fetch"):
                    pass
                payload = MessageToDict(encode_spans(list(child.spans)))
            # Collect real SDK aggregation without doing network I/O from a
            # fork child. Native OTLP serialization/export has separate tests.
            collected = child.reader._collect(child.reader, timeout_millis=100)
            scopes = {
                metric.name: [
                    {"value": point.value, "attributes": dict(point.attributes or {})}
                    for point in metric.data.data_points
                ]
                for resource in collected.resource_metrics
                for instrumentation in resource.scope_metrics
                for metric in instrumentation.metrics
                if metric.name.startswith("superset_operation_scopes_")
            }
            scope_resources = [
                resource.resource.attributes["service.instance.id"]
                for resource in collected.resource_metrics
            ]
            result = {
                "clean": clean,
                "pid": child.pid,
                "identity": child.resource["service.instance.id"],
                "payload": payload,
                "scopes": scopes,
                "scope_resources": scope_resources,
                "threads": [thread.name for thread in threading.enumerate()],
            }
            os.write(write_fd, json.dumps(result).encode())
        except BaseException as error:
            os.write(
                write_fd, json.dumps({"child_error": type(error).__name__}).encode()
            )
        finally:
            os.close(write_fd)
            os._exit(0)
    os.close(write_fd)
    chunks = []
    while chunk := os.read(read_fd, 65536):
        chunks.append(chunk)
    payload = b"".join(chunks)
    os.close(read_fd)
    _, status = os.waitpid(pid, 0)
    assert status == 0
    child_result = json.loads(payload)
    assert "child_error" not in child_result
    assert child_result["clean"]
    assert child_result["pid"] == pid
    assert child_result["identity"] != parent_identity
    assert "parent-private-request" not in payload.decode()
    assert "parent-in-flight-request" not in payload.decode()
    names = [
        span["name"]
        for resource in child_result["payload"]["resourceSpans"]
        for scope in resource["scopeSpans"]
        for span in scope["spans"]
    ]
    assert names == ["sql.fetch"]
    assert child_result["threads"].count("SupersetOtelExport") == 1
    assert child_result["threads"].count("SupersetEventWriter") == 1
    assert set(child_result["scope_resources"]) == {child_result["identity"]}
    assert [
        point["value"]
        for point in child_result["scopes"]["superset_operation_scopes_started_total"]
    ] == [1]
    assert [
        point["value"]
        for point in child_result["scopes"]["superset_operation_scopes_completed_total"]
    ] == [1]
    assert [
        point["value"]
        for point in child_result["scopes"]["superset_operation_scopes_inflight"]
    ] == [0]
    assert all(
        point["attributes"] == {"operation": "sql.fetch"}
        for points in child_result["scopes"].values()
        for point in points
    )
    assert not any(
        name.startswith("OtelBatch") or name.startswith("OtelPeriodic")
        for name in child_result["threads"]
    )
    assert runtime._STATE is state
    assert state.resource["service.instance.id"] == parent_identity


def test_loaded_identity_never_resolves_auth_proxy_or_guest_claims() -> None:
    from werkzeug.local import LocalProxy

    app = Flask(__name__)
    app.secret_key = "guest-pseudonym-test-key"  # noqa: S105

    def forbidden_auth_load() -> Any:
        raise AssertionError("observing public requests must not trigger auth")

    with app.test_request_context(), restore_context(None):
        g.user = LocalProxy(forbidden_auth_load)
        assert get_context() == {}
        g._login_user = SimpleNamespace(id=83)
        assert get_context() == {"request_actor_user_id": 83, "effective_user_id": 83}
        with telemetry_context(request_actor_user_id=None):
            assert get_context() == {
                "request_actor_user_id": None,
                "effective_user_id": 83,
            }

        class Guest:
            is_guest_user = True

        guest = Guest()
        guest.username = "guest-person@example.test"
        g.user = guest
        identity = get_context()
        assert len(identity["guest_actor_id"]) == 64
        assert "guest-person" not in json.dumps(identity)
        assert "request_actor_user_id" not in identity


def test_background_impersonation_does_not_create_a_request_actor() -> None:
    app = Flask(__name__)
    with app.app_context(), restore_context(None):
        g.user = SimpleNamespace(id=83)
        assert get_context() == {}
        with telemetry_context(source="report", effective_user_id=83):
            assert get_context() == {"source": "report", "effective_user_id": 83}


def test_asset_and_configured_queue_gauges_do_not_collapse(
    telemetry: Any, monkeypatch: Any
) -> None:
    from superset.observability.metrics import _DIMENSIONS, configure_metric_queues

    state, receiver, _ = telemetry
    monkeypatch.setitem(_DIMENSIONS, "queue_name", _DIMENSIONS["queue_name"])
    configure_metric_queues(["custom.fast", "custom.slow"])
    for warehouse, database, count in (("DASH WH", 1, 3), ("REPORT_WH", 2, 7)):
        gauge(
            "warehouse.own_queries.window_count",
            count,
            warehouse_name=warehouse,
            database_id=database,
            metric_scope="own_user",
            window_seconds=900,
        )
    gauge("celery.queue.depth", 2, queue_name="custom.fast", state="ready")
    gauge("celery.queue.depth", 9, queue_name="custom.slow", state="ready")
    state.export_once()
    metrics = metric_map(receiver)
    points = metrics["superset_warehouse_own_queries_window_count"].gauge.data_points
    assert {
        (
            attribute_dict(point.attributes)["warehouse_name"],
            attribute_dict(point.attributes)["database_id"],
            point.as_int,
        )
        for point in points
    } == {("DASH WH", 1, 3), ("REPORT_WH", 2, 7)}
    queues = metrics["superset_celery_queue_depth"].gauge.data_points
    assert {
        (attribute_dict(point.attributes)["queue_name"], point.as_int)
        for point in queues
    } == {("custom.fast", 2), ("custom.slow", 9)}


def test_legacy_builtin_keys_remain_distinct_and_arbitrary_keys_are_bounded(
    telemetry: Any,
) -> None:
    from superset.observability.legacy import canonical_legacy_key

    state, receiver, _ = telemetry
    adapter = ObservabilityStatsLogger(DummyStatsLogger())
    keys = (
        "sqllab.query.time_executing_query",
        "sqllab.query.results_backend_write_serialization",
        "ChartRestApi.get.time",
        "DatabaseRestApi.select_star.time",
        "Database.load_into_dataframe",
    )
    for key in keys:
        assert canonical_legacy_key(key) == key
        adapter.timing(key, 1000)
    for identifier in range(1000):
        adapter.incr(f"private-user-{identifier}")
    state.export_once()
    metrics = metric_map(receiver)
    timings = metrics["superset_legacy_duration_seconds"].histogram.data_points
    assert {attribute_dict(point.attributes)["legacy_key"] for point in timings} == set(
        keys
    )
    assert all(point.sum == 1 for point in timings)
    counters = metrics["superset_legacy_events_total"].sum.data_points
    assert len(counters) == 1
    assert counters[0].as_int == 1000
    assert attribute_dict(counters[0].attributes) == {"legacy_key": "other"}


def test_standalone_process_is_env_only_and_idempotent(collector: Any) -> None:
    import subprocess
    import sys

    _, port = collector
    environment = os.environ.copy()
    environment.update(
        SUPERSET_OBSERVABILITY_ENABLED="true",
        SUPERSET_OBSERVABILITY_POOL="warehouse-enricher",
        SUPERSET_OBSERVABILITY_EXPORT_TIMEOUT_SECONDS="0.1",
        OTEL_EXPORTER_OTLP_ENDPOINT=f"http://127.0.0.1:{port}",
        OTEL_EXPORTER_OTLP_PROTOCOL="grpc",
    )
    code = """
import json, os
import superset
def forbidden(*args, **kwargs):
    raise AssertionError('standalone telemetry cannot construct Flask or sync DB')
superset.create_app = forbidden
from superset.observability import init_process, is_enabled
from superset.observability import runtime
init_process(role='warehouse-enricher')
first = runtime.get_runtime()
init_process(role='warehouse-enricher')
assert first is runtime.get_runtime()
assert first.pid == os.getpid()
print(json.dumps({
    'enabled': is_enabled(), 'pool': first.resource['superset.worker_pool']
}))
"""
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", code],
        env=environment,
        capture_output=True,
        text=True,
        check=True,
        timeout=20,
    )
    assert json.loads(result.stdout) == {"enabled": True, "pool": "warehouse-enricher"}


def test_nonfinite_or_oversized_numbers_cannot_change_caller_results(
    telemetry: Any,
) -> None:
    with telemetry_context(rows=10**1000, duration_seconds=float("inf")):
        assert "rows" not in get_context()
        assert "duration_seconds" not in get_context()
    with operation("sql.execute", result_bytes=10**1000):
        gauge("task.backlog", 10**1000)
        increment("task.transitions", 10**1000)
        result = 17
    assert result == 17


def test_standard_resource_attributes_cannot_replace_writer_identity(
    monkeypatch: Any,
) -> None:
    digest = "sha256:" + "a" * 64
    monkeypatch.setenv("SUPERSET_VERSION", "generic-library-release")
    monkeypatch.setenv(
        "OTEL_RESOURCE_ATTRIBUTES",
        f"deployment.environment=test,service.namespace=data%20platform,custom.component=queries,service.instance.id=forged,process.pid=999,superset.worker_pool=forged,service.version={digest}",
    )
    resource = runtime._resource("queries")
    assert resource["service.version"] == digest
    assert resource["service.namespace"] == "data platform"
    assert resource["custom.component"] == "queries"
    assert resource["process.pid"] == os.getpid()
    assert resource["superset.worker_pool"] == "queries"
    assert resource["service.instance.id"] != "forged"


def test_unsampled_external_parent_does_not_disable_first_party_spans(
    telemetry: Any,
) -> None:
    state, receiver, stream = telemetry
    trace_id = "0123456789abcdef0123456789abcdef"
    with (
        restore_context({"traceparent": f"00-{trace_id}-0123456789abcdef-00"}),
        operation("sql.execute"),
    ):
        pass
    state.export_once()
    spans = [
        span
        for request in receiver.traces
        for resource in request.resource_spans
        for scope in resource.scope_spans
        for span in scope.spans
    ]
    assert len(spans) == 1
    assert spans[0].trace_id.hex() == trace_id
    assert (
        metric_map(receiver)["superset_operations_total"].sum.data_points[0].as_int == 1
    )
    assert len(stream.getvalue().splitlines()) == 2


def test_legacy_logger_without_prefix_and_delegate_errors_are_preserved(
    telemetry: Any,
) -> None:
    from superset.stats_logger import BaseStatsLogger

    expected = ValueError("existing delegate failure")

    class LegacyWithoutPrefix(BaseStatsLogger):
        def __init__(self) -> None:
            self.values: list[float] = []

        def timing(self, key: str, value: float) -> None:
            self.values.append(value)

        def incr(self, key: str) -> None:
            raise expected

    delegate = LegacyWithoutPrefix()
    adapter = ObservabilityStatsLogger(delegate)
    adapter.timing("sqllab.query.time_pending", 125)
    assert delegate.values == [125]
    with pytest.raises(ValueError, match=re.escape(str(expected))) as caught:
        adapter.incr("health")
    assert caught.value is expected


def test_event_writer_identity_survives_unavailable_sdk(
    telemetry: Any, monkeypatch: Any
) -> None:
    _, _, stream = telemetry
    monkeypatch.setattr(runtime, "_STATE", None)
    monkeypatch.setattr(runtime, "_ATTEMPTED_PID", os.getpid())
    monkeypatch.setattr(runtime, "_EVENT_RESOURCE", None)
    emit_event("task.created")
    emit_event("task.created")
    records = [json.loads(line)["metadata"] for line in stream.getvalue().splitlines()]
    assert records[0]["service_instance_id"] == records[1]["service_instance_id"]
    assert records[0]["service_instance_id"].startswith(f"test-pod:{os.getpid()}:")
    # Restore fixture-owned providers so teardown closes only owned channels.
    monkeypatch.setattr(runtime, "_STATE", telemetry[0])


def test_full_stdout_pipe_cannot_block_business_or_native_exports(
    telemetry: Any, monkeypatch: Any
) -> None:
    import time

    state, receiver, _ = telemetry
    read_fd, write_fd = os.pipe()
    os.set_blocking(write_fd, False)
    try:
        while True:
            os.write(write_fd, b"x" * 65536)
    except BlockingIOError:
        pass
    os.set_blocking(write_fd, True)
    output = os.fdopen(write_fd, "w", buffering=1)
    entered = threading.Event()

    class ObservedPipe:
        def write(self, value: str) -> int:
            entered.set()
            return output.write(value)

        def flush(self) -> None:
            output.flush()

    monkeypatch.setattr(event_writer, "_SINK", ObservedPipe())
    monkeypatch.setattr(event_writer, "_MAX_QUEUE_SIZE", 2)
    completed = threading.Event()
    results = []

    def business() -> None:
        with operation("sql.execute"):
            results.append(42)
        completed.set()

    caller = threading.Thread(target=business)
    try:
        emit_event("task.created")
        assert entered.wait(2), "event writer never reached the full OS pipe"
        caller.start()
        assert completed.wait(1), "business operation waited on stdout backpressure"
        assert results == [42]
        emit_event("task.joined")
        health = event_writer.diagnostics()
        assert health["inflight"] == 1
        assert health["queue_size"] == 2
        assert health["dropped"] >= 1
        assert health["failures"] == 0
        state.export_once()
        metrics = metric_map(receiver)
        assert metrics["superset_operations_total"].sum.data_points[0].as_int == 1
        assert (
            metrics["superset_observability_event_queue_size"]
            .gauge.data_points[0]
            .as_int
            == 2
        )
        assert (
            metrics["superset_observability_event_writer_inflight"]
            .gauge.data_points[0]
            .as_int
            == 1
        )
        assert (
            metrics["superset_observability_dropped_events_total"]
            .sum.data_points[0]
            .as_int
            >= 1
        )
        assert (
            metrics["superset_observability_dropped_spans_total"]
            .sum.data_points[0]
            .as_int
            == 0
        )
        started = time.monotonic()
        event_writer.shutdown(timeout=0.02)
        assert time.monotonic() - started < 0.5
        assert event_writer.diagnostics()["queue_size"] == 0
        assert event_writer.diagnostics()["inflight"] == 1
    finally:
        # Release the real blocked write without waiting on another stdout
        # write or closing a stream whose daemon still owns its internal lock.
        os.set_blocking(read_fd, False)
        try:
            while os.read(read_fd, 65536):
                pass
        except BlockingIOError:
            pass
        if caller.ident is not None:
            caller.join(timeout=2)
        event_writer.shutdown(timeout=2)
        output.close()
        os.close(read_fd)


def test_stdout_failures_preserve_original_exception_and_have_separate_health(
    telemetry: Any, monkeypatch: Any
) -> None:
    state, receiver, stream = telemetry

    class FailedOutput:
        def write(self, value: str) -> int:
            raise OSError("private stdout error")

        def flush(self) -> None:
            raise AssertionError("write failure must not attempt a second I/O")

    monkeypatch.setattr(event_writer, "_SINK", FailedOutput())
    expected = ValueError("original business failure")
    with pytest.raises(ValueError, match=re.escape(str(expected))) as caught:
        with operation("sql.execute"):
            raise expected
    assert caught.value is expected
    assert event_writer.flush(timeout=2)
    state.export_once()
    metrics = metric_map(receiver)
    assert (
        metrics["superset_observability_event_write_failures_total"]
        .sum.data_points[0]
        .as_int
        == 2
    )
    assert (
        metrics["superset_observability_dropped_events_total"].sum.data_points[0].as_int
        == 0
    )
    assert (
        metrics["superset_observability_export_failures_total"]
        .sum.data_points[0]
        .as_int
        == 0
    )
    assert stream.getvalue() == ""


@pytest.mark.asyncio
async def test_registered_mcp_tools_keep_distinct_native_metrics(
    telemetry: Any, monkeypatch: Any
) -> None:
    from fastmcp import FastMCP

    from superset.extensions import stats_logger_manager
    from superset.mcp_service.middleware import LoggingMiddleware
    from superset.observability import legacy

    state, receiver, _ = telemetry
    monkeypatch.setattr(legacy, "_MCP_TOOLS", frozenset({"unknown", "call_tool"}))
    monkeypatch.setattr(
        stats_logger_manager,
        "_stats_logger",
        ObservabilityStatsLogger(DummyStatsLogger()),
    )
    registry = FastMCP("observability-metric-registry")

    @registry.tool(name="query_dataset")
    def query_dataset() -> str:
        return "registered tool"

    @registry.tool(name="extensions.analytics.lookup")
    def lookup() -> str:
        return "registered extension tool"

    context = SimpleNamespace(fastmcp_context=SimpleNamespace(fastmcp=registry))
    middleware = LoggingMiddleware()
    for name, success, user_error, duration in (
        ("query_dataset", True, False, 125),
        ("query_dataset", False, True, 250),
        ("query_dataset", False, False, 500),
        ("extensions.analytics.lookup", True, False, 1000),
    ):
        await middleware._emit_call_metrics(
            context,
            name,
            None,
            success=success,
            raised_is_user_error=user_error,
            duration_ms=duration,
        )
    for identifier in range(50):
        await middleware._emit_call_metrics(
            context,
            f"unregistered_client_{identifier}",
            None,
            success=False,
            raised_is_user_error=False,
            duration_ms=10,
        )
        assert (
            legacy.canonical_legacy_key(
                f"mcp.tool.unregistered_client_{identifier}.error"
            )
            == "other"
        )
    state.export_once()
    metrics = metric_map(receiver)
    counters = {
        attribute_dict(point.attributes)["legacy_key"]: point.as_int
        for point in metrics["superset_legacy_events_total"].sum.data_points
    }
    assert counters == {
        "mcp.tool.query_dataset.success": 1,
        "mcp.tool.query_dataset.warning": 1,
        "mcp.tool.query_dataset.error": 1,
        "mcp.tool.extensions.analytics.lookup.success": 1,
        "mcp.tool.unknown.error": 50,
    }
    durations = {
        attribute_dict(point.attributes)["legacy_key"]: point
        for point in metrics["superset_legacy_duration_seconds"].histogram.data_points
    }
    assert durations["mcp.tool.query_dataset.time"].count == 3
    assert durations["mcp.tool.query_dataset.time"].sum == 0.875
    assert durations["mcp.tool.extensions.analytics.lookup.time"].sum == 1
    assert len(legacy._MCP_TOOLS) == 4


def test_celery_timeline_context_and_supplied_durations_remain_exact(
    telemetry: Any,
) -> None:
    from superset.observability import observe_duration

    state, receiver, _ = telemetry
    submitted = "2026-11-03T01:00:00+00:00"
    published = "2026-11-03T01:00:02+00:00"
    received = "2026-11-03T01:00:02.500000+00:00"
    with telemetry_context(
        submitted_at=submitted,
        published_at=published,
        received_at=received,
        celery_task_id="attempt-1",
        task_id="task-1",
        source="chart",
    ):
        propagated = wire_context()
        assert propagated["submitted_at"] == submitted
        assert propagated["published_at"] == published
        assert propagated["received_at"] == received
        with telemetry_context(published_at="2026-11-03T01:00:05+00:00"):
            assert get_context()["submitted_at"] == submitted
            assert get_context()["published_at"] != published
        for name, phase, duration in (
            ("celery.broker_wait", "broker", 0.5),
            ("celery.worker_wait", "worker", 0.25),
            ("celery.dispatch_wait", "queue", 0.75),
        ):
            observe_duration(
                name, duration, phase=phase, outcome="success", queue_name="queries"
            )
        observe_duration("celery.broker_wait", None, phase="broker")
        observe_duration("celery.worker_wait", -1, phase="worker")
    state.export_once()
    metrics = metric_map(receiver)
    points = metrics["superset_operation_duration_seconds"].histogram.data_points
    durations = {
        attribute_dict(point.attributes)["operation"]: point.sum for point in points
    }
    assert durations == {
        "celery.broker_wait": 0.5,
        "celery.worker_wait": 0.25,
        "celery.dispatch_wait": 0.75,
    }
    assert sum(point.count for point in points) == 3
    for point in points:
        assert (
            not {
                "submitted_at",
                "published_at",
                "received_at",
                "celery_task_id",
                "task_id",
            }
            & attribute_dict(point.attributes).keys()
        )


def test_long_backend_dispatch_wait_is_not_clipped_to_browser_input_limit(
    telemetry: Any,
) -> None:
    from superset.observability import observe_duration

    state, receiver, _ = telemetry
    observe_duration(
        "celery.dispatch_wait", 172800, phase="scheduled", outcome="success"
    )
    observe_duration("browser.chart.load", 172800, phase="load", outcome="success")
    state.export_once()
    points = metric_map(receiver)[
        "superset_operation_duration_seconds"
    ].histogram.data_points
    assert len(points) == 1
    assert points[0].count == 1
    assert points[0].sum == 172800
    assert attribute_dict(points[0].attributes)["phase"] == "scheduled"


def test_actual_scope_start_complete_and_balance_use_frozen_safe_dimensions(
    telemetry: Any,
) -> None:
    state, receiver, _ = telemetry
    scope = operation(
        "sql.execute",
        source="chart",
        phase="execute",
        database_id=9,
        warehouse_name="DASH_WH",
        request_actor_user_id=42,
        execution_id="execution-1",
        outcome="running",
        error_category="database",
    )
    expected = ValueError("original scoped business failure")
    with pytest.raises(ValueError, match=re.escape(str(expected))) as caught:  # noqa: PT012
        with scope:
            state.export_once()
            current = metric_map(receiver)
            started = current["superset_operation_scopes_started_total"].sum
            inflight = current["superset_operation_scopes_inflight"].sum
            assert started.is_monotonic
            assert not inflight.is_monotonic
            assert (
                started.aggregation_temporality == inflight.aggregation_temporality == 2
            )
            assert started.data_points[0].as_int == inflight.data_points[0].as_int == 1
            frozen = attribute_dict(inflight.data_points[0].attributes)
            assert frozen == {
                "operation": "sql.execute",
                "source": "chart",
                "phase": "execute",
            }
            scope.set_attribute("source", "report")
            scope.set_attribute("phase", "fetch")
            scope.set_attribute("error_category", "timeout")
            scope.set_attribute("warehouse_name", "OTHER_WH")
            raise expected
    assert caught.value is expected
    # Duplicate teardown/reentry cannot create an additional lifecycle or leak.
    scope.__exit__(None, None, None)
    scope.__enter__()
    scope.__exit__(None, None, None)
    state.export_once()
    final = metric_map(receiver)
    assert (
        final["superset_operation_scopes_started_total"].sum.data_points[0].as_int == 1
    )
    assert (
        final["superset_operation_scopes_completed_total"].sum.data_points[0].as_int
        == 1
    )
    balance = final["superset_operation_scopes_inflight"].sum.data_points
    assert len(balance) == 1
    assert balance[0].as_int == 0
    assert attribute_dict(balance[0].attributes) == frozen
    assert (
        attribute_dict(
            final["superset_operation_scopes_completed_total"]
            .sum.data_points[0]
            .attributes
        )
        == frozen
    )


def test_supplied_browser_and_queue_observations_do_not_create_fake_scopes(
    telemetry: Any,
) -> None:
    from superset.observability import observe_duration

    state, receiver, _ = telemetry
    observe_duration("browser.chart.render", 0.25, phase="render", outcome="success")
    observe_duration("celery.broker_wait", 0.75, phase="broker", outcome="success")
    state.export_once()
    metrics = metric_map(receiver)
    assert not any(name.startswith("superset_operation_scopes_") for name in metrics)
    assert (
        sum(
            point.as_int
            for point in metrics["superset_operations_total"].sum.data_points
        )
        == 2
    )


def test_scope_finish_is_idempotent_under_concurrent_teardown(telemetry: Any) -> None:
    state, receiver, _ = telemetry
    with operation("sql.execute", source="sql_lab") as scope:
        token = scope._scope_metrics
        assert token is not None
        barrier = threading.Barrier(9)

        def finish() -> None:
            barrier.wait(timeout=2)
            token.finish()

        finishers = [threading.Thread(target=finish) for _ in range(8)]
        for finisher in finishers:
            finisher.start()
        barrier.wait(timeout=2)
        for finisher in finishers:
            finisher.join(timeout=2)
            assert not finisher.is_alive()
    state.export_once()
    metrics = metric_map(receiver)
    assert (
        metrics["superset_operation_scopes_started_total"].sum.data_points[0].as_int
        == 1
    )
    assert (
        metrics["superset_operation_scopes_completed_total"].sum.data_points[0].as_int
        == 1
    )
    assert metrics["superset_operation_scopes_inflight"].sum.data_points[0].as_int == 0


def test_scope_overflow_and_late_dimension_changes_cannot_unbalance_inflight(
    telemetry: Any,
) -> None:
    from superset.observability.metrics import _DIMENSIONS

    state, receiver, _ = telemetry
    outer = operation("http.request", source="chart", phase="request")
    outer.__enter__()
    count = 1
    for source in sorted(_DIMENSIONS["source"]):
        for phase in sorted(_DIMENSIONS["phase"]):
            with operation("sql.execute", source=source, phase=phase):
                count += 1
    assert len(state.series["superset_operation_scopes"]) == 256
    state.export_once()
    before = metric_map(receiver)["superset_operation_scopes_inflight"].sum.data_points
    assert sum(point.as_int for point in before) == 1
    assert all(point.as_int >= 0 for point in before)
    outer.set_attribute("source", "report")
    outer.set_attribute("phase", "fetch")
    outer.set_attribute("outcome", "error")
    outer.__exit__(None, None, None)
    state.export_once()
    metrics = metric_map(receiver)
    starts = metrics["superset_operation_scopes_started_total"].sum.data_points
    completed = metrics["superset_operation_scopes_completed_total"].sum.data_points
    inflight = metrics["superset_operation_scopes_inflight"].sum.data_points
    assert sum(point.as_int for point in starts) == count
    assert sum(point.as_int for point in completed) == count
    assert all(point.as_int == 0 for point in inflight)
    assert any(
        attribute_dict(point.attributes) == {"otel.metric.overflow": True}
        and point.as_int > 0
        for point in starts
    )
    assert (
        {tuple(sorted(attribute_dict(point.attributes).items())) for point in starts}
        == {
            tuple(sorted(attribute_dict(point.attributes).items()))
            for point in completed
        }
        == {
            tuple(sorted(attribute_dict(point.attributes).items()))
            for point in inflight
        }
    )
