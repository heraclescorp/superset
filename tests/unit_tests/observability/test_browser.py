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
"""Browser decoding, authorization, identity, bounds and transport isolation."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from flask import Flask, g, Response
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from pytest_mock import MockerFixture

from superset.observability import (
    browser,
    metrics as metrics_module,
    observe_duration as native_observe_duration,
)
from superset.security.guest_token import GuestToken, GuestUser
from superset.utils import json


def event(**overrides: Any) -> dict[str, Any]:
    """Produce a valid client observation without any business-query payload."""
    return {
        "event_name": "chart.load",
        "browser_event_id": "browser-event-0001",
        "browser_view_id": "browser-view-0001",
        "browser_load_id": "browser-load-0001",
        "chart_id": 7,
        "phase": "load",
        "outcome": "success",
        "duration_seconds": 0.25,
        **overrides,
    }


@pytest.fixture
def ingress(mocker: MockerFixture) -> SimpleNamespace:
    """Use real Flask parsing with an existing authenticated-route boundary."""
    app = Flask(__name__)
    app.config.update(
        OBSERVABILITY_ENABLED=True,
        WTF_CSRF_ENABLED=False,
        SECRET_KEY="unit-test-key",  # noqa: S106
    )
    browser.register_browser(app)
    mocker.patch.object(
        browser, "is_enabled", side_effect=lambda: app.config["OBSERVABILITY_ENABLED"]
    )
    user = SimpleNamespace(id=42, is_authenticated=True, username="authenticated")
    emitted = mocker.patch.object(browser, "emit_event")
    observed = mocker.patch.object(browser, "observe_duration")
    manager = mocker.patch.object(browser, "security_manager", new_callable=MagicMock)
    manager.is_guest_user.return_value = False
    chart = SimpleNamespace(id=7, datasource_id=9, datasource_type="table")
    dashboard = SimpleNamespace(id=12, slices=[chart])
    session = mocker.patch.object(browser, "db").session

    def query(model: Any) -> MagicMock:
        result = MagicMock()
        result.filter_by.return_value.one_or_none.return_value = (
            chart if model is browser.Slice else dashboard
        )
        return result

    session.query.side_effect = query
    legacy = MagicMock(return_value=Response(status=200))
    handler = browser.browser_log(legacy)
    app.add_url_rule("/log/", "log", handler, methods=["POST"])

    @app.before_request
    def authenticate() -> None:
        g.user = state.user

    state = SimpleNamespace(
        app=app,
        client=app.test_client(),
        emitted=emitted,
        observed=observed,
        manager=manager,
        user=user,
        chart=chart,
        dashboard=dashboard,
        session=session,
        legacy=legacy,
    )
    return state


@pytest.fixture
def browser_metrics(
    ingress: SimpleNamespace, mocker: MockerFixture
) -> Iterator[InMemoryMetricReader]:
    """Aggregate real native SDK counters/histograms without any exporter I/O."""
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    state = SimpleNamespace(
        meter_provider=provider,
        instrument_lock=threading.RLock(),
        instruments={},
        series={},
        overflowed_metric_observations=0,
    )
    mocker.patch.object(metrics_module, "get_runtime", return_value=state)
    ingress.observed.side_effect = native_observe_duration
    try:
        yield reader
    finally:
        provider.shutdown()


def collected_metrics(reader: InMemoryMetricReader) -> dict[str, Any]:
    """Read actual SDK aggregation, not a forwarding mock's call arguments."""
    data = reader.get_metrics_data()
    if data is None:
        return {}
    return {
        metric.name: metric
        for resource in data.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
    }


def test_browser_payload_is_allowlisted_and_actor_is_server_derived(
    ingress: SimpleNamespace,
) -> None:
    response = ingress.client.post(
        "/log/?observability=browser",
        json={
            "observability_events": [
                event(
                    request_id="chart-request-123",
                    request_actor_user_id=999,
                    effective_user_id=998,
                    guest_actor_id="forged-guest-id",
                    source="report",
                    trust="server",
                    sql="SELECT secret FROM private",
                    filters=[{"secret": "private"}],
                    error_details="sensitive failure message",
                    token="raw-token",  # noqa: S106
                )
            ]
        },
    )
    assert response.status_code == 200
    ingress.legacy.assert_not_called()
    ingress.manager.raise_for_access.assert_called_once_with(chart=ingress.chart)
    args, fields = ingress.emitted.call_args
    assert args == ("browser.chart.load",)
    assert fields == {
        "browser_event_id": "browser-event-0001",
        "browser_view_id": "browser-view-0001",
        "browser_load_id": "browser-load-0001",
        "chart_id": 7,
        "dataset_id": 9,
        "datasource_type": "table",
        "phase": "load",
        "outcome": "success",
        "duration_seconds": 0.25,
        "request_id": "chart-request-123",
        "request_actor_user_id": 42,
        "effective_user_id": 42,
        "guest_actor_id": None,
        "source": "browser",
        "trust": "client_untrusted",
    }


def test_active_duration_is_allowlisted_and_used_for_metrics(
    ingress: SimpleNamespace,
) -> None:
    response = ingress.client.post(
        "/log/?observability=browser",
        json={"observability_events": [event(active_duration_seconds=0.1)]},
    )
    assert response.status_code == 200
    _, fields = ingress.emitted.call_args
    assert fields["duration_seconds"] == 0.25
    assert fields["active_duration_seconds"] == 0.1
    observed_args, _ = ingress.observed.call_args
    assert observed_args == ("browser.chart.load", 0.1)


def test_guest_form_batches_use_authorized_assets_and_canonical_pseudoprincipal(
    ingress: SimpleNamespace,
) -> None:
    ingress.manager.is_guest_user.return_value = True
    token: GuestToken = {
        "iat": 0,
        "exp": 100,
        "user": {"username": "guest-sensitive-identity"},
        "resources": [],
        "rls_rules": [],
    }
    ingress.user = GuestUser(token, [])
    with ingress.app.test_request_context():
        g.user = ingress.user
        canonical_guest_id = browser.get_context()["guest_actor_id"]
    response = ingress.client.post(
        "/log/?observability=browser",
        data={
            "observability_events": json.dumps([event(dashboard_id=12)]),
            "csrf_token": "csrf-credential",
            "guest_token": "guest-credential",
        },
    )
    assert response.status_code == 200
    ingress.legacy.assert_not_called()
    assert ingress.manager.raise_for_access.call_count == 2
    fields = ingress.emitted.call_args.kwargs
    assert fields["request_actor_user_id"] is None
    assert fields["effective_user_id"] is None
    assert len(fields["guest_actor_id"]) == 64
    assert fields["guest_actor_id"] == canonical_guest_id
    assert "guest-sensitive-identity" not in json.dumps(fields)
    assert "credential" not in json.dumps(fields)


@pytest.mark.parametrize("denial", ["chart", "dashboard", "membership", "dataset"])
def test_unauthorized_or_inconsistent_assets_are_silently_dropped(
    ingress: SimpleNamespace, denial: str
) -> None:
    payload = event(dashboard_id=12, dataset_id=9, datasource_type="table")
    if denial in {"chart", "dashboard"}:

        def authorize(**kwargs: Any) -> None:
            if denial in kwargs:
                raise PermissionError("private access failure")

        ingress.manager.raise_for_access.side_effect = authorize
    elif denial == "membership":
        ingress.dashboard.slices = []
    else:
        payload["dataset_id"] = 999
    response = ingress.client.post(
        "/log/?observability=browser", json={"observability_events": [payload]}
    )
    assert response.status_code == 200
    ingress.emitted.assert_not_called()
    ingress.legacy.assert_not_called()


@pytest.mark.parametrize(
    "overrides",
    [
        {"duration_seconds": -1},
        {"duration_seconds": float("nan")},
        {"duration_seconds": float("inf")},
        {"duration_seconds": True},
        {"duration_seconds": 86401},
        {"active_duration_seconds": -1},
        {"active_duration_seconds": float("nan")},
        {"active_duration_seconds": float("inf")},
        {"active_duration_seconds": True},
        {"active_duration_seconds": 86401},
        {"chart_id": True},
        {"chart_id": "7"},
        {"phase": ["load"]},
        {"outcome": {"success": True}},
        {"event_name": "sql.execute"},
        {"browser_event_id": "secret\nSELECT private"},
        {"browser_view_id": "x" * 1000},
        {"browser_load_id": None},
        {"cache_hit": "true"},
        {"async_execution": 1},
        {"force_refresh": None},
    ],
)
def test_invalid_events_never_reach_asset_lookup_or_logging(
    ingress: SimpleNamespace, overrides: dict[str, Any]
) -> None:
    response = ingress.client.post(
        "/log/?observability=browser",
        json={"observability_events": [event(**overrides)]},
    )
    assert response.status_code == 200
    ingress.session.query.assert_not_called()
    ingress.emitted.assert_not_called()


def test_batch_and_body_bounds_are_applied_before_asset_lookup(
    ingress: SimpleNamespace,
) -> None:
    response = ingress.client.post(
        "/log/?observability=browser", json={"observability_events": [event()] * 65}
    )
    assert response.status_code == 200
    response = ingress.client.post(
        "/log/?observability=browser",
        data=json.dumps({"observability_events": [event()], "padding": "x" * 65536}),
        content_type="application/json",
    )
    assert response.status_code == 200
    ingress.session.query.assert_not_called()
    ingress.emitted.assert_not_called()
    ingress.legacy.assert_not_called()


def test_duplicates_disabled_telemetry_and_logging_failures_are_isolated(
    ingress: SimpleNamespace,
) -> None:
    ingress.client.post(
        "/log/?observability=browser", json={"observability_events": [event(), event()]}
    )
    assert ingress.emitted.call_count == 1
    ingress.emitted.reset_mock()
    ingress.app.config["OBSERVABILITY_ENABLED"] = False
    ingress.client.post(
        "/log/?observability=browser", json={"observability_events": [event()]}
    )
    ingress.emitted.assert_not_called()
    ingress.app.config["OBSERVABILITY_ENABLED"] = True
    ingress.emitted.side_effect = RuntimeError("exporter broken")
    assert (
        ingress.client.post(
            "/log/?observability=browser", json={"observability_events": [event()]}
        ).status_code
        == 200
    )


def test_anonymous_claims_cannot_create_actor_records(ingress: SimpleNamespace) -> None:
    ingress.user.id = None
    ingress.user.is_authenticated = False
    ingress.client.post(
        "/log/?observability=browser",
        json={"observability_events": [event(request_actor_user_id=1)]},
    )
    ingress.session.query.assert_not_called()
    ingress.emitted.assert_not_called()


def test_existing_log_events_still_use_the_existing_logger(
    ingress: SimpleNamespace,
) -> None:
    assert (
        ingress.client.post(
            "/log/", json={"events": [{"event_name": "mount_dashboard"}]}
        ).status_code
        == 200
    )
    ingress.legacy.assert_called_once()
    ingress.emitted.assert_not_called()


def test_unsaved_charts_require_datasource_access_and_do_not_mutate_sql(
    ingress: SimpleNamespace,
) -> None:
    datasource = SimpleNamespace(id=9, sql="SELECT private FROM protected")
    datasource_query = ingress.session.query.return_value.filter_by.return_value
    datasource_query.one_or_none.return_value = datasource
    ingress.session.query.side_effect = None
    payload = event(dataset_id=9, datasource_type="table")
    del payload["chart_id"]
    ingress.client.post(
        "/log/?observability=browser", json={"observability_events": [payload]}
    )
    ingress.manager.raise_for_access.assert_called_once_with(datasource=datasource)
    assert datasource.sql == "SELECT private FROM protected"
    fields = ingress.emitted.call_args.kwargs
    assert fields["dataset_id"] == 9
    assert fields["datasource_type"] == "table"
    assert "private" not in json.dumps(fields)


@pytest.mark.parametrize("raw", ["not-json", "{", "null", "{}", "[[]]"])
def test_malformed_form_batches_never_fall_through_to_legacy_payload_logging(
    ingress: SimpleNamespace, raw: str
) -> None:
    response = ingress.client.post(
        "/log/?observability=browser",
        data={"observability_events": raw, "guest_token": "private-credential"},
    )
    assert response.status_code == 200
    ingress.session.query.assert_not_called()
    ingress.emitted.assert_not_called()
    ingress.legacy.assert_not_called()


def test_legacy_batches_keep_their_existing_size_behavior(
    ingress: SimpleNamespace,
) -> None:
    response = ingress.client.post(
        "/log/",
        json={"events": [{"event_name": "mount_dashboard", "value": "x" * 65536}]},
    )
    assert response.status_code == 200
    ingress.legacy.assert_called_once()
    ingress.emitted.assert_not_called()


@pytest.mark.parametrize("initiator", [None, 123])
def test_trusted_report_initiator_is_distinct_from_authenticated_executor(
    ingress: SimpleNamespace, initiator: int | None
) -> None:
    with browser.telemetry_context(source="report", request_actor_user_id=initiator):
        ingress.client.post(
            "/log/?observability=browser",
            json={"observability_events": [event(request_actor_user_id=999)]},
        )
    fields = ingress.emitted.call_args.kwargs
    assert fields["request_actor_user_id"] == initiator
    assert fields["effective_user_id"] == 42
    assert fields["source"] == "browser"


def test_authorized_reported_browser_seconds_produce_native_counts_and_histograms(
    ingress: SimpleNamespace, browser_metrics: InMemoryMetricReader
) -> None:
    observations = [
        ("chart.load", "load", "success", 1.25),
        ("chart.data_ready", "data", "success", 0.4),
        ("chart.render", "render", "error", 0.125),
        ("chart.request", "request", "success", 0.2),
        ("dashboard.load", "load", "abandoned", 3.5),
    ]
    payloads = []
    for index, (name, phase, outcome, duration) in enumerate(observations):
        payload = event(
            event_name=name,
            phase=phase,
            outcome=outcome,
            duration_seconds=duration,
            browser_event_id=f"browser-event-{index:04d}",
            cache_hit=True,
            async_execution=False,
            force_refresh=True,
        )
        if name == "dashboard.load":
            del payload["chart_id"]
            del payload["browser_load_id"]
            payload["dashboard_id"] = 12
        payloads.append(payload)
    with browser.telemetry_context(
        source="report",
        database_id=999,
        warehouse_name="PRIVATE_WH",
        chart_id=7,
        request_actor_user_id=123,
        task_id="private-task-id",
    ):
        response = ingress.client.post(
            "/log/?observability=browser", json={"observability_events": payloads}
        )
    assert response.status_code == 200
    assert ingress.emitted.call_count == 5
    metrics = collected_metrics(browser_metrics)
    assert set(metrics) == {
        "superset_operations_total",
        "superset_operation_duration_seconds",
    }
    counter = metrics["superset_operations_total"].data
    histogram = metrics["superset_operation_duration_seconds"].data
    assert counter.is_monotonic
    assert counter.aggregation_temporality == 2
    assert histogram.aggregation_temporality == 2
    assert metrics["superset_operation_duration_seconds"].unit == "s"
    counts = {point.attributes["operation"]: point for point in counter.data_points}
    durations = {
        point.attributes["operation"]: point for point in histogram.data_points
    }
    for name, phase, outcome, duration in observations:
        operation = f"browser.{name}"
        assert counts[operation].value == 1
        assert durations[operation].count == 1
        assert durations[operation].sum == pytest.approx(duration)
        assert sum(durations[operation].bucket_counts) == 1
        expected = {
            "operation": operation,
            "source": "browser",
            "trust": "client_untrusted",
            "phase": phase,
            "outcome": outcome,
            "cache_hit": True,
            "async_execution": False,
            "force_refresh": True,
        }
        assert dict(counts[operation].attributes) == expected
        assert dict(durations[operation].attributes) == expected


def test_started_invalid_unauthorized_and_disabled_browser_records_never_count(
    ingress: SimpleNamespace, browser_metrics: InMemoryMetricReader
) -> None:
    ingress.client.post(
        "/log/?observability=browser",
        json={"observability_events": [event(outcome="started", duration_seconds=50)]},
    )
    ingress.client.post(
        "/log/?observability=browser",
        json={"observability_events": [event(duration_seconds=float("nan"))]},
    )
    ingress.manager.raise_for_access.side_effect = PermissionError("private")
    ingress.client.post(
        "/log/?observability=browser", json={"observability_events": [event()]}
    )
    ingress.manager.raise_for_access.side_effect = None
    ingress.app.config["OBSERVABILITY_ENABLED"] = False
    ingress.client.post(
        "/log/?observability=browser", json={"observability_events": [event()]}
    )
    assert ingress.emitted.call_count == 1
    assert collected_metrics(browser_metrics) == {}


def test_browser_native_metrics_ignore_identifier_cardinality_and_count_duplicates_once(
    ingress: SimpleNamespace, browser_metrics: InMemoryMetricReader
) -> None:
    for batch in range(3):
        payloads = [
            event(
                browser_event_id=f"browser-event-{batch}-{index:04d}",
                browser_view_id=f"browser-view-{batch}-{index:04d}",
                browser_load_id=f"browser-load-{batch}-{index:04d}",
                request_id=f"server-request-{batch}-{index:04d}",
            )
            for index in range(32)
        ]
        with browser.telemetry_context(
            database_id=1000 + batch,
            warehouse_name=f"PRIVATE_{batch}",
        ):
            ingress.client.post(
                "/log/?observability=browser",
                json={"observability_events": payloads + [payloads[0]]},
            )
    metrics = collected_metrics(browser_metrics)
    points = metrics["superset_operations_total"].data.data_points
    assert len(points) == 1
    assert points[0].value == 96
    histogram = metrics["superset_operation_duration_seconds"].data.data_points
    assert len(histogram) == 1
    assert histogram[0].count == 96
    assert histogram[0].sum == 24
    assert set(points[0].attributes) == {
        "operation",
        "source",
        "trust",
        "phase",
        "outcome",
    }


def test_browser_native_metrics_and_events_fail_independently(
    ingress: SimpleNamespace, browser_metrics: InMemoryMetricReader
) -> None:
    ingress.emitted.side_effect = RuntimeError("private logger failure")
    response = ingress.client.post(
        "/log/?observability=browser", json={"observability_events": [event()]}
    )
    assert response.status_code == 200
    metrics = collected_metrics(browser_metrics)
    assert metrics["superset_operations_total"].data.data_points[0].value == 1
    ingress.emitted.side_effect = None
    ingress.emitted.reset_mock()
    ingress.observed.side_effect = RuntimeError("private metric failure")
    response = ingress.client.post(
        "/log/?observability=browser", json={"observability_events": [event()]}
    )
    assert response.status_code == 200
    ingress.emitted.assert_called_once()
