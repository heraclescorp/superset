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
"""Broker lifecycle privacy, request joins and real HTTP redirect isolation."""

from __future__ import annotations

import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from urllib.request import build_opener, Request

import pytest
from flask import Flask

from superset.observability import (
    events,
    get_context,
    http,
    lifecycle,
    metrics,
    restore_context,
    telemetry_context,
)
from superset.utils import json


@pytest.fixture
def observations(mocker):
    """Exercise real schema/context/operation code, without network exporters."""
    records = []
    mocker.patch.object(lifecycle, "is_enabled", return_value=True)
    mocker.patch.object(events, "enabled", return_value=True)
    mocker.patch.object(http, "enabled", return_value=True)
    mocker.patch.object(events, "get_runtime", return_value=None)
    mocker.patch.object(metrics, "get_runtime", return_value=None)
    mocker.patch.object(events, "configure_event_logger")
    mocker.patch.object(
        events._LOGGER,
        "info",
        side_effect=lambda line: records.append(json.loads(line)),
    )
    with restore_context({}):
        yield records


def test_task_starts_before_body_and_restores_identity(observations):
    headers = {
        "id": "celery-1",
        lifecycle.HEADER: {"request_actor_user_id": 999, "raw_sql": "secret"},
    }
    with telemetry_context(
        source="chart",
        task_id="native-1",
        request_actor_user_id=7,
        effective_user_id=12,
        chart_id=44,
        raw_sql="select secret",
    ):
        lifecycle._before_publish(headers=headers, sender="tasks.execute")
    assert headers[lifecycle.HEADER]["request_actor_user_id"] == 7
    assert "raw_sql" not in headers[lifecycle.HEADER]
    task = SimpleNamespace(
        name="tasks.execute",
        request=SimpleNamespace(headers=headers, retries=0, is_eager=False),
    )
    with telemetry_context(request_actor_user_id=88):
        lifecycle._prerun(task=task, task_id="celery-1")
        # No task/metastore body has run: pickup remains visible even if its
        # first metadata connection will fail.
        assert observations[-1]["message"] == "celery.task.started"
        assert get_context()["task_id"] == "native-1"
        assert get_context()["request_actor_user_id"] == 7
        assert get_context()["effective_user_id"] == 12
        lifecycle._postrun(
            task=task,
            state="SUCCESS",
            retval={"status": "failure", "result": "private result"},
        )
        lifecycle._postrun(task=task, state="SUCCESS")
        assert get_context()["request_actor_user_id"] == 88
        assert "task_id" not in get_context()
    finished = [
        item for item in observations if item["message"] == "celery.task.finished"
    ]
    assert len(finished) == 1
    assert finished[0]["metadata"]["outcome"] == "error"
    assert "private result" not in json.dumps(observations)
    assert "select secret" not in json.dumps(observations)


def test_retry_republish_keeps_native_source_and_never_formats_errors(observations):
    class PrivateError(Exception):
        def __str__(self):
            raise AssertionError("Telemetry must never format exception messages")

    headers = {"id": "celery-retry"}
    with telemetry_context(source="report", report_schedule_id=3, task_id="native-2"):
        lifecycle._before_publish(headers=headers, sender="reports.execute")
    task = SimpleNamespace(
        name="reports.execute",
        request=SimpleNamespace(headers=headers, retries=2, is_eager=False),
    )
    lifecycle._prerun(task=task, task_id="celery-retry")
    lifecycle._failure(exception=PrivateError("password=secret"))
    lifecycle._retry(request=task.request, reason=PrivateError("secret"))
    retried = {"id": "celery-retry"}
    lifecycle._before_publish(headers=retried, sender="reports.execute")
    assert retried[lifecycle.HEADER]["task_id"] == "native-2"
    assert retried[lifecycle.HEADER]["report_schedule_id"] == 3
    assert retried[lifecycle.HEADER]["report_execution_id"] == "celery-retry"
    lifecycle._postrun(task=task, state="RETRY")
    assert get_context() == {}
    assert observations[-1]["metadata"]["outcome"] == "retry"
    assert "password" not in json.dumps(observations)


def test_relationship_is_one_edge_per_request_not_one_execution_per_join(observations):
    app = Flask(__name__)
    for actor in (7, 8):
        with app.test_request_context(), telemetry_context(request_actor_user_id=actor):
            lifecycle.task_relationship("shared-task", actor == 7, "chart_data")
            lifecycle.task_relationship("shared-task", False, "chart_data")
    edges = [
        item["metadata"]
        for item in observations
        if item["message"] == "task.relationship"
    ]
    assert [
        (edge["request_actor_user_id"], edge["consumer_action"]) for edge in edges
    ] == [
        (7, "create"),
        (8, "join"),
    ]
    assert not any(item["message"] == "celery.task.started" for item in observations)


def test_disabled_producer_and_origin_check_do_no_signing(mocker):
    mocker.patch.object(lifecycle, "is_enabled", return_value=False)
    signer = mocker.patch.object(lifecycle, "signed_execution_headers")
    headers = {"id": "task"}
    lifecycle._before_publish(headers=headers, sender="reports.execute")
    assert lifecycle.HEADER not in headers
    assert lifecycle.execution_headers_for_url("https://superset.test/") == {}
    signer.assert_not_called()


@contextmanager
def recording_server():
    received = []
    destinations = {}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            received.append((self.path, self.headers.get(http.EXECUTION_HEADER)))
            if self.path in destinations:
                self.send_response(302)
                self.send_header("Location", destinations[self.path])
            else:
                self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *_):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", received, destinations
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_signed_report_headers_follow_only_superset_origin(observations):
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "unit-test-signing-key"  # noqa: S105
    with (
        recording_server() as (origin, own, redirects),
        recording_server() as (external, other, _),
    ):
        app.config["WEBDRIVER_BASEURL"] = origin
        redirects["/same"] = origin + "/final"
        redirects["/external"] = external + "/outside"
        with (
            app.app_context(),
            telemetry_context(
                source="report",
                report_schedule_id=17,
                report_execution_id="report-run-1",
                request_actor_user_id=7,
                effective_user_id=9,
            ),
        ):
            opener = build_opener(lifecycle.ExecutionContextRedirectHandler())
            for path in ("/same", "/external"):
                request = Request(origin + path)  # noqa: S310
                lifecycle.sign_execution_request(request)
                with opener.open(request, timeout=2) as response:
                    assert response.read() == b"ok"
            assert lifecycle.execution_headers_for_url(external + "/outside") == {}
            assert lifecycle.execution_headers_for_url("http://127.0.0.1.evil/") == {}
            payload = http._serializer().loads(own[0][1], max_age=60)
            assert payload["context"]["report_execution_id"] == "report-run-1"
            assert payload["context"]["request_actor_user_id"] == 7
            assert "effective_user_id" not in payload["context"]
        assert all(header for _, header in own)
        assert other == [("/outside", None)]


@pytest.mark.parametrize(
    "status,outcome",
    [
        ("success", "success"),
        ("failure", "error"),
        ("aborted", "cancelled"),
        ("timed_out", "timeout"),
        ("in_progress", "running"),
    ],
)
def test_native_statuses_use_canonical_lowercase(status, outcome, observations):
    lifecycle.task_transition("native-task", status)
    assert observations[-1]["metadata"]["task_status"] == status
    assert observations[-1]["metadata"]["outcome"] == outcome


def test_report_retry_is_not_a_successful_execution(observations, mocker):
    from datetime import datetime
    from uuid import uuid4

    from superset.commands.report.execute import (
        _report_outcome,
        AsyncExecuteReportScheduleCommand,
    )

    execution_id = str(uuid4())
    command = AsyncExecuteReportScheduleCommand(execution_id, 17, datetime.now())

    def body():
        assert get_context()["source"] == "report"
        assert get_context()["report_execution_id"] == execution_id
        assert get_context()["report_schedule_id"] == 17
        _report_outcome.set("retry")

    mocker.patch.object(command, "_run", side_effect=body)
    with telemetry_context(request_actor_user_id=7):
        assert command.run() is None
        assert get_context() == {"request_actor_user_id": 7}
    finished = [
        item for item in observations if item["message"] == "report.execute.finished"
    ]
    assert len(finished) == 1
    assert finished[0]["metadata"]["outcome"] == "retry"
    assert finished[0]["metadata"]["request_actor_user_id"] == 7


def test_warmup_partial_capture_preserves_actor_and_effective_user(
    observations, mocker
):
    from superset.tasks import cache

    app = Flask(__name__)
    app.config.update(
        SUPERSET_CACHE_WARMUP_USER="warmup-user",
        WEBDRIVER_WINDOW={"dashboard": (100, 100)},
    )
    mocker.patch.object(cache, "is_enabled", return_value=True)
    user = SimpleNamespace(id=9)
    captured_contexts = []

    def find_user(username):
        assert username == "warmup-user"
        assert get_context()["source"] == "warmup"
        assert get_context()["warmup_run_id"]
        return user

    class Strategy:
        uses_webdriver = True

        def get_urls(self):
            return [
                "https://superset.test/ok",
                "https://superset.test/private?token=secret",
            ]

    class Driver:
        def __init__(self, *_):
            return

        def get_screenshot(self, url, element, user):
            captured_contexts.append(get_context())
            return b"image" if url.endswith("/ok") else None

    mocker.patch.dict(cache.strategy_registry, {"unit-strategy": Strategy})
    mocker.patch.object(cache, "security_manager", SimpleNamespace(find_user=find_user))
    mocker.patch.object(cache, "WebDriverPlaywright", Driver)
    cleanup = mocker.patch.object(cache._browser_manager, "_cleanup")
    with app.app_context(), telemetry_context(request_actor_user_id=7):
        result = cache.cache_warmup.run("unit-strategy")
        assert get_context() == {"request_actor_user_id": 7}
    assert len(result["success"]) == 1
    assert len(result["errors"]) == 1
    assert all(context["source"] == "warmup" for context in captured_contexts)
    assert all(context["request_actor_user_id"] == 7 for context in captured_contexts)
    assert all(context["effective_user_id"] == 9 for context in captured_contexts)
    cleanup.assert_called_once()
    captures = [
        item["metadata"]["outcome"]
        for item in observations
        if item["message"] == "warmup.capture.finished"
    ]
    assert captures == ["success", "error"]
    assert observations[-1]["message"] == "warmup.execute.finished"
    assert observations[-1]["metadata"]["outcome"] == "partial"
    assert "token=secret" not in json.dumps(observations)


def test_revoked_parent_request_restores_trusted_header_without_body(observations):
    request = SimpleNamespace(
        id="revoked-celery",
        name="reports.execute",
        request_dict={
            lifecycle.HEADER: {
                "source": "report",
                "report_schedule_id": 17,
                "report_execution_id": "revoked-run",
                "request_actor_user_id": None,
                "raw_sql": "secret",
                "guest_token": "private",
            }
        },
    )
    lifecycle._revoked(request=request, expired=True)
    assert observations[-1]["message"] == "celery.task.cancelled"
    metadata = observations[-1]["metadata"]
    assert metadata["outcome"] == "expired"
    assert metadata["source"] == "report"
    assert metadata["report_execution_id"] == "revoked-run"
    assert metadata["request_actor_user_id"] is None
    assert get_context() == {}
    assert "secret" not in json.dumps(observations)
    assert "guest_token" not in json.dumps(observations)


@pytest.fixture
def queue_transport(mocker, observations):
    """Use pinned Celery Request/Context and Kombu message metadata, not mocks."""
    from datetime import datetime, timezone

    from celery import Celery
    from celery.worker.request import Request as WorkerRequest
    from kombu import Queue
    from kombu.message import Message

    app = Celery("lifecycle-queue-timing", broker="memory://", set_as_current=False)
    app.conf.update(
        task_default_queue="queries",
        timezone="UTC",
        task_queues=(Queue("queries", routing_key="queries"),),
    )

    @app.task(name="unit.lifecycle.echo", shared=False)
    def echo(value, **kwargs):
        return value

    base = datetime(2026, 11, 3, tzinfo=timezone.utc)
    clock = [base]
    mocker.patch.object(lifecycle, "celery_app", app)
    mocker.patch.object(lifecycle, "_utc_now", side_effect=lambda: clock[0])

    def parent_request(headers, *, redelivered=False):
        message = Message(
            body=json.dumps(
                [["select private from payload"], {"guest_token": "private guest"}, {}]
            ),
            content_type="application/json",
            content_encoding="utf-8",
            headers=headers,
            properties={"correlation_id": headers["id"]},
            delivery_info={
                "exchange": "",
                "routing_key": "queries",
                "redelivered": redelivered,
            },
        )
        return WorkerRequest(message, app=app, task=echo)

    try:
        yield SimpleNamespace(
            app=app, task=echo, base=base, clock=clock, parent_request=parent_request
        )
    finally:
        app.close()


def test_received_stamp_survives_real_celery_parent_child_boundary(
    queue_transport, observations
):
    import pickle
    from datetime import timedelta

    transport = queue_transport
    headers = {"id": "celery-clock-1", "task": transport.task.name, "retries": 0}
    with telemetry_context(
        source="chart", task_id="native-clock-1", request_actor_user_id=7
    ):
        lifecycle._before_publish(
            headers=headers, sender=transport.task.name, routing_key="queries"
        )
    parent = transport.parent_request(headers)
    transport.clock[0] = transport.base + timedelta(seconds=2.5)
    lifecycle._received(request=parent)
    assert get_context() == {}
    assert (
        parent.message.headers[lifecycle.HEADER]["received_at"]
        == transport.clock[0].isoformat()
    )
    received = next(
        record["metadata"]
        for record in observations
        if record["message"] == "celery.task.received"
    )
    assert received["queue_name"] == "queries"
    assert received["task_id"] == "native-clock-1"
    assert received["request_actor_user_id"] == 7

    # Celery 5.6 serializes Request.request_dict into the prefork job. Its
    # Context reconstructs custom headers separately from business args/kwargs.
    child_request = pickle.loads(pickle.dumps(parent.request_dict))  # noqa: S301
    child_request.update(called_directly=False, is_eager=False)
    transport.task.push_request(**child_request)
    try:
        transport.clock[0] = transport.base + timedelta(seconds=5.25)
        lifecycle._prerun(task=transport.task, task_id=headers["id"])
        assert (
            transport.task.run(
                *transport.task.request.args, **transport.task.request.kwargs
            )
            == "select private from payload"
        )
        waits = {
            record["metadata"]["operation"]: record["metadata"]
            for record in observations
            if record["message"].endswith("_wait.observed")
        }
        assert {name: record["duration_seconds"] for name, record in waits.items()} == {
            "celery.dispatch_wait": 5.25,
            "celery.broker_wait": 2.5,
            "celery.worker_wait": 2.75,
        }
        assert waits["celery.dispatch_wait"]["phase"] == "queue"
        assert waits["celery.worker_wait"]["queue_name"] == "queries"
        transport.clock[0] = transport.base + timedelta(seconds=9)
        lifecycle._postrun(
            task=transport.task, state="SUCCESS", retval="private result"
        )
        finished = observations[-1]["metadata"]
        assert (
            finished["started_at"]
            == (transport.base + timedelta(seconds=5.25)).isoformat()
        )
        assert finished["finished_at"] == transport.clock[0].isoformat()
    finally:
        transport.task.pop_request()
    assert get_context() == {}
    assert "select private" not in json.dumps(observations)
    assert "private guest" not in json.dumps(observations)
    assert "private result" not in json.dumps(observations)


def test_retry_refreshes_attempt_publication_without_losing_original_submission(
    queue_transport, observations
):
    from datetime import timedelta

    transport = queue_transport
    original = transport.base.isoformat()
    with telemetry_context(
        source="chart",
        task_id="native-retry",
        submitted_at=original,
        published_at=original,
        received_at=(transport.base + timedelta(seconds=1)).isoformat(),
        started_at=(transport.base + timedelta(seconds=2)).isoformat(),
        finished_at=(transport.base + timedelta(seconds=3)).isoformat(),
    ):
        transport.clock[0] = transport.base + timedelta(seconds=10)
        headers = {"id": "same-celery-id", "task": transport.task.name, "retries": 1}
        lifecycle._before_publish(
            headers=headers, sender=transport.task.name, routing_key="queries"
        )
    attempt = headers[lifecycle.HEADER]
    assert attempt["submitted_at"] == original
    assert attempt["published_at"] == transport.clock[0].isoformat()
    assert attempt["retry_count"] == 1
    assert not {"received_at", "started_at", "finished_at"} & attempt.keys()
    transport.clock[0] = transport.base + timedelta(seconds=12)
    parent = transport.parent_request(headers)
    lifecycle._received(request=parent)
    from celery.app.task import Context

    child = SimpleNamespace(
        name=transport.task.name,
        request=Context(parent.request_dict, called_directly=False),
    )
    transport.clock[0] = transport.base + timedelta(seconds=15)
    lifecycle._prerun(task=child, task_id=headers["id"])
    lifecycle._postrun(task=child, state="SUCCESS")
    waits = {
        record["metadata"]["operation"]: record["metadata"]["duration_seconds"]
        for record in observations
        if record["message"].endswith("_wait.observed")
    }
    assert waits == {
        "celery.dispatch_wait": 5,
        "celery.broker_wait": 2,
        "celery.worker_wait": 3,
    }


@pytest.mark.parametrize(
    "received_seconds,started_seconds,expected",
    [
        (2, 65, {"celery.dispatch_wait": 65, "celery.worker_wait": 5}),
        (
            65,
            70,
            {
                "celery.dispatch_wait": 70,
                "celery.broker_wait": 5,
                "celery.worker_wait": 5,
            },
        ),
    ],
)
def test_eta_hold_is_not_ready_or_reserved_backlog(
    queue_transport, observations, received_seconds, started_seconds, expected
):
    from datetime import timedelta

    from celery.app.task import Context

    transport = queue_transport
    headers = {
        "id": "scheduled-clock",
        "task": transport.task.name,
        "eta": (transport.base + timedelta(seconds=60)).isoformat(),
    }
    lifecycle._before_publish(
        headers=headers, sender=transport.task.name, routing_key="queries"
    )
    transport.clock[0] = transport.base + timedelta(seconds=received_seconds)
    parent = transport.parent_request(headers)
    lifecycle._received(request=parent)
    child = SimpleNamespace(
        name=transport.task.name,
        request=Context(parent.request_dict, called_directly=False),
    )
    transport.clock[0] = transport.base + timedelta(seconds=started_seconds)
    lifecycle._prerun(task=child, task_id=headers["id"])
    lifecycle._postrun(task=child, state="SUCCESS")
    waits = {
        record["metadata"]["operation"]: record["metadata"]["duration_seconds"]
        for record in observations
        if record["message"].endswith("_wait.observed")
    }
    assert waits == expected
    dispatch = next(
        record["metadata"]
        for record in observations
        if record["message"] == "celery.dispatch_wait.observed"
    )
    assert dispatch["phase"] == "scheduled"


@pytest.mark.parametrize(
    "published,received,expected",
    [
        (None, None, {}),
        ("naive", None, {}),
        (0, None, {"celery.dispatch_wait": 5}),
        (10, 2, {"celery.worker_wait": 3}),
        (0, 10, {"celery.broker_wait": 10}),
    ],
)
def test_missing_and_skewed_clocks_never_create_fake_zero_waits(
    queue_transport, observations, published, received, expected
):
    from datetime import timedelta

    transport = queue_transport
    metadata = {}
    for field, offset in (("published_at", published), ("received_at", received)):
        if offset is not None:
            metadata[field] = (
                "2026-11-03T00:00:00"
                if offset == "naive"
                else (transport.base + timedelta(seconds=offset)).isoformat()
            )
    child = SimpleNamespace(
        name=transport.task.name,
        request=SimpleNamespace(
            headers={lifecycle.HEADER: metadata},
            retries=0,
            is_eager=False,
            called_directly=False,
        ),
    )
    transport.clock[0] = transport.base + timedelta(seconds=5)
    lifecycle._prerun(task=child, task_id="legacy-clock")
    lifecycle._postrun(task=child, state="SUCCESS")
    waits = {
        record["metadata"]["operation"]: record["metadata"]["duration_seconds"]
        for record in observations
        if record["message"].endswith("_wait.observed")
    }
    assert waits == expected


def test_redelivery_has_no_known_new_publish_broker_wait(queue_transport, observations):
    from datetime import timedelta

    from celery.app.task import Context

    transport = queue_transport
    headers = {"id": "redelivered-clock", "task": transport.task.name}
    lifecycle._before_publish(
        headers=headers, sender=transport.task.name, routing_key="queries"
    )
    transport.clock[0] = transport.base + timedelta(seconds=60)
    parent = transport.parent_request(headers, redelivered=True)
    lifecycle._received(request=parent)
    child = SimpleNamespace(
        name=transport.task.name,
        request=Context(parent.request_dict, called_directly=False),
    )
    transport.clock[0] = transport.base + timedelta(seconds=63)
    lifecycle._prerun(task=child, task_id=headers["id"])
    lifecycle._postrun(task=child, state="SUCCESS")
    waits = {
        record["metadata"]["operation"]: record["metadata"]["duration_seconds"]
        for record in observations
        if record["message"].endswith("_wait.observed")
    }
    assert waits == {"celery.worker_wait": 3}
