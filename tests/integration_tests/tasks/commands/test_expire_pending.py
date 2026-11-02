# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License.  You may obtain
# a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from datetime import datetime, timedelta
from os import environ
from time import monotonic
from unittest.mock import patch
from uuid import uuid4

import pytest
from flask import current_app
from flask_appbuilder.security.sqla.models import User
from superset_core.tasks.types import TaskScope, TaskStatus

from superset import db
from superset.commands.tasks.expire_pending import (
    ExpirePendingTasksCommand,
    PENDING_ERROR_MESSAGE,
)
from superset.coordination.base import CoordinationService
from superset.coordination.cache_backend import RedisCacheBackend
from superset.daos.tasks import TaskDAO
from superset.extensions import cache_manager
from superset.models.tasks import Task
from superset.tasks.async_queries import CHART_QUERY_TASK, ChartQueryConsumerPolicy
from superset.tasks.manager import TaskManager
from superset.tasks.utils import get_finished_dedup_key
from superset.utils import json


def _make_task(
    admin: User,
    key: str,
    status: TaskStatus,
    *,
    celery_task_id: str | None = None,
) -> Task:
    """Create a queue-expiry test task without a worker claim or heartbeat."""
    task = TaskDAO.create_task(
        task_type="test_type",
        task_key=key,
        scope=TaskScope.PRIVATE,
        user_id=admin.id,
    )
    task.created_by = admin
    task.set_status(status)
    if celery_task_id is not None:
        task.update_framework_private({"celery_task_id": celery_task_id})
    db.session.commit()
    return task


def _cleanup(*tasks: Task) -> None:
    """Delete test tasks and their dependency links."""
    for task in tasks:
        existing = db.session.get(Task, task.id)
        if existing:
            db.session.delete(existing)
    db.session.commit()


def test_pending_expiry_is_opt_in_and_publishes_terminal_failure(
    app_context, get_user, login_as, monkeypatch
) -> None:
    """Lost standalone work releases dedup and exposes FAILURE to every consumer."""
    login_as("admin")
    admin = get_user("admin")
    now = datetime(2026, 10, 1, 12)
    expired = _make_task(
        admin, "pending_expired", TaskStatus.PENDING, celery_task_id="queued-celery"
    )
    fresh = _make_task(admin, "pending_fresh", TaskStatus.PENDING)
    replacement = None
    expired.created_on = now - timedelta(hours=1)
    fresh.created_on = now - timedelta(seconds=1799)
    expired.update_properties({"private": {"subscription": {"consumers": ["tabA"]}}})
    db.session.commit()
    monkeypatch.setitem(current_app.config, "GTF_PENDING_TASK_TIMEOUT", None)
    try:
        with (
            patch.object(TaskDAO, "_db_utcnow", return_value=now),
            patch("superset.commands.tasks.expire_pending.celery_app") as celery,
            patch(
                "superset.tasks.manager.TaskManager.publish_completion"
            ) as completion,
            patch("superset.tasks.manager.TaskManager.publish_entity_change") as entity,
            patch(
                "superset.tasks.manager.TaskManager.publish_required_by_changed"
            ) as dependents,
        ):
            assert ExpirePendingTasksCommand().run() == 0
            completion.assert_not_called()
            db.session.refresh(expired)
            assert expired.status == TaskStatus.PENDING.value

            monkeypatch.setitem(current_app.config, "GTF_PENDING_TASK_TIMEOUT", 1800)
            assert ExpirePendingTasksCommand().run() == 1
            completion.assert_called_once_with(expired.uuid, TaskStatus.FAILURE.value)
            entity.assert_called_once_with(expired.uuid)
            dependents.assert_called_once_with(expired.uuid)
            celery.control.revoke.assert_called_once_with("queued-celery")

        db.session.refresh(expired)
        db.session.refresh(fresh)
        result = expired.to_dict()
        assert result["status"] == TaskStatus.FAILURE.value
        assert result["ended_at"] == now.isoformat()
        assert result["started_at"] is None
        assert expired.last_heartbeat is None
        assert result["properties"]["error_message"] == PENDING_ERROR_MESSAGE
        assert result["properties"]["private"]["framework"]["exception_type"] == (
            "PendingTaskTimeoutError"
        )
        assert result["properties"]["private"]["subscription"] == {
            "consumers": ["tabA"]
        }
        assert expired.dedup_key == get_finished_dedup_key(expired.uuid)
        assert fresh.status == TaskStatus.PENDING.value
        replacement = _make_task(admin, "pending_expired", TaskStatus.PENDING)
        assert replacement.uuid != expired.uuid
    finally:
        _cleanup(*([replacement] if replacement is not None else []), fresh, expired)


@pytest.mark.parametrize(
    "parent_status",
    [TaskStatus.SUCCESS, TaskStatus.FAILURE, TaskStatus.ABORTED, TaskStatus.TIMED_OUT],
)
def test_pending_dag_gets_full_budget_after_all_parents_finish(
    app_context, get_user, login_as, monkeypatch, parent_status
) -> None:
    """A long parent wait is protected; failed-parent deliveries cannot strand work."""
    login_as("admin")
    admin = get_user("admin")
    now = datetime(2026, 10, 1, 12)
    parent = _make_task(admin, "expiry_parent", TaskStatus.IN_PROGRESS)
    earlier_parent = _make_task(admin, "expiry_earlier_parent", TaskStatus.SUCCESS)
    child = _make_task(admin, "expiry_child", TaskStatus.PENDING)
    child.created_on = now - timedelta(hours=2)
    earlier_parent.ended_at = now - timedelta(hours=1)
    TaskDAO.add_dependencies(child.id, [earlier_parent.id, parent.id])
    db.session.commit()
    monkeypatch.setitem(current_app.config, "GTF_PENDING_TASK_TIMEOUT", 1800)
    try:
        with (
            patch.object(TaskDAO, "_db_utcnow", return_value=now) as clock,
            patch("superset.commands.tasks.expire_pending.celery_app"),
            patch(
                "superset.tasks.manager.TaskManager.publish_completion"
            ) as completion,
            patch("superset.tasks.manager.TaskManager.publish_entity_change"),
            patch("superset.tasks.manager.TaskManager.publish_required_by_changed"),
        ):
            assert ExpirePendingTasksCommand().run() == 0
            db.session.refresh(child)
            assert child.status == TaskStatus.PENDING.value

            parent.set_status(parent_status)
            parent.ended_at = now
            db.session.commit()
            assert ExpirePendingTasksCommand().run() == 0
            clock.return_value = now + timedelta(seconds=1799)
            assert ExpirePendingTasksCommand().run() == 0
            completion.assert_not_called()

            clock.return_value = now + timedelta(seconds=1801)
            assert ExpirePendingTasksCommand().run() == 1
            completion.assert_called_once_with(child.uuid, TaskStatus.FAILURE.value)

        db.session.refresh(child)
        assert child.status == TaskStatus.FAILURE.value
        assert child.ended_at == now + timedelta(seconds=1801)
        assert child.dedup_key == get_finished_dedup_key(child.uuid)
        assert child.properties_dict["error_message"] == PENDING_ERROR_MESSAGE
    finally:
        _cleanup(child, earlier_parent, parent)


@pytest.mark.parametrize("claim", ["heartbeat", "started_at", "in_progress"])
def test_pending_expiry_does_not_fail_worker_claim_after_candidate_scan(
    app_context, get_user, login_as, monkeypatch, claim
) -> None:
    """A fresh claim between scan and UPDATE prevents all expiry side effects."""
    login_as("admin")
    admin = get_user("admin")
    now = datetime(2026, 10, 1, 12)
    task = _make_task(admin, "pending_claim_race", TaskStatus.PENDING)
    task.created_on = now - timedelta(hours=1)
    db.session.commit()
    task_uuid = task.uuid
    original_dedup_key = task.dedup_key
    find_candidates = TaskDAO.find_expired_pending

    def claim_after_scan(timeout: int):
        candidates = find_candidates(timeout)
        assert task_uuid in candidates
        if claim == "heartbeat":
            TaskDAO.touch_heartbeat(task.id)
        else:
            assert TaskDAO.conditional_status_update(
                task_uuid,
                TaskStatus.IN_PROGRESS
                if claim == "in_progress"
                else TaskStatus.PENDING,
                expected_status=TaskStatus.PENDING,
                set_started_at=True,
            )
            db.session.commit()
        return candidates

    monkeypatch.setitem(current_app.config, "GTF_PENDING_TASK_TIMEOUT", 1800)
    try:
        with (
            patch.object(TaskDAO, "_db_utcnow", return_value=now),
            patch.object(TaskDAO, "find_expired_pending", side_effect=claim_after_scan),
            patch("superset.commands.tasks.expire_pending.celery_app") as celery,
            patch(
                "superset.tasks.manager.TaskManager.publish_completion"
            ) as completion,
            patch("superset.tasks.manager.TaskManager.publish_entity_change") as entity,
            patch(
                "superset.tasks.manager.TaskManager.publish_required_by_changed"
            ) as dependents,
        ):
            assert ExpirePendingTasksCommand().run() == 0
            completion.assert_not_called()
            entity.assert_not_called()
            dependents.assert_not_called()
            celery.control.revoke.assert_not_called()

        db.session.refresh(task)
        expected = (
            TaskStatus.IN_PROGRESS if claim == "in_progress" else TaskStatus.PENDING
        )
        assert task.status == expected.value
        assert task.ended_at is None
        assert task.dedup_key == original_dedup_key
        assert "error_message" not in task.properties_dict
    finally:
        _cleanup(task)


def _make_shared_task(
    admin: User, gamma: User, task_type: str
) -> tuple[Task, list[str]]:
    """Use real subscriber rows and the chart policy's persisted tab state."""
    task = TaskDAO.create_task(
        task_type=task_type,
        task_key=f"pending_shared_{uuid4().hex}",
        scope=TaskScope.SHARED,
        user_id=admin.id,
    )
    task.created_by = admin
    task.created_on = TaskDAO._db_utcnow() - timedelta(seconds=361)
    guest_key = f"guest:{uuid4().hex}"
    TaskDAO.add_subscriber(task.id, user_id=gamma.id)
    TaskDAO.add_guest_subscriber(task.id, guest_key)
    routes = [
        f"user:{admin.id}:tabA",
        f"user:{admin.id}:tabB",
        f"user:{gamma.id}:tabC",
        f"{guest_key}:tabG",
    ]
    policy = ChartQueryConsumerPolicy()
    for route in routes:
        principal, tab_id = route.rsplit(":", 1)
        policy.on_subscribe(task, principal=principal, client_ref=tab_id)
    db.session.commit()
    return task, routes


@pytest.mark.parametrize("terminalizer", ["expiry", "force_cancel"])
def test_shared_pending_terminal_state_reaches_subscriber_apis_and_poll(
    app_context, get_user, login_as, test_client, monkeypatch, terminalizer
) -> None:
    """Expiry and force cancel preserve subscribers and expose terminal state."""
    login_as("admin")
    admin, gamma = get_user("admin"), get_user("gamma")
    task, routes = _make_shared_task(admin, gamma, CHART_QUERY_TASK)
    fresh = _make_task(admin, f"pending_fresh_{uuid4().hex}", TaskStatus.PENDING)
    monkeypatch.setitem(current_app.config, "GTF_PENDING_TASK_TIMEOUT", 360)
    monkeypatch.setitem(current_app.config, "DEBUG", False)
    task_uuid = str(task.uuid)
    try:
        cursors = {}
        for username in ("admin", "gamma"):
            login_as(username)
            baseline = test_client.get("/api/v1/task/status_changes")
            assert baseline.status_code == 200
            cursors[username] = baseline.get_json()["cursor"]
            assert baseline.get_json()["statuses"] == {}

        if terminalizer == "expiry":
            assert ExpirePendingTasksCommand().run() == 1
            expected_status = TaskStatus.FAILURE.value
        else:
            login_as("admin")
            cancelled = test_client.post(
                f"/api/v1/task/{task_uuid}/cancel", json={"force": True}
            )
            assert cancelled.status_code == 200
            assert cancelled.get_json()["action"] == "aborted"
            expected_status = TaskStatus.ABORTED.value

        for username, cursor in cursors.items():
            login_as(username)
            detail = test_client.get(f"/api/v1/task/{task_uuid}")
            assert detail.status_code == 200
            result = detail.get_json()["result"]
            assert result["status"] == expected_status
            assert result["subscriber_count"] == 3
            assert result["started_at"] is None
            assert result["ended_at"] is not None
            assert "private" not in result["properties"]
            if terminalizer == "expiry":
                assert result["properties"]["error_message"] == PENDING_ERROR_MESSAGE
            status = test_client.get(f"/api/v1/task/{task_uuid}/status")
            assert status.status_code == 200
            assert status.get_json()["status"] == expected_status
            changed = test_client.get(
                "/api/v1/task/status_changes",
                query_string={"cursor": cursor, "task_type": CHART_QUERY_TASK},
            )
            assert changed.status_code == 200
            assert (
                changed.get_json()["statuses"][task_uuid]["status"] == expected_status
            )

        db.session.refresh(task)
        db.session.refresh(fresh)
        assert task.subscriber_count == 3
        assert ChartQueryConsumerPolicy().routing_channels(task) == routes
        assert task.dedup_key == get_finished_dedup_key(task.uuid)
        assert fresh.status == TaskStatus.PENDING.value
        completed = TaskManager.wait_for_completion(task.uuid, timeout=1)
        assert completed.status == expected_status
    finally:
        _cleanup(fresh, task)


@pytest.fixture
def real_coordination_backend(app_context, monkeypatch):
    """Use configured coordination or an explicitly advertised test Redis endpoint."""
    backend = CoordinationService.get_backend()
    if backend is not None:
        yield backend
        return
    if not environ.get("REDIS_PORT"):
        pytest.skip("Requires coordination config or test REDIS_HOST/REDIS_PORT")
    backend = RedisCacheBackend(
        host=environ.get("REDIS_HOST", "127.0.0.1"),
        port=int(environ["REDIS_PORT"]),
    )
    monkeypatch.setattr(cache_manager, "_distributed_coordination", backend)
    try:
        yield backend
    finally:
        backend._cache.close()


@pytest.mark.parametrize("task_type", ["test_type", CHART_QUERY_TASK])
@pytest.mark.parametrize("stream_failure", [False, True])
def test_pending_expiry_real_redis_fanout_survives_stream_failure(
    app_context,
    get_user,
    login_as,
    test_client,
    monkeypatch,
    real_coordination_backend,
    task_type,
    stream_failure,
) -> None:
    """Real Redis delivery reaches all subscriber routes even if XADD fails."""
    backend = real_coordination_backend
    login_as("admin")
    task, tab_routes = _make_shared_task(
        get_user("admin"), get_user("gamma"), task_type
    )
    child = _make_task(
        get_user("admin"), f"pending_child_{uuid4().hex}", TaskStatus.PENDING
    )
    TaskDAO.add_dependencies(child.id, [task.id])
    db.session.commit()
    task_uuid, task_id, child_id = str(task.uuid), task.id, child.id
    completion_channel = TaskManager.get_completion_channel(task.uuid)
    monkeypatch.setitem(current_app.config, "GTF_PENDING_TASK_TIMEOUT", 360)
    try:
        login_as("gamma")
        baseline = test_client.get("/api/v1/task/status_changes")
        assert baseline.status_code == 200
        cursor = baseline.get_json()["cursor"]
        if stream_failure:
            # A genuine stream-specific Redis error, not mocked notification wiring.
            backend.set(completion_channel, "not-a-stream", ex=60)
        with backend.pubsub() as consumer:
            consumer.subscribe(TaskManager.get_realtime_channel())
            assert consumer.get_message(timeout=5)["type"] == "subscribe"
            assert ExpirePendingTasksCommand().run() == 1
            envelopes = []
            deadline = monotonic() + 5
            while len(envelopes) < 3 and monotonic() < deadline:
                message = consumer.get_message(
                    ignore_subscribe_messages=True, timeout=1
                )
                if message is None:
                    continue
                envelope = json.loads(message["data"])
                payload = envelope["payload"]
                if payload.get("task_id") == task_uuid or payload.get("id") in {
                    task_id,
                    child_id,
                }:
                    envelopes.append(envelope)
            assert ExpirePendingTasksCommand().run() == 0

        status = test_client.get(f"/api/v1/task/{task_uuid}/status")
        assert status.status_code == 200
        assert status.get_json()["status"] == TaskStatus.FAILURE.value
        changed = test_client.get(
            "/api/v1/task/status_changes", query_string={"cursor": cursor}
        )
        assert changed.status_code == 200
        assert changed.get_json()["statuses"][task_uuid]["status"] == (
            TaskStatus.FAILURE.value
        )

        statuses = [e for e in envelopes if e["topic"] == TaskManager.TOPIC_TASK_STATUS]
        assert len(statuses) == 1
        assert statuses[0]["payload"] == {
            "task_id": task_uuid,
            "status": TaskStatus.FAILURE.value,
        }
        if task_type == CHART_QUERY_TASK:
            assert statuses[0]["scope"] == TaskManager.SCOPE_TAB
            assert set(statuses[0]["routes"]) == set(tab_routes)
        else:
            assert statuses[0]["scope"] == TaskManager.SCOPE_PRINCIPAL
            assert set(statuses[0]["routes"]) == {
                r.rsplit(":", 1)[0] for r in tab_routes
            }
        assert {
            e["payload"]["id"]
            for e in envelopes
            if e["topic"] == TaskManager.TOPIC_ENTITY_CHANGED
        } == {task_id, child_id}
        if stream_failure:
            assert backend.get(completion_channel) in ("not-a-stream", b"not-a-stream")
        else:
            entries = backend.xrange(completion_channel)
            assert len(entries) == 1
            fields = entries[0][1]
            assert fields.get("m", fields.get(b"m")) in (
                TaskStatus.FAILURE.value,
                TaskStatus.FAILURE.value.encode(),
            )
    finally:
        backend.delete(completion_channel)
        _cleanup(child, task)
