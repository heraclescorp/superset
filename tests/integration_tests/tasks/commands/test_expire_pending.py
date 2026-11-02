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
from unittest.mock import patch

import pytest
from flask import current_app
from flask_appbuilder.security.sqla.models import User
from superset_core.tasks.types import TaskScope, TaskStatus

from superset import db
from superset.commands.tasks.expire_pending import (
    ExpirePendingTasksCommand,
    PENDING_ERROR_MESSAGE,
)
from superset.daos.tasks import TaskDAO
from superset.models.tasks import Task
from superset.tasks.utils import get_finished_dedup_key


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
