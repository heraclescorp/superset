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
"""Metastore regressions for failures before a GTF worker claims its task."""

from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session
from superset_core.tasks.types import TaskScope, TaskStatus

from superset import db
from superset.daos.tasks import TaskDAO
from superset.models.tasks import Task
from superset.tasks import scheduler
from superset.tasks.utils import naive_utcnow


@pytest.fixture
def queued_task(app_context: Any, get_user: Any) -> Iterator[Task]:
    """Persist a queued task with subscriber routing and internal handles."""
    admin = get_user("admin")
    task = TaskDAO.create_task(
        task_type="preclaim_recovery_test",
        task_key=uuid4().hex,
        scope=TaskScope.PRIVATE,
        user_id=admin.id,
        properties={
            "timeout": 60,
            "private": {
                "framework": {"celery_task_id": "original-celery-id"},
                "task": {"cancel_query_id": "original-query-id"},
                "subscription": {"client_refs": {str(admin.id): ["chart-id"]}},
            },
        },
    )
    task.created_by = admin
    db.session.commit()
    task_uuid = task.uuid
    try:
        yield task
    finally:
        db.session.rollback()
        persisted = TaskDAO.find_one_or_none(uuid=task_uuid, skip_base_filter=True)
        if persisted:
            db.session.delete(persisted)
            db.session.commit()


def _execute(task_uuid: str) -> dict[str, Any]:
    celery = SimpleNamespace(
        request=SimpleNamespace(id="recovery-celery-id", retries=0)
    )
    return scheduler.execute_task.run.__func__(
        celery, task_uuid, "preclaim_recovery_test", (), {}
    )


def test_failed_initial_select_recovers_session_and_releases_dedup(
    queued_task: Task,
) -> None:
    """A genuine SQL error must rollback before the terminal write and notifications."""
    task = queued_task
    task_uuid = task.uuid
    user_id = task.user_id
    original_dedup = task.dedup_key
    original_properties = task.properties_dict
    find = TaskDAO.find_one_or_none
    first_read = True

    def failing_initial_read(*args: Any, **kwargs: Any) -> Task | None:
        nonlocal first_read
        if first_read:
            first_read = False
            db.session.execute(text("SELECT * FROM nonexistent_gtf_recovery_table"))
        return find(*args, **kwargs)

    def assert_committed(*args: Any) -> None:
        with Session(db.engine) as independent:
            persisted = independent.query(Task).filter_by(uuid=task_uuid).one()
            assert persisted.status == TaskStatus.FAILURE.value
            assert persisted.dedup_key != original_dedup
            assert persisted.ended_at is not None

    replacement = None
    try:
        with (
            patch.object(TaskDAO, "find_one_or_none", side_effect=failing_initial_read),
            patch.object(scheduler, "TaskManager") as manager,
        ):
            manager.publish_entity_change.side_effect = assert_committed
            manager.publish_required_by_changed.side_effect = assert_committed
            manager.publish_completion.side_effect = assert_committed
            assert _execute(str(task_uuid)) == {
                "status": TaskStatus.FAILURE.value,
                "task_uuid": str(task_uuid),
            }

        manager.publish_entity_change.assert_called_once_with(task_uuid)
        manager.publish_required_by_changed.assert_called_once_with(task_uuid)
        manager.publish_completion.assert_called_once_with(
            task_uuid, TaskStatus.FAILURE.value
        )
        db.session.refresh(task)
        assert task.started_at is None
        assert task.last_heartbeat is None
        assert task.has_subscriber(user_id)
        assert (
            task.properties_dict["private"]["task"]
            == original_properties["private"]["task"]
        )
        assert (
            task.properties_dict["private"]["subscription"]
            == (original_properties["private"]["subscription"])
        )
        assert task.properties_dict["private"]["framework"]["celery_task_id"] == (
            "original-celery-id"
        )
        assert "error_message" in task.properties_dict
        replacement = TaskDAO.create_task(
            task_type=task.task_type,
            task_key=task.task_key,
            scope=task.scope,
            user_id=user_id,
        )
        db.session.commit()
        assert replacement.dedup_key == original_dedup
    finally:
        if replacement is not None:
            db.session.delete(replacement)
            db.session.commit()


@pytest.mark.parametrize(
    "claim", ["in_progress", "started_at", "heartbeat", "aborting"]
)
def test_early_failure_atomic_guard_preserves_concurrent_claim(
    queued_task: Task, claim: str
) -> None:
    """A claim committed between recovery's read and UPDATE must win atomically."""
    task = queued_task
    task_uuid = task.uuid
    original_dedup = task.dedup_key
    original_properties = task.properties_dict
    find = TaskDAO.find_one_or_none
    update = TaskDAO.conditional_status_update
    first_read = True
    claimed_status = claim if claim in {"in_progress", "aborting"} else "pending"

    def failing_initial_read(*args: Any, **kwargs: Any) -> Task | None:
        nonlocal first_read
        if first_read:
            first_read = False
            raise RuntimeError("initial load failed")
        return find(*args, **kwargs)

    def claim_before_failure_update(*args: Any, **kwargs: Any) -> bool:
        db.session.rollback()
        with Session(db.engine) as independent:
            values: dict[str, Any] = {"status": claimed_status}
            if claim == "started_at":
                values["started_at"] = naive_utcnow()
            elif claim == "heartbeat":
                values["last_heartbeat"] = naive_utcnow()
            independent.query(Task).filter_by(uuid=task_uuid).update(
                values, synchronize_session=False
            )
            independent.commit()
        return update(*args, **kwargs)

    with (
        patch.object(TaskDAO, "find_one_or_none", side_effect=failing_initial_read),
        patch.object(
            TaskDAO,
            "conditional_status_update",
            side_effect=claim_before_failure_update,
        ),
        patch.object(scheduler, "TaskManager") as manager,
    ):
        with pytest.raises(RuntimeError, match="initial load failed"):
            _execute(str(task_uuid))

    db.session.refresh(task)
    assert task.status == claimed_status
    assert task.dedup_key == original_dedup
    assert task.ended_at is None
    assert task.properties_dict == original_properties
    manager.publish_completion.assert_not_called()
    manager.publish_entity_change.assert_not_called()
    manager.publish_required_by_changed.assert_not_called()
