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
"""Unit tests for independent unclaimed pending task expiry."""

from collections.abc import Iterator
from contextlib import contextmanager
from unittest.mock import MagicMock, patch
from uuid import UUID

from superset_core.tasks.types import TaskStatus

from superset.commands.tasks.expire_pending import (
    ExpirePendingTasksCommand,
    PENDING_ERROR_MESSAGE,
)
from superset.utils import json

TEST_UUID = UUID("b8b61b7b-1cd3-4a31-a74a-0a95341afc06")


def test_pending_expiry_is_disabled_by_default() -> None:
    """Deployments must opt in before pending expiry scans never-claimed tasks."""
    from superset import config

    assert config.GTF_PENDING_TASK_TIMEOUT is None
    with (
        patch("superset.commands.tasks.expire_pending.current_app") as app,
        patch("superset.daos.tasks.TaskDAO.find_expired_pending") as find_pending,
        patch("superset.daos.tasks.TaskDAO.fail_expired_pending") as fail_pending,
    ):
        app.config = {"GTF_PENDING_TASK_TIMEOUT": None}
        assert ExpirePendingTasksCommand().run() == 0

    find_pending.assert_not_called()
    fail_pending.assert_not_called()


@contextmanager
def _patched_pending(*, cas_result: bool = True) -> Iterator[tuple[MagicMock, ...]]:
    """Keep the pending path distinct from the existing stale-heartbeat path."""
    stats = MagicMock()
    app = MagicMock()
    app.config = {
        "STATS_LOGGER": stats,
        "GTF_PENDING_TASK_TIMEOUT": 1800,
    }
    with (
        patch("superset.commands.tasks.expire_pending.current_app", app),
        patch("superset.commands.tasks.expire_pending.db") as db,
        patch("superset.daos.tasks.TaskDAO.find_orphaned") as find_orphaned,
        patch(
            "superset.daos.tasks.TaskDAO.find_expired_pending", return_value=[TEST_UUID]
        ) as find_pending,
        patch(
            "superset.daos.tasks.TaskDAO.fail_expired_pending", return_value=cas_result
        ) as cas,
        patch("superset.commands.tasks.expire_pending.celery_app") as celery,
        patch("superset.tasks.manager.TaskManager.publish_completion") as publish,
        patch("superset.tasks.manager.TaskManager.publish_entity_change") as entity,
        patch(
            "superset.tasks.manager.TaskManager.publish_required_by_changed"
        ) as dependents,
        patch("superset.tasks.query_cancel.cancel_chart_query") as cancel,
    ):
        db.session.query.return_value.filter.return_value.scalar.return_value = (
            json.dumps(
                {
                    "is_abortable": False,
                    "private": {
                        "framework": {"celery_task_id": "queued-delivery"},
                        "subscription": {"consumers": ["user:1:tabA"]},
                        "task": {"cancel_database_id": 1, "cancel_query_id": "unused"},
                    },
                }
            )
        )
        yield stats, cas, celery, publish, entity, dependents, db
        find_pending.assert_called_once_with(1800)
        find_orphaned.assert_not_called()
        cancel.assert_not_called()


def test_pending_expiry_publishes_all_terminal_notifications_after_commit() -> None:
    """Queue loss surfaces a normal FAILURE to waiters, rows and downstream DAGs."""
    with _patched_pending() as (stats, cas, celery, publish, entity, dependents, db):

        def assert_no_terminal_side_effects() -> None:
            publish.assert_not_called()
            entity.assert_not_called()
            dependents.assert_not_called()
            celery.control.revoke.assert_not_called()

        db.session.commit.side_effect = assert_no_terminal_side_effects
        assert ExpirePendingTasksCommand().run() == 1

    db.session.commit.assert_called_once_with()
    cas.assert_called_once()
    task_uuid, timeout, properties = cas.call_args.args
    assert (task_uuid, timeout) == (TEST_UUID, 1800)
    assert properties["error_message"] == PENDING_ERROR_MESSAGE
    assert properties["is_abortable"] is False
    assert properties["private"]["framework"] == {
        "celery_task_id": "queued-delivery",
        "exception_type": "PendingTaskTimeoutError",
    }
    assert properties["private"]["subscription"] == {"consumers": ["user:1:tabA"]}
    assert properties["private"]["task"] == {
        "cancel_database_id": 1,
        "cancel_query_id": "unused",
    }
    publish.assert_called_once_with(TEST_UUID, TaskStatus.FAILURE.value)
    entity.assert_called_once_with(TEST_UUID)
    dependents.assert_called_once_with(TEST_UUID)
    celery.control.revoke.assert_called_once_with("queued-delivery")
    stats.incr.assert_any_call("gtf.task.pending_expired")


def test_pending_candidate_claimed_before_update_has_no_terminal_side_effects() -> None:
    """Losing the full eligibility guard must not notify or revoke a fresh worker."""
    with _patched_pending(cas_result=False) as (
        _stats,
        _cas,
        celery,
        publish,
        entity,
        dependents,
        db,
    ):
        assert ExpirePendingTasksCommand().run() == 0

    db.session.commit.assert_not_called()
    db.session.rollback.assert_called_once_with()
    publish.assert_not_called()
    entity.assert_not_called()
    dependents.assert_not_called()
    celery.control.revoke.assert_not_called()


