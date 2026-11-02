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
"""Unit tests for ReapOrphanedTasksCommand."""

from collections.abc import Iterator
from contextlib import contextmanager
from unittest.mock import MagicMock, patch
from uuid import UUID

from superset_core.tasks.types import TaskStatus

from superset.commands.tasks.reap import (
    ORPHAN_ERROR_MESSAGE,
    PENDING_ERROR_MESSAGE,
    ReapOrphanedTasksCommand,
)
from superset.utils import json

TEST_UUID = UUID("b8b61b7b-1cd3-4a31-a74a-0a95341afc06")
_UNSET = object()


@contextmanager
def _patched(*, celery_task_id="celery-1", cas_result=True):
    """Patch the reaper's collaborators; yield the mocks it interacts with."""
    stats = MagicMock()
    app = MagicMock()
    app.config = {"STATS_LOGGER": stats, "GTF_ORPHAN_TASK_TIMEOUT": 60}
    properties = json.dumps(
        {"private": {"framework": {"celery_task_id": celery_task_id}}}
        if celery_task_id
        else {}
    )
    with (
        patch("superset.commands.tasks.reap.current_app", app),
        patch("superset.commands.tasks.reap.db") as db,
        patch("superset.daos.tasks.TaskDAO.find_orphaned", return_value=[TEST_UUID]),
        patch(
            "superset.daos.tasks.TaskDAO.conditional_status_update",
            return_value=cas_result,
        ) as cas,
        patch("superset.commands.tasks.reap.celery_app") as celery,
        patch("superset.tasks.manager.TaskManager.publish_completion") as publish,
        patch("superset.tasks.manager.TaskManager.publish_entity_change"),
    ):
        # The pre-CAS properties re-read returns the orphan's current properties.
        db.session.query.return_value.filter.return_value.scalar.return_value = (
            properties
        )
        yield stats, cas, celery, publish


def test_orphan_is_failed_published_and_revoked() -> None:
    """A dead-worker orphan is forced to FAILURE, published, and its job revoked."""
    with _patched() as (stats, cas, celery, publish):
        reaped = ReapOrphanedTasksCommand().run()

    assert reaped == 1
    # Revoke is non-terminating: no forced signal into a (possibly healthy) process.
    celery.control.revoke.assert_called_once_with("celery-1")
    _args, kwargs = cas.call_args
    assert kwargs["properties"]["error_message"] == ORPHAN_ERROR_MESSAGE
    publish.assert_called_once_with(TEST_UUID, TaskStatus.FAILURE.value)
    stats.incr.assert_any_call("gtf.task.orphan_reaped")
    stats.incr.assert_any_call("gtf.task.revoke")


def test_cas_noop_leaves_revived_worker_untouched() -> None:
    """A revived worker that already committed a terminal status wins the CAS race.

    The reaper must not publish — and, critically, must not revoke the job or
    cancel the query, since the worker is healthy (destructive cleanup is gated
    on winning the CAS).
    """
    with _patched(cas_result=False) as (_stats, _cas, celery, publish):
        reaped = ReapOrphanedTasksCommand().run()

    assert reaped == 0
    celery.control.revoke.assert_not_called()
    publish.assert_not_called()


def test_missing_celery_id_skips_revoke_but_still_reaps() -> None:
    """A task with no stored Celery id is still reaped; revoke is simply skipped."""
    with _patched(celery_task_id=None) as (_stats, _cas, celery, publish):
        reaped = ReapOrphanedTasksCommand().run()

    assert reaped == 1
    celery.control.revoke.assert_not_called()
    publish.assert_called_once_with(TEST_UUID, TaskStatus.FAILURE.value)


@contextmanager
def _patched_with_handle(
    properties: str, database: object = _UNSET, *, cas_result: bool = True
):
    """Patch collaborators with a specific serialized `properties` blob."""
    stats = MagicMock()
    app = MagicMock()
    app.config = {"STATS_LOGGER": stats, "GTF_ORPHAN_TASK_TIMEOUT": 60}
    resolved_db = MagicMock() if database is _UNSET else database
    with (
        patch("superset.commands.tasks.reap.current_app", app),
        patch("superset.commands.tasks.reap.db") as db,
        patch("superset.daos.tasks.TaskDAO.find_orphaned", return_value=[TEST_UUID]),
        patch(
            "superset.daos.tasks.TaskDAO.conditional_status_update",
            return_value=cas_result,
        ),
        patch("superset.commands.tasks.reap.celery_app"),
        patch("superset.tasks.manager.TaskManager.publish_completion"),
        patch("superset.tasks.manager.TaskManager.publish_entity_change"),
        patch("superset.tasks.query_cancel.cancel_chart_query") as cancel,
    ):
        db.session.query.return_value.filter.return_value.scalar.return_value = (
            properties
        )
        db.session.get.return_value = resolved_db
        yield cancel, resolved_db


def test_orphaned_query_is_cancelled_when_handle_present() -> None:
    """A persisted cancel handle → the reaper cancels the abandoned query."""
    properties = json.dumps(
        {
            "private": {
                "framework": {"celery_task_id": "c1"},
                "task": {"cancel_query_id": "42", "cancel_database_id": 7},
            }
        }
    )
    with _patched_with_handle(properties) as (cancel, database):
        assert ReapOrphanedTasksCommand().run() == 1

    cancel.assert_called_once_with(database, "42")


def test_no_cancel_when_handle_absent() -> None:
    """No cancel handle in properties → no query cancellation attempted."""
    properties = json.dumps({"private": {"framework": {"celery_task_id": "c1"}}})
    with _patched_with_handle(properties) as (cancel, _db):
        assert ReapOrphanedTasksCommand().run() == 1

    cancel.assert_not_called()


def test_no_cancel_when_database_missing() -> None:
    """Handle present but the database is gone → skip cancel, still reap."""
    properties = json.dumps(
        {"private": {"task": {"cancel_query_id": "42", "cancel_database_id": 7}}}
    )
    with _patched_with_handle(properties, database=None) as (cancel, _db):
        assert ReapOrphanedTasksCommand().run() == 1

    cancel.assert_not_called()


def test_no_cancel_when_worker_wins_cas() -> None:
    """R1: destructive cleanup is gated on winning the CAS.

    A stalled-but-healthy worker that revives and commits its own terminal status
    beats the reaper's CAS; the reaper must then leave its warehouse query alone.
    """
    properties = json.dumps(
        {"private": {"task": {"cancel_query_id": "42", "cancel_database_id": 7}}}
    )
    with _patched_with_handle(properties, cas_result=False) as (cancel, _db):
        assert ReapOrphanedTasksCommand().run() == 0

    cancel.assert_not_called()


def test_reap_orphaned_tasks_beat_task_delegates_to_command() -> None:
    """The reap_orphaned_tasks beat task runs the reaper (its own schedule)."""
    from superset.tasks.scheduler import reap_orphaned_tasks

    with (
        patch("superset.tasks.scheduler.current_app") as app,
        patch("superset.tasks.scheduler.ReapOrphanedTasksCommand") as cmd,
    ):
        app.config = {"STATS_LOGGER": MagicMock()}
        reap_orphaned_tasks()

    cmd.return_value.run.assert_called_once()


def test_pending_expiry_is_disabled_by_default() -> None:
    """Deployments must opt in before the reaper scans never-claimed tasks."""
    from superset import config

    assert config.GTF_PENDING_TASK_TIMEOUT is None
    with (
        _patched(),
        patch("superset.daos.tasks.TaskDAO.find_expired_pending") as find_pending,
        patch("superset.daos.tasks.TaskDAO.fail_expired_pending") as fail_pending,
    ):
        assert ReapOrphanedTasksCommand().run() == 1

    find_pending.assert_not_called()
    fail_pending.assert_not_called()


@contextmanager
def _patched_pending(*, cas_result: bool = True) -> Iterator[tuple[MagicMock, ...]]:
    """Keep the pending path distinct from the existing stale-heartbeat path."""
    stats = MagicMock()
    app = MagicMock()
    app.config = {
        "STATS_LOGGER": stats,
        "GTF_ORPHAN_TASK_TIMEOUT": 60,
        "GTF_PENDING_TASK_TIMEOUT": 1800,
    }
    with (
        patch("superset.commands.tasks.reap.current_app", app),
        patch("superset.commands.tasks.reap.db") as db,
        patch("superset.daos.tasks.TaskDAO.find_orphaned", return_value=[]),
        patch(
            "superset.daos.tasks.TaskDAO.find_expired_pending", return_value=[TEST_UUID]
        ) as find_pending,
        patch(
            "superset.daos.tasks.TaskDAO.fail_expired_pending", return_value=cas_result
        ) as cas,
        patch("superset.commands.tasks.reap.celery_app") as celery,
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
        cancel.assert_not_called()


def test_pending_expiry_publishes_all_terminal_notifications_after_commit() -> None:
    """Queue loss surfaces a normal FAILURE to waiters, rows and downstream DAGs."""
    with _patched_pending() as (stats, cas, celery, publish, entity, dependents, db):
        db.session.commit.side_effect = lambda: publish.assert_not_called()
        assert ReapOrphanedTasksCommand().run() == 1

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
        assert ReapOrphanedTasksCommand().run() == 0

    db.session.commit.assert_not_called()
    db.session.rollback.assert_called_once_with()
    publish.assert_not_called()
    entity.assert_not_called()
    dependents.assert_not_called()
    celery.control.revoke.assert_not_called()
