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
"""Unit tests for the GTF executor body (scheduler._execute_task_body)."""

from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from celery.exceptions import Retry
from superset_core.tasks.types import TaskStatus

from superset.tasks.heartbeat import SELF_FENCE_ERROR_MESSAGE


def _run_body_with_raising_executor(
    *,
    timeout_triggered: bool = False,
    abort_detected: bool = False,
    fence_triggered: bool = False,
):
    """Drive _execute_task_body where the task body raises mid-abort/timeout/fence.

    Returns ``(result, transition_mock, task_manager_mock, stats_logger_mock)`` for
    the caller to assert on the terminal handling.
    """
    native = uuid4()
    task = SimpleNamespace(
        uuid=native,
        status=TaskStatus.PENDING.value,
        properties_dict={},  # no "timeout" key -> no timeout timer started
    )
    if fence_triggered:
        final_status = TaskStatus.FAILURE.value
    elif timeout_triggered:
        final_status = TaskStatus.TIMED_OUT.value
    elif abort_detected:
        final_status = TaskStatus.ABORTED.value
    else:
        final_status = TaskStatus.FAILURE.value
    # What the finally block's terminal transition commits, read back at the end.
    refreshed = SimpleNamespace(status=final_status)

    ctx = MagicMock()
    ctx._abort_detected = abort_detected
    ctx.timeout_triggered = timeout_triggered
    ctx.fence_triggered = fence_triggered
    ctx.abort_handlers_completed = True
    ctx.aborting_in_flight = abort_detected or timeout_triggered or fence_triggered
    # Echo the error detail back as a real dict (the real error_properties merges
    # the executor cache with the error message / exception detail).
    ctx.error_properties.side_effect = lambda exception=None, error_message=None: {
        "error_message": error_message if error_message else str(exception)
    }

    transition = MagicMock()
    transition.return_value.run.return_value = True  # PENDING->IN_PROGRESS succeeds
    executor_calls = 0

    def _raise(*_args, **_kwargs):
        nonlocal executor_calls
        executor_calls += 1
        # Simulates the abort handler cancelling the warehouse query, which makes
        # the blocked get_df raise inside the task body.
        raise RuntimeError("query cancelled")

    stats_logger = MagicMock()
    app = MagicMock()
    app.config = {"STATS_LOGGER": stats_logger}

    with (
        patch(
            "superset.tasks.scheduler.TaskDAO.find_one_or_none", return_value=refreshed
        ),
        patch("superset.tasks.scheduler.TaskContext", return_value=ctx),
        patch(
            "superset.tasks.scheduler.TaskRegistry.get_executor", return_value=_raise
        ),
        patch("superset.tasks.scheduler.TaskManager") as task_manager,
        patch("superset.tasks.scheduler.current_app", app),
        patch(
            "superset.commands.tasks.internal_update.InternalStatusTransitionCommand",
            transition,
        ),
    ):
        from superset.tasks.scheduler import _execute_task_body

        # SimpleNamespace stands in for the Task ORM row the body only reads from.
        result = _execute_task_body(task, native, "some.task", (), {}, MagicMock())  # type: ignore[arg-type]
    assert executor_calls == 1
    return result, transition, task_manager, stats_logger


def _requested_statuses(transition: MagicMock):
    return [call.kwargs.get("new_status") for call in transition.call_args_list]


def _find_call(transition: MagicMock, new_status):
    return next(
        (
            c
            for c in transition.call_args_list
            if c.kwargs.get("new_status") == new_status
        ),
        None,
    )


def test_exception_during_timeout_finalizes_timed_out_not_failure() -> None:
    """A body that raises while timing out must end TIMED_OUT, never FAILURE."""
    result, transition, task_manager, stats_logger = _run_body_with_raising_executor(
        timeout_triggered=True, abort_detected=False
    )

    assert TaskStatus.FAILURE not in _requested_statuses(transition)
    # The finally block must actually issue the ABORTING -> TIMED_OUT transition.
    timed_out = _find_call(transition, TaskStatus.TIMED_OUT)
    assert timed_out is not None
    assert timed_out.kwargs.get("expected_status") == TaskStatus.ABORTING
    assert result["status"] == TaskStatus.TIMED_OUT.value
    task_manager.publish_completion.assert_called_once()
    # The failure metric must not fire for a successful cancellation.
    assert ("gtf.task.failure",) not in [
        c.args for c in stats_logger.incr.call_args_list
    ]


def test_exception_during_abort_finalizes_aborted_not_failure() -> None:
    """A body that raises while aborting must end ABORTED, never FAILURE."""
    result, transition, _task_manager, _stats = _run_body_with_raising_executor(
        timeout_triggered=False, abort_detected=True
    )

    assert TaskStatus.FAILURE not in _requested_statuses(transition)
    aborted = _find_call(transition, TaskStatus.ABORTED)
    assert aborted is not None
    assert aborted.kwargs.get("expected_status") == TaskStatus.ABORTING
    assert result["status"] == TaskStatus.ABORTED.value


def test_exception_during_self_fence_finalizes_failure() -> None:
    """A body that raises after the worker self-fenced must end FAILURE."""
    result, transition, task_manager, _stats = _run_body_with_raising_executor(
        fence_triggered=True
    )

    failure = _find_call(transition, TaskStatus.FAILURE)
    assert failure is not None
    # Accepts IN_PROGRESS too: the ABORTING write may have failed under partition.
    assert failure.kwargs.get("expected_status") == [
        TaskStatus.IN_PROGRESS,
        TaskStatus.ABORTING,
    ]
    assert failure.kwargs["properties"]["error_message"] == SELF_FENCE_ERROR_MESSAGE
    assert result["status"] == TaskStatus.FAILURE.value
    task_manager.publish_completion.assert_called_once()


@pytest.fixture
def execution_setup() -> Iterator[SimpleNamespace]:
    """Isolate preparation and recovery without replacing their control flow."""
    from superset.tasks import scheduler

    native = uuid4()
    task = SimpleNamespace(
        id=1,
        uuid=native,
        status=TaskStatus.PENDING.value,
        properties_dict={
            "timeout": 60,
            "private": {
                "framework": {"celery_task_id": "previous-celery-id"},
                "task": {"cancel_query_id": "query-id"},
                "subscription": {"client_refs": {"1": ["chart-id"]}},
            },
        },
    )
    celery = SimpleNamespace(
        request=SimpleNamespace(id="celery-early-failure", retries=0), retry=MagicMock()
    )
    dao = MagicMock()
    dao.find_one_or_none.return_value = task
    dao.conditional_status_update.return_value = True
    database = MagicMock()
    manager = MagicMock()
    app = MagicMock()
    app.config = {"STATS_LOGGER": MagicMock()}
    gate = MagicMock(return_value=None)
    persist = MagicMock()
    heartbeat = MagicMock()
    body = MagicMock(return_value={"status": "success", "task_uuid": str(native)})
    setup = SimpleNamespace(
        native=native,
        task=task,
        celery=celery,
        dao=dao,
        db=database,
        manager=manager,
        gate=gate,
        persist=persist,
        heartbeat=heartbeat,
        body=body,
        lifecycle=scheduler._execute_task_body,
        raw=scheduler.execute_task.run.__func__,
    )
    with patch.multiple(
        scheduler,
        TaskDAO=dao,
        db=database,
        TaskManager=manager,
        current_app=app,
        unmet_prerequisite=gate,
        _persist_celery_task_id=persist,
        task_heartbeat=heartbeat,
        _execute_task_body=body,
    ):
        yield setup


def _execute(setup: SimpleNamespace) -> dict[str, Any]:
    return setup.raw(
        setup.celery,
        str(setup.native),
        "some.task",
        ("private task argument",),
        {"payload": "private task payload"},
    )


@pytest.mark.parametrize(
    "stage", ["load", "dag", "defer", "persist", "heartbeat", "claim"]
)
def test_preexecution_failure_recovers_unclaimed_task(
    execution_setup: SimpleNamespace, stage: str
) -> None:
    from superset.tasks.dependencies import DAG_WAITING

    setup = execution_setup
    error = RuntimeError("early execution failure")
    if stage == "load":
        setup.dao.find_one_or_none.side_effect = [error, setup.task]
    elif stage == "dag":
        setup.gate.side_effect = error
    elif stage == "defer":
        setup.gate.return_value = DAG_WAITING
        setup.celery.retry.side_effect = error
    elif stage == "persist":
        setup.persist.side_effect = error
    elif stage == "heartbeat":
        setup.heartbeat.return_value.__enter__.side_effect = error
    else:
        setup.body.side_effect = setup.lifecycle

    with patch(
        "superset.commands.tasks.internal_update.InternalStatusTransitionCommand"
    ) as claim:
        claim.return_value.run.side_effect = error
        result = _execute(setup)

    assert result == {
        "status": TaskStatus.FAILURE.value,
        "task_uuid": str(setup.native),
    }
    setup.manager.publish_completion.assert_called_once_with(
        setup.native, TaskStatus.FAILURE.value
    )


def test_early_failure_does_not_overwrite_concurrent_claim(
    execution_setup: SimpleNamespace,
) -> None:
    setup = execution_setup
    error = RuntimeError("initial load failed")
    setup.dao.find_one_or_none.side_effect = [error, setup.task]
    setup.dao.conditional_status_update.return_value = False

    with pytest.raises(RuntimeError, match="initial load failed"):
        _execute(setup)

    setup.manager.publish_completion.assert_not_called()
    setup.manager.publish_entity_change.assert_not_called()
    setup.manager.publish_required_by_changed.assert_not_called()


@pytest.mark.parametrize("failed_write", ["read", "update", "commit"])
def test_early_failure_during_metastore_outage_remains_visible(
    execution_setup: SimpleNamespace, failed_write: str
) -> None:
    setup = execution_setup
    initial_error = RuntimeError("initial load failed")
    outage = RuntimeError("metastore unavailable")
    setup.dao.find_one_or_none.side_effect = [
        initial_error,
        outage if failed_write == "read" else setup.task,
    ]
    if failed_write == "update":
        setup.dao.conditional_status_update.side_effect = outage
    elif failed_write == "commit":
        setup.db.session.commit.side_effect = outage

    with patch("superset.tasks.scheduler.logger") as logger:
        with pytest.raises(RuntimeError, match="initial load failed"):
            _execute(setup)

    setup.manager.publish_completion.assert_not_called()
    setup.manager.publish_entity_change.assert_not_called()
    for logged in logger.exception.call_args_list:
        assert setup.native in logged.args
        assert setup.celery.request.id in logged.args
        assert "private task argument" not in str(logged)
        assert "private task payload" not in str(logged)


def test_dag_retry_is_not_an_early_failure(execution_setup: SimpleNamespace) -> None:
    from superset.tasks.dependencies import DAG_WAITING

    setup = execution_setup
    setup.gate.return_value = DAG_WAITING
    setup.celery.retry.side_effect = Retry()

    with pytest.raises(Retry):
        _execute(setup)

    setup.manager.publish_completion.assert_not_called()


def test_executor_error_is_failed_without_retrying_work() -> None:
    """An executor exception runs its body once and follows normal FAILURE cleanup."""
    result, _transition, task_manager, _stats = _run_body_with_raising_executor()

    assert result["status"] == TaskStatus.FAILURE.value
    task_manager.publish_completion.assert_called_once()
