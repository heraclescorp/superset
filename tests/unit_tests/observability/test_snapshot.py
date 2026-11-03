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
"""Real SQL aggregates, real Kombu queues and bounded control coordination."""

from datetime import datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
import sqlalchemy as sa
from celery import Celery
from flask import Flask
from kombu import Queue
from sqlalchemy.orm import Session

from superset.coordination.base import CoordinationService
from superset.models.task_dependencies import TaskDependency
from superset.models.tasks import Task
from superset.observability import events, metrics, snapshot


@pytest.fixture(autouse=True)
def no_exporters(mocker):
    """SQL/broker behavior is real; unit tests never contact an OTLP endpoint."""
    mocker.patch.object(events, "enabled", return_value=False)
    mocker.patch.object(metrics, "get_runtime", return_value=None)


def test_native_snapshot_uses_runnable_age_and_one_aggregate_query(mocker):
    engine = sa.create_engine("sqlite://")
    Task.__table__.create(engine)
    TaskDependency.__table__.create(engine)
    session = Session(engine)
    now = datetime(2026, 11, 3, 12, 0)
    mocker.patch.object(snapshot, "db", SimpleNamespace(session=session))
    mocker.patch.object(snapshot.TaskDAO, "_db_utcnow", return_value=now)
    measurements = []
    mocker.patch.object(
        snapshot,
        "gauge",
        side_effect=lambda name, value, **attrs: measurements.append(
            (name, value, attrs)
        ),
    )

    def task(identifier, status, created, started=None, ended=None):
        session.execute(
            Task.__table__.insert().values(
                id=identifier,
                uuid=uuid4(),
                task_key=str(identifier),
                dedup_key=str(identifier),
                task_type="chart_data",
                scope="shared",
                status=status,
                created_on=now - timedelta(seconds=created),
                created_by_fk=None,
                changed_by_fk=None,
                changed_on=now - timedelta(seconds=created),
                started_at=now - timedelta(seconds=started)
                if started is not None
                else None,
                ended_at=now - timedelta(seconds=ended) if ended is not None else None,
            )
        )

    task(1, "pending", 120)
    task(2, "pending", 300)
    task(3, "pending", 300)
    task(4, "pending", 200)
    task(5, "in_progress", 600, started=60)
    task(6, "aborting", 70, started=30)
    task(7, "success", 500, started=400, ended=15)
    task(8, "success", 500, started=400, ended=30)
    task(9, "failure", 500, started=400, ended=10)
    task(10, "pending", 50, started=40)
    session.execute(
        TaskDependency.__table__.insert(),
        [
            {
                "task_id": task_id,
                "depends_on_task_id": parent_id,
                "created_by_fk": None,
                "changed_by_fk": None,
            }
            for task_id, parent_id in ((2, 5), (3, 7), (3, 8), (4, 9))
        ],
    )
    session.commit()
    statements = []
    sa.event.listen(
        engine, "before_cursor_execute", lambda *args: statements.append(args[2])
    )

    result = {
        (row["task_status"], row["dependency_state"]): row
        for row in snapshot._native_snapshot()
    }
    assert len(statements) == 1  # No ORM row/subscriber/dependency hydration.
    assert result[("pending", "runnable")]["count"] == 2
    assert result[("pending", "runnable")]["oldest_age_seconds"] == 120
    assert result[("pending", "dependency_waiting")]["oldest_age_seconds"] == 300
    assert result[("pending", "dependency_failed")]["count"] == 1
    assert result[("pending", "claimed")]["count"] == 1
    assert result[("in_progress", "running")]["oldest_age_seconds"] == 60
    assert result[("aborting", "running")]["oldest_age_seconds"] == 30

    # The dependent has been pending 300s, but has only been runnable since the
    # last of its successful prerequisites ended 15s ago.
    session.execute(
        sa.update(Task.__table__)
        .where(Task.id == 1)
        .values(status="success", changed_by_fk=None)
    )
    runnable = next(
        row
        for row in snapshot._native_snapshot()
        if row["dependency_state"] == "runnable"
    )
    assert runnable["count"] == 1
    assert runnable["oldest_age_seconds"] == 15
    session.execute(
        sa.update(Task.__table__).values(status="success", changed_by_fk=None)
    )
    drained = snapshot._native_snapshot()
    assert all(row["count"] == 0 and row["oldest_age_seconds"] == 0 for row in drained)
    assert measurements == []  # Collecting never changes the prior good snapshot.
    snapshot._publish_snapshot("metastore", drained)
    assert any(
        name == "task.oldest_age" and value == 0 for name, value, _ in measurements
    )
    session.close()
    engine.dispose()


def test_broker_ready_counts_use_configured_queue_names(mocker):
    app = Celery("snapshot-unit-test", broker="memory://", set_as_current=False)
    queues = (Queue("unit-control"), Queue("unit-reports"))
    app.conf.update(task_queues=queues, task_default_queue="unit-control")
    mocker.patch.object(snapshot, "celery_app", app)
    mocker.patch.object(snapshot, "gauge")
    # Real Kombu virtual broker: no Celery result-backend counts or guessed
    # Redis keys. Reserved/active jobs are deliberately not counted as ready.
    with app.connection_for_write() as connection:
        with connection.channel() as channel:
            for queue in queues:
                queue(channel).declare()
    app.send_task("unit.report", queue="unit-reports", args=["private payload"])
    app.send_task("unit.control", queue="unit-control")
    assert snapshot._broker_snapshot() == {"unit-control": 1, "unit-reports": 1}
    with app.connection_for_write() as connection:
        with connection.channel() as channel:
            channel.queue_delete("unit-control")
            channel.queue_delete("unit-reports")
    app.close()


def test_active_reserved_and_scheduled_are_not_merged(mocker):
    queues = {
        name: Queue(name, routing_key=name)
        for name in ("queries", "reports", "control")
    }
    mocker.patch.object(
        snapshot, "celery_app", SimpleNamespace(amqp=SimpleNamespace(queues=queues))
    )
    mocker.patch.object(snapshot, "gauge")
    inspector = SimpleNamespace(
        active=lambda: {
            "worker": [{"delivery_info": {"routing_key": "queries"}, "args": "private"}]
        },
        reserved=lambda: {"worker": [{"delivery_info": {"routing_key": "reports"}}]},
        scheduled=lambda: {
            "worker": [
                {
                    "eta": "future",
                    "request": {"delivery_info": {"routing_key": "control"}},
                }
            ]
        },
    )
    result = snapshot._worker_states(lambda state: getattr(inspector, state)())
    assert result["active"]["queries"] == 1
    assert result["reserved"]["queries"] == 0
    assert result["reserved"]["reports"] == 1
    assert result["scheduled"]["control"] == 1
    assert result["active"]["control"] == 0
    inspector.active = inspector.reserved = inspector.scheduled = lambda: None
    with pytest.raises(TimeoutError):
        snapshot._worker_states(lambda state: getattr(inspector, state)())


def test_coordination_lease_suppresses_duplicate_snapshot_deliveries(mocker):
    app = Flask(__name__)
    app.config["OBSERVABILITY_ENABLED"] = True
    stored = {}
    clock = [0]
    mocker.patch.object(CoordinationService, "is_backend_defined", return_value=True)

    def set_value(key, value, *, ttl, if_absent):
        if key in stored and stored[key][1] > clock[0] and if_absent:
            return False
        stored[key] = (value, clock[0] + ttl)
        return True

    mocker.patch.object(CoordinationService, "set_value", side_effect=set_value)
    calls = []
    for name in ("_native_snapshot", "_broker_snapshot", "_worker_snapshot"):
        mocker.patch.object(
            snapshot, name, side_effect=lambda: calls.append("collected") or {}
        )
    with app.app_context():
        assert snapshot.collect_snapshot()["outcome"] == "success"
        assert snapshot.collect_snapshot()["outcome"] == "skipped"
        assert len(calls) == 3
        clock[0] += snapshot.SNAPSHOT_INTERVAL_SECONDS + 1
        assert snapshot.collect_snapshot()["outcome"] == "success"
        assert len(calls) == 6


def test_unavailable_metastore_does_not_report_zero_or_block_other_sources(mocker):
    app = Flask(__name__)
    app.config["OBSERVABILITY_ENABLED"] = True
    mocker.patch.object(snapshot.AcquireDistributedLock, "run")
    mocker.patch.object(
        snapshot, "_native_snapshot", side_effect=ConnectionError("secret URL")
    )
    mocker.patch.object(snapshot, "_broker_snapshot", return_value={"queries": 2})
    mocker.patch.object(
        snapshot,
        "_worker_snapshot",
        return_value={
            "active": {"queries": 1},
            "reserved": {"queries": 0},
            "scheduled": {"queries": 0},
        },
    )
    db = mocker.patch.object(snapshot, "db")
    gauges = mocker.patch.object(snapshot, "gauge")
    events = mocker.patch.object(snapshot, "emit_event")
    with app.app_context():
        result = snapshot.collect_snapshot()
    assert result["outcome"] == "partial"
    assert "metastore" not in result
    assert result["broker"] == {"queries": 2}
    db.session.rollback.assert_called_once()
    assert all(call.args[0] != "task.backlog" for call in gauges.call_args_list)
    timestamps = [
        call
        for call in gauges.call_args_list
        if call.args[0] == "observability.snapshot.timestamp"
    ]
    assert {call.kwargs["phase"] for call in timestamps} == {"broker", "workers"}
    attempts = [
        call
        for call in gauges.call_args_list
        if call.args[0] == "observability.snapshot.attempt_timestamp"
    ]
    assert {call.kwargs["phase"] for call in attempts} == {
        "metastore",
        "broker",
        "workers",
    }
    assert "secret URL" not in str(events.call_args_list)


def test_snapshot_honors_soft_deadline_instead_of_continuing_remote_calls(mocker):
    from celery.exceptions import SoftTimeLimitExceeded

    app = Flask(__name__)
    app.config["OBSERVABILITY_ENABLED"] = True
    mocker.patch.object(snapshot.AcquireDistributedLock, "run")
    mocker.patch.object(
        snapshot, "_native_snapshot", side_effect=SoftTimeLimitExceeded()
    )
    broker = mocker.patch.object(snapshot, "_broker_snapshot")
    with app.app_context(), pytest.raises(SoftTimeLimitExceeded):
        snapshot.collect_snapshot()
    broker.assert_not_called()


def test_pidbox_publisher_never_retries_even_when_mailbox_requests_it(mocker):
    app = Celery("snapshot-bounded-publish", broker="memory://", set_as_current=False)
    with app.connection_for_write() as connection:
        with connection.channel() as channel:
            ensure = mocker.spy(connection, "ensure")
            publish = mocker.patch.object(
                channel, "basic_publish", side_effect=ConnectionError("offline")
            )
            producer = snapshot._SnapshotProducer(channel, auto_declare=False)
            with pytest.raises(ConnectionError):
                producer.publish({"method": "active"}, retry=True)
            publish.assert_called_once()
            ensure.assert_not_called()
    app.close()


def test_incomplete_worker_phase_does_not_publish_partial_state(mocker):
    queues = {"queries": Queue("queries", routing_key="queries")}
    mocker.patch.object(
        snapshot, "celery_app", SimpleNamespace(amqp=SimpleNamespace(queues=queues))
    )
    published = mocker.patch.object(snapshot, "gauge")

    def read_state(state):
        if state == "active":
            return {"worker": [{"delivery_info": {"routing_key": "queries"}}]}
        return None

    with pytest.raises(TimeoutError):
        snapshot._worker_states(read_state)
    published.assert_not_called()


def test_failed_new_writer_availability_retains_last_complete_success(mocker):
    """Model independent PID writers; real-prefork coverage belongs to control smoke."""
    from itertools import count

    app = Flask(__name__)
    app.config["OBSERVABILITY_ENABLED"] = True
    mocker.patch.object(snapshot.AcquireDistributedLock, "run")
    mocker.patch.object(snapshot, "db")
    clock = count(100)
    mocker.patch.object(snapshot, "time", SimpleNamespace(time=lambda: next(clock)))
    writer = ["child-a"]
    values = {}

    def record(name, value, **attributes):
        values[(writer[0], name, tuple(sorted(attributes.items())))] = value

    mocker.patch.object(snapshot, "gauge", side_effect=record)

    def metric(child, name, phase, **attributes):
        return values[
            (child, name, tuple(sorted({"phase": phase, **attributes}.items())))
        ]

    def newest(name, phase):
        candidates = [
            child
            for child in ("child-a", "child-b")
            if (child, name, (("phase", phase),)) in values
        ]
        return max(candidates, key=lambda child: metric(child, name, phase))

    def native_rows(pending):
        return [
            {
                "task_status": "pending",
                "dependency_state": "runnable",
                "count": pending,
                "oldest_age_seconds": pending * 10,
            }
        ]

    native = mocker.patch.object(
        snapshot, "_native_snapshot", return_value=native_rows(5)
    )
    broker = mocker.patch.object(
        snapshot, "_broker_snapshot", return_value={"queries": 7}
    )
    workers = mocker.patch.object(
        snapshot,
        "_worker_snapshot",
        return_value={
            "active": {"queries": 3},
            "reserved": {"queries": 0},
            "scheduled": {"queries": 0},
        },
    )
    phases = ("metastore", "broker", "workers")
    with app.app_context():
        assert snapshot.collect_snapshot()["outcome"] == "success"
        writer[0] = "child-b"

        def unavailable():
            # The attempt is already visible and cannot inherit a prior 1.
            assert (
                metric("child-b", "observability.snapshot.available", "metastore") == 0
            )
            assert (
                metric(
                    "child-b", "observability.snapshot.attempt_timestamp", "metastore"
                )
                > 0
            )
            raise ConnectionError("offline")

        native.side_effect = unavailable
        broker.side_effect = workers.side_effect = ConnectionError("offline")
        assert snapshot.collect_snapshot()["outcome"] == "partial"
        for phase in phases:
            attempted = newest("observability.snapshot.attempt_timestamp", phase)
            successful = newest("observability.snapshot.timestamp", phase)
            assert attempted == "child-b"
            assert metric(attempted, "observability.snapshot.available", phase) == 0
            assert successful == "child-a"
        assert not any(
            child == "child-b" and name == "celery.queue.depth"
            for child, name, _ in values
        )
        assert (
            metric(
                "child-a",
                "task.backlog",
                "metastore",
                task_status="pending",
                dependency_state="runnable",
            )
            == 5
        )

        native.side_effect = broker.side_effect = workers.side_effect = None
        native.return_value = native_rows(2)
        broker.return_value = {"queries": 1}
        workers.return_value = {
            "active": {"queries": 0},
            "reserved": {"queries": 0},
            "scheduled": {"queries": 0},
        }
        assert snapshot.collect_snapshot()["outcome"] == "success"
        selected = newest("observability.snapshot.timestamp", "metastore")
        assert selected == "child-b"
        # Correct projection is 2, not max(old,new)=5 or sum(old,new)=7.
        assert (
            metric(
                selected,
                "task.backlog",
                "metastore",
                task_status="pending",
                dependency_state="runnable",
            )
            == 2
        )
        assert (
            metric(
                selected,
                "task.oldest_age",
                "metastore",
                task_status="pending",
                dependency_state="runnable",
            )
            == 20
        )
        assert metric("child-b", "observability.snapshot.available", "metastore") == 1
        previous_success = metric(
            "child-b", "observability.snapshot.timestamp", "metastore"
        )
        native.side_effect = ConnectionError("offline again")
        assert snapshot.collect_snapshot()["outcome"] == "partial"
        assert (
            metric("child-b", "observability.snapshot.timestamp", "metastore")
            == previous_success
        )
        assert (
            metric("child-b", "observability.snapshot.attempt_timestamp", "metastore")
            > previous_success
        )
        assert metric("child-b", "observability.snapshot.available", "metastore") == 0
        assert metric("child-b", "observability.snapshot.available", "broker") == 1
        assert (
            metric(
                "child-b",
                "task.backlog",
                "metastore",
                task_status="pending",
                dependency_state="runnable",
            )
            == 2
        )


def test_later_broker_queue_failure_never_publishes_partial_ready_state(mocker):
    channel = mocker.MagicMock()
    channel.__enter__.return_value = channel
    channel.queue_declare.side_effect = [
        SimpleNamespace(message_count=9),
        ConnectionError("offline"),
    ]
    connection = mocker.MagicMock()
    connection.__enter__.return_value = connection
    connection.channel.return_value = channel
    connection.transport.driver_type = "amqp"
    app = SimpleNamespace(
        amqp=SimpleNamespace(
            queues={"queries": Queue("queries"), "reports": Queue("reports")}
        ),
        conf=SimpleNamespace(broker_transport_options={}),
        connection_for_read=lambda **_: connection,
    )
    mocker.patch.object(snapshot, "celery_app", app)
    published = mocker.patch.object(snapshot, "gauge")
    with pytest.raises(ConnectionError):
        snapshot._broker_snapshot()
    assert channel.queue_declare.call_count == 2
    published.assert_not_called()
