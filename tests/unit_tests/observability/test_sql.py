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
"""Statement identity/privacy and real connector/dialect request-boundary coverage.

The connector test uses the installed deployment-pinned Snowflake cursor, replacing
only its network connection with a deterministic response source. It executes and
fetches real driver result sets, including errors, without a Snowflake account.
"""

from __future__ import annotations

import re
import sqlite3
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, Iterator
from uuid import uuid4

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from superset.observability import get_context, restore_context, sql, telemetry_context
from superset.utils import json


class RecordedOperation:
    def __init__(self, name: str, attributes: dict[str, Any]) -> None:
        self.name = name
        self.attributes = attributes
        self.active = True

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value


@pytest.fixture
def observed(monkeypatch: Any) -> Iterator[list[RecordedOperation]]:
    records: list[RecordedOperation] = []

    @contextmanager
    def record(name: str, **attributes: Any) -> Iterator[RecordedOperation]:
        scope = RecordedOperation(name, {**get_context(), **attributes})
        records.append(scope)
        try:
            yield scope
        except BaseException as error:
            scope.set_attribute("error_type", type(error).__name__)
            raise
        finally:
            scope.active = False

    monkeypatch.setattr(sql, "operation", record)
    monkeypatch.setattr(sql, "is_enabled", lambda: True)
    with restore_context(None):
        yield records


def database() -> Any:
    return SimpleNamespace(
        id=9,
        db_engine_spec=SimpleNamespace(engine="snowflake"),
        is_oauth2_enabled=lambda: False,
    )


def test_statement_ids_native_ids_and_context_survive_cursor_reuse(
    observed: Any,
) -> None:
    cursor = SimpleNamespace(
        sfqid="cancellation-probe-query", connection=SimpleNamespace(session_id=123)
    )
    attempts = []
    with telemetry_context(
        source="chart",
        chart_id=7,
        dashboard_id=8,
        request_actor_user_id=11,
        effective_user_id=12,
        task_id="task-1",
        snowflake_query_id="inherited-old",
    ):
        for index in range(2):
            with sql.observe_statement(
                database(), cursor, f"SELECT {index}", statement_index=index
            ) as statement:
                attempts.append(get_context()["statement_id"])
                assert get_context()["snowflake_query_id"] is None
                with statement.execute():
                    cursor.sfqid = f"native-{index}"
                with statement.fetch() as fetched:
                    fetched.set_attribute("rows", 1)
        assert get_context()["snowflake_query_id"] == "inherited-old"
    assert len(set(attempts)) == 2
    statements = [
        scope.attributes for scope in observed if scope.name == "sql.statement"
    ]
    assert [scope["snowflake_query_id"] for scope in statements] == [
        "native-0",
        "native-1",
    ]
    assert [scope["statement_index"] for scope in statements] == [0, 1]
    assert all(scope["snowflake_session_id"] == "123" for scope in statements)
    assert all(
        scope["request_actor_user_id"] == 11 and scope["effective_user_id"] == 12
        for scope in statements
    )
    assert all(
        scope["chart_id"] == 7 and scope["dashboard_id"] == 8 for scope in statements
    )
    assert "SELECT" not in json.dumps(statements)
    assert get_context() == {}


def test_error_ids_and_codes_are_captured_without_replacing_exception(
    observed: Any,
) -> None:
    cursor = SimpleNamespace(
        sfqid="old-query", connection=SimpleNamespace(session_id=456)
    )
    driver_error = RuntimeError("SELECT private WHERE password='do-not-export'")
    driver_error.sfqid = "failed-native"  # type: ignore[attr-defined]
    driver_error.errno = 1003  # type: ignore[attr-defined]
    wrapped = ValueError("private driver wrapper")
    wrapped.__cause__ = driver_error
    with pytest.raises(ValueError, match=re.escape(str(wrapped))) as caught:  # noqa: PT012
        with sql.observe_statement(database(), cursor, "SELECT 'private'") as statement:
            with statement.execute():
                raise wrapped
    assert caught.value is wrapped
    for scope in observed:
        assert scope.attributes["snowflake_query_id"] == "failed-native"
        assert scope.attributes["error_code"] == "1003"
    encoded = json.dumps([scope.attributes for scope in observed])
    assert "do-not-export" not in encoded
    assert "private" not in encoded


def test_early_execute_failure_never_reuses_previous_native_id(observed: Any) -> None:
    cursor = SimpleNamespace(
        sfqid="previous-statement", connection=SimpleNamespace(session_id=999)
    )
    error = RuntimeError("transport failed")
    error.sfqid = "previous-statement"  # type: ignore[attr-defined]
    with pytest.raises(RuntimeError) as caught:  # noqa: PT012
        with sql.observe_statement(database(), cursor, "SELECT 1") as statement:
            with statement.execute():
                raise error
    assert caught.value is error
    assert all(scope.attributes.get("snowflake_query_id") is None for scope in observed)
    assert all(scope.attributes["snowflake_session_id"] == "999" for scope in observed)


@pytest.mark.parametrize("engine", ["snowflake", "postgresql", "mysql"])
def test_fingerprint_normalizes_literals_comments_and_whitespace(
    observed: Any, engine: str
) -> None:
    first = sql.sql_fingerprint(
        "select amount from sales where owner = 'private' "
        "and amount > 123 -- a secret\n",
        engine,
    )
    second = sql.sql_fingerprint(
        "SELECT amount FROM sales WHERE owner='different' AND amount>456", engine
    )
    assert first is not None
    assert first == second
    assert len(first) == 64
    assert first != sql.sql_fingerprint(
        "SELECT amount FROM other_sales WHERE owner='different' AND amount>456", engine
    )
    assert sql.sql_fingerprint("SELECT :secret_bind", engine) == sql.sql_fingerprint(
        "SELECT :other_bind", engine
    )
    assert sql.sql_fingerprint("SELECT 'unterminated", engine) is None


def test_tags_are_allowlisted_bounded_and_context_local(observed: Any) -> None:
    with telemetry_context(
        source="report",
        statement_id=str(uuid4()),
        chart_id=1,
        dashboard_id=2,
        request_actor_user_id=3,
        effective_user_id=4,
        report_schedule_id=5,
        task_id="task-1",
        sql="SELECT private",
        password="do-not-export",  # noqa: S106
    ):
        tag = sql.statement_query_tag()
        assert tag is not None
        assert len(tag.encode()) <= 1800
        payload = json.loads(tag)
        assert payload["superset_observability"] == 1
        assert payload["source"] == "report"
        assert payload["request_actor_user_id"] == 3
        assert payload["effective_user_id"] == 4
        assert payload["chart_id"] == 1
        assert payload["dashboard_id"] == 2
        assert "sql" not in payload
        assert "password" not in payload
    assert sql.statement_query_tag() is None


def test_disabled_observation_preserves_driver_calls_and_avoids_introspection(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(sql, "is_enabled", lambda: False)

    class Private:
        def __getattribute__(self, name: str) -> Any:
            raise AssertionError(f"disabled telemetry accessed {name}")

    class Cursor:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, Any]]] = []

        def execute(self, query: str, **kwargs: Any) -> None:
            self.calls.append((query, kwargs))

    cursor = Cursor()
    with sql.observe_statement(Private(), Private(), "SELECT private") as statement:
        with statement.execute():
            sql.execute_with_statement_tag(cursor, "SELECT private")
        with statement.fetch():
            pass
    assert cursor.calls == [("SELECT private", {})]
    assert sql.sql_fingerprint("SELECT private", "snowflake") is None
    assert sql.dataframe_attributes(Private()) == {}
    assert (
        sql.execute_tagged_sqlalchemy(Private(), "SELECT private", (), Private())
        is None
    )


def test_dataframe_size_is_native_storage_not_private_serialization(
    observed: Any,
) -> None:
    df = pd.DataFrame({"secret": ["private", "private2"], "amount": [1, 2]})
    assert sql.dataframe_attributes(df) == {
        "rows": 2,
        "result_bytes": int(df.memory_usage(index=True, deep=False).sum()),
    }


@pytest.mark.parametrize(
    "query,expected",
    [
        ("SELECT 1", "select"),
        ("WITH cte AS (SELECT 1) SELECT * FROM cte", "select"),
        ("WITH cte AS (SELECT 1) INSERT INTO destination SELECT * FROM cte", "dml"),
        ("CREATE TABLE destination AS SELECT 1", "ddl"),
        ("UPDATE destination SET x=1", "dml"),
        ("ALTER SESSION SET QUERY_TAG='private'", "session"),
        ("USE WAREHOUSE warehouse_name", "session"),
        ("SHOW TABLES", "metadata"),
        ("CALL custom_procedure()", "other"),
    ],
)
def test_statement_type_distinguishes_workload_from_session_plumbing(
    observed: Any, query: str, expected: str
) -> None:
    with telemetry_context(source="sql_lab"):
        with sql.observe_statement(database(), None, query):
            assert get_context()["statement_type"] == expected
    with sql.observe_statement(
        database(), None, "SELECT CURRENT_SESSION()", source="metadata"
    ):
        assert get_context()["statement_type"] == "metadata"


class DriverConnection:
    """Only the connector's transport boundary; cursor/results remain real."""

    log_max_query_length = 1000
    network_timeout = None
    is_pyformat = False
    client_fetch_threads = 1
    client_prefetch_threads = 1
    client_fetch_use_mp = False
    _reuse_results = False
    _internal_application_name = "Superset"
    _internal_application_version = "test"
    application = "Superset"
    session_id = 987
    telemetry_enabled = False

    def __init__(self) -> None:
        self._session_parameters = {"QUERY_TAG": "operator-default"}
        self._session_manager = SimpleNamespace(clone=lambda: None)
        self.requests: list[dict[str, Any]] = []
        self.messages: list[Any] = []
        self.counter = 0

    def is_closed(self) -> bool:
        return False

    def _format_query_for_log_lazy(self, query: str) -> str:
        return query

    def _next_sequence_counter(self) -> int:
        self.counter += 1
        return self.counter

    def _process_params_qmarks(self, params: Any, cursor: Any) -> Any:
        return None

    def _log_telemetry(self, data: Any) -> None:
        pass

    def cmd_query(
        self, query: str, sequence: int, request_id: Any, **kwargs: Any
    ) -> dict[str, Any]:
        self.requests.append({"sql": query, **kwargs})
        if query == "SELECT broken":
            return {
                "success": False,
                "code": "1003",
                "message": "private backend error",
                "data": {
                    "queryId": f"native-{sequence}",
                    "sqlState": "42000",
                    "total": 0,
                },
            }
        return {
            "success": True,
            "data": {
                "queryId": f"native-{sequence}",
                "rowtype": [],
                "rowset": [[]],
                "total": 1,
                "queryResultFormat": "json",
            },
        }


def test_pinned_snowflake_cursor_transmits_request_tags_and_preserves_session_results(
    observed: Any,
) -> None:
    pytest.importorskip("snowflake.connector")
    from snowflake.connector.cursor import SnowflakeCursor
    from snowflake.connector.errors import ProgrammingError

    from superset.db_engine_specs.snowflake import SnowflakeEngineSpec

    connection = DriverConnection()
    cursor = SnowflakeCursor(connection)  # type: ignore[arg-type]
    attempts = []
    for actor, chart in ((11, 21), (12, 22)):
        with telemetry_context(
            source="chart", request_actor_user_id=actor, chart_id=chart
        ):
            with sql.observe_statement(database(), cursor, "SELECT 1") as statement:
                attempts.append(get_context()["statement_id"])
                with statement.execute():
                    SnowflakeEngineSpec.execute(cursor, "SELECT 1", database())
                with statement.fetch():
                    assert cursor.fetchall() == [()]
        assert connection._session_parameters == {"QUERY_TAG": "operator-default"}
    with pytest.raises(ProgrammingError) as failed_query:  # noqa: PT012
        with sql.observe_statement(database(), cursor, "SELECT broken") as statement:
            with statement.execute():
                SnowflakeEngineSpec.execute(cursor, "SELECT broken", database())
    assert failed_query.value.sfqid == "native-3"
    assert failed_query.value.errno == 1003
    # The same pooled/native cursor's next unobserved call gets no prior tag.
    SnowflakeEngineSpec.execute(cursor, "SELECT 1", database())
    assert cursor.fetchall() == [()]
    tags = [
        json.loads(request["statement_params"]["QUERY_TAG"])
        for request in connection.requests[:3]
    ]
    assert [tag["request_actor_user_id"] for tag in tags[:2]] == [11, 12]
    assert [tag["chart_id"] for tag in tags[:2]] == [21, 22]
    assert [tag["statement_id"] for tag in tags[:2]] == attempts
    assert "QUERY_TAG" not in connection.requests[-1]["statement_params"]
    assert connection._session_parameters == {"QUERY_TAG": "operator-default"}
    assert len(connection.requests) == 4  # No ALTER SESSION/reset/commit SQL.
    failed = [scope for scope in observed if scope.name == "sql.statement"][-1]
    assert failed.attributes["snowflake_query_id"] == "native-3"
    assert failed.attributes["error_code"] == "42000"
    assert "private backend error" not in json.dumps(tags)
    cursor.close()


def test_sqlalchemy_hook_preserves_results_pool_isolation_and_error_ids(
    observed: Any,
) -> None:
    from superset.db_engine_specs.snowflake import SnowflakeEngineSpec

    class Cursor:
        def __init__(self, connection: Any) -> None:
            self.connection = connection
            self.inner = connection.inner.cursor()
            self.sfqid = None

        def __getattr__(self, name: str) -> Any:
            return getattr(self.inner, name)

        def execute(self, query: str, parameters: Any = (), **kwargs: Any) -> Any:
            self.sfqid = str(uuid4())
            self.connection.requests.append({"query": query, **kwargs})
            try:
                return self.inner.execute(query, parameters)
            except sqlite3.Error as error:
                error.sfqid = self.sfqid
                raise

    class Connection:
        session_id = 123

        def __init__(self) -> None:
            self.inner = sqlite3.connect(":memory:")
            self.requests: list[dict[str, Any]] = []

        def __getattr__(self, name: str) -> Any:
            return getattr(self.inner, name)

        def cursor(self) -> Cursor:
            return Cursor(self)

    native = Connection()
    engine = create_engine("sqlite://", creator=lambda: native)
    SnowflakeEngineSpec.register_engine_events(engine)
    try:
        with engine.connect() as connection:
            with telemetry_context(source="export", chart_id=7):
                with sql.observe_statement(database(), None, "SELECT 42") as statement:
                    with statement.execute():
                        result = connection.execution_options(
                            superset_statement_observation=statement
                        ).execute(text("SELECT 42"))
                    with statement.fetch():
                        assert result.fetchall() == [(42,)]
            with pytest.raises(DBAPIError):  # noqa: PT012
                with sql.observe_statement(
                    database(), None, "SELECT missing FROM unknown"
                ) as statement:
                    with statement.execute():
                        connection.execution_options(
                            superset_statement_observation=statement
                        ).execute(text("SELECT missing FROM unknown"))
        with engine.connect() as connection:
            assert connection.execute(text("SELECT 99")).scalar() == 99
        exported = next(
            request for request in native.requests if request["query"] == "SELECT 42"
        )
        assert json.loads(exported["_statement_params"]["QUERY_TAG"])["chart_id"] == 7
        assert "_statement_params" not in native.requests[-1]
        statements = [
            scope.attributes for scope in observed if scope.name == "sql.statement"
        ]
        assert all(scope["snowflake_query_id"] != "123" for scope in statements)
        assert statements[-1]["error_type"] == "OperationalError"
    finally:
        engine.dispose()


def test_streaming_fetch_phase_excludes_generator_consumer_time(
    observed: Any, monkeypatch: Any
) -> None:
    import csv
    import io

    from flask import Flask

    from superset.commands.sql_lab.streaming_export_command import (
        StreamingSqlResultExportCommand,
    )
    from superset.commands.streaming_export import base

    monkeypatch.setattr(base, "operation", sql.operation)
    monkeypatch.setattr(base, "is_enabled", lambda: True)
    batches = iter([[(1,), (2,)], []])
    calls = []

    def fetchmany(size: int) -> list[tuple[int]]:
        assert [scope.name for scope in observed if scope.active] == ["sql.fetch"]
        calls.append(size)
        return next(batches)

    with Flask(__name__).app_context():
        command = StreamingSqlResultExportCommand("client-1", chunk_size=2)
    buffer = io.StringIO()
    chunks = command._process_rows(
        SimpleNamespace(fetchmany=fetchmany), csv.writer(buffer), buffer, None
    )
    data, rows, byte_count = next(chunks)
    assert data == "1\r\n2\r\n"
    assert rows == 2
    assert byte_count == 6
    assert calls == [2, 2]
    assert not any(scope.active for scope in observed)
    assert [scope.attributes["rows"] for scope in observed] == [2, 0]
    assert list(chunks) == []


def test_disabled_snowflake_cancel_helpers_do_not_read_query_metadata(
    monkeypatch: Any,
) -> None:
    from superset.db_engine_specs import snowflake

    monkeypatch.setattr(snowflake, "is_enabled", lambda: False)

    class Query:
        def __getattribute__(self, name: str) -> Any:
            raise AssertionError(f"disabled telemetry read query metadata: {name}")

    statements = []
    cursor = SimpleNamespace(
        execute=statements.append,
        fetchone=lambda: ("session123",),
    )
    assert (
        snowflake.SnowflakeEngineSpec.get_cancel_query_id(cursor, Query())
        == "session123"
    )
    assert (
        snowflake.SnowflakeEngineSpec.cancel_query(cursor, Query(), "session123")
        is True
    )
    assert statements == [
        "SELECT CURRENT_SESSION()",
        "SELECT SYSTEM$CANCEL_ALL_QUERIES(session123)",
    ]


@pytest.mark.parametrize(
    "source,expected",
    [("sql_lab", "export"), ("report", "report"), ("warmup", "warmup")],
)
def test_sql_lab_deferred_export_preserves_request_actor_not_query_owner(
    monkeypatch: Any, source: str, expected: str
) -> None:
    from flask import Flask

    from superset.commands.sql_lab.streaming_export_command import (
        StreamingSqlResultExportCommand,
    )
    from superset.models.sql_lab import Query

    app = Flask(__name__)
    app.config["OBSERVABILITY_ENABLED"] = True
    seen = []
    with (
        app.app_context(),
        telemetry_context(
            source=source, request_actor_user_id=11, effective_user_id=12
        ),
    ):
        command = StreamingSqlResultExportCommand("client-1")
        command._query = Query(id=51, client_id="client-1", user_id=999, database_id=9)
        monkeypatch.setattr(
            command,
            "_get_sql_and_database",
            lambda: ("SELECT 1", database(), None, None),
        )
        monkeypatch.setattr(command, "_get_row_limit", lambda: None)

        def stream(*args: Any) -> Iterator[str]:
            seen.append(get_context())
            yield "csv\n"

        monkeypatch.setattr(command, "_execute_query_and_stream", stream)
        generate = command.run()
    with restore_context(None):
        assert list(generate()) == ["csv\n"]
        assert get_context() == {}
    assert seen[0]["source"] == expected
    assert seen[0]["request_actor_user_id"] == 11
    assert seen[0]["effective_user_id"] == 12
    assert seen[0]["sql_lab_query_id"] == 51
    assert seen[0]["sql_lab_client_id"] == "client-1"
    assert 999 not in seen[0].values()


def test_database_engine_factory_instruments_before_cache_and_handles_real_pool_errors(
    monkeypatch: Any,
) -> None:
    from flask import Flask
    from flask_babel import Babel
    from sqlalchemy.pool import NullPool, QueuePool

    from superset.models import core
    from superset.observability import engine as health

    counters: list[tuple[str, int, dict[str, Any]]] = []
    gauges: list[tuple[str, float, dict[str, Any]]] = []
    events: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(health, "enabled", lambda: True)
    monkeypatch.setattr(
        health,
        "increment",
        lambda name, value=1, **attributes: counters.append((name, value, attributes)),
    )
    monkeypatch.setattr(
        health,
        "gauge",
        lambda name, value, **attributes: gauges.append((name, value, attributes)),
    )
    monkeypatch.setattr(
        health,
        "emit_event",
        lambda name, **attributes: events.append((name, attributes)),
    )

    class PublishingCache(dict):
        def __setitem__(self, key: Any, value: Any) -> None:
            # Registration must finish before another caller can obtain it.
            assert value in health._ENGINES
            super().__setitem__(key, value)

    monkeypatch.setattr(core, "_ENGINE_CACHE", PublishingCache())
    app = Flask(__name__)
    app.config.from_object("superset.config")
    Babel(app)
    app.config["DB_CONNECTION_MUTATOR"] = None
    app.config["PREVENT_UNSAFE_DB_CONNECTIONS"] = False
    database_model = core.Database(
        id=9, database_name="warehouse-health", sqlalchemy_uri="sqlite://"
    )
    monkeypatch.setattr(
        database_model,
        "get_extra",
        lambda source: {"engine_params": {"poolclass": QueuePool}},
    )
    monkeypatch.setattr(database_model, "get_impersonation_username", lambda url: None)
    monkeypatch.setattr(database_model, "get_oauth2_config", lambda: None)
    engines = []
    try:
        with app.app_context():
            pooled = database_model._get_sqla_engine(nullpool=False)
            engines.append(pooled)
            assert database_model._get_sqla_engine(nullpool=False) is pooled
            core.instrument_engine(pooled, role="warehouse", database_id=9)
            assert not counters  # No connection opened by instrumentation.
            with pooled.connect() as connection:
                assert connection.execute(text("SELECT 1")).scalar() == 1
                with pytest.raises(DBAPIError):
                    connection.execute(text("SELECT private_column FROM private_table"))
                connection.invalidate()
            assert (
                sum(value for name, value, _ in counters if name == "db.connections")
                == 1
            )
            assert sum(value for name, value, _ in counters if name == "db.errors") == 1
            assert (
                sum(value for name, value, _ in counters if name == "db.invalidations")
                == 1
            )
            assert {name for name, _, _ in gauges} == {
                "db.pool.checked_out",
                "db.pool.size",
                "db.pool.overflow",
            }
            gauge_count = len(gauges)
            unpooled = database_model._get_sqla_engine(nullpool=True)
            engines.append(unpooled)
            assert isinstance(unpooled.pool, NullPool)
            with unpooled.connect() as connection:
                assert connection.execute(text("SELECT 2")).scalar() == 2
            assert (
                len(gauges) == gauge_count
            )  # Unsupported utilization is absent, not 0.
        assert all(
            attributes == {"role": "warehouse", "db_system": "sqlite", "database_id": 9}
            for _, _, attributes in gauges
        )
        assert all(
            attributes["database_id"] == 9 and attributes["role"] == "warehouse"
            for _, _, attributes in counters
        )
        assert any(name == "db.connection.error" for name, _ in events)
        assert not any(name.startswith("sql.") for name, _ in events)
        encoded = json.dumps(events)
        assert "private_column" not in encoded
        assert "private_table" not in encoded
        assert "sqlite://" not in encoded
    finally:
        for created in engines:
            created.dispose()
