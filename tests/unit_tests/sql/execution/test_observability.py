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

"""Exercise SQL Lab telemetry at execution boundaries, not by inspecting source."""

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, PropertyMock

import pytest
from celery.exceptions import SoftTimeLimitExceeded
from flask import current_app
from pytest_mock import MockerFixture
from superset_core.queries.types import CacheOptions, QueryOptions, QueryStatus

from superset.common.db_query_status import QueryStatus as LegacyQueryStatus
from superset.constants import QUERY_CANCEL_KEY
from superset.db_engine_specs.base import BaseEngineSpec
from superset.exceptions import OAuth2RedirectError, SupersetErrorException
from superset.observability import get_context, telemetry_context
from superset.sql.execution.executor import (
    execute_sql_with_cursor,
    sql_lab_query_context,
    SQLExecutor,
)
from superset.sql.parse import SQLScript
from superset.sql_lab import (
    execute_query,
    execute_sql_statements,
    SqlLabException,
    SqlLabQueryStoppedException,
)
from superset.utils import json  # noqa: TID251


class NativeIdEngineSpec(BaseEngineSpec):
    """Use the ordinary DB-API path with Snowflake-shaped native identifiers."""

    engine = "snowflake"


class NativeFailureError(RuntimeError):
    """Model an exception carrying a driver's fresh query ID."""

    def __init__(self, query_id: str | None = None) -> None:
        super().__init__("private-driver-error-message")
        self.sfqid = query_id


@pytest.fixture
def sql_events(mocker: MockerFixture, app_context: None) -> list[dict[str, Any]]:
    """Decode real sanitized event envelopes without starting an OTLP exporter."""
    events: list[dict[str, Any]] = []
    mocker.patch("superset.observability.events.enabled", return_value=True)
    mocker.patch("superset.observability.events.get_runtime", return_value=None)
    mocker.patch("superset.observability.metrics.get_runtime", return_value=None)
    mocker.patch("superset.observability.enabled", return_value=True)
    mocker.patch.dict(current_app.config, {"OBSERVABILITY_ENABLED": True})
    mocker.patch("superset.observability.events.observe_operation")
    mocker.patch("superset.observability.events.configure_event_logger")
    mocker.patch(
        "superset.observability.events._LOGGER.info",
        side_effect=lambda message: events.append(json.loads(message)),
    )
    return events


@pytest.fixture
def observation_query(
    mocker: MockerFixture,
    mock_database: MagicMock,
    mock_query: MagicMock,
    app_context: None,
) -> MagicMock:
    """Provide real result-set conversion and fake only the external boundaries."""
    mock_database.db_engine_spec = NativeIdEngineSpec
    mock_database.is_oauth2_enabled.return_value = False
    mock_database.allow_run_async = False
    mock_database.allow_dml = True
    mock_query.database = mock_database
    mock_query.client_id = "sql-client-123"
    mock_query.user_id = 42
    mock_query.status = LegacyQueryStatus.RUNNING
    mock_query.limit = None
    mock_query.select_as_cta_used = False
    mock_query.set_extra_json_key.side_effect = mock_query.extra.__setitem__
    mocker.patch("superset.sql_lab.warm_and_release_connection")
    mocker.patch("superset.sql_lab.is_feature_enabled", return_value=False)
    mocker.patch.dict(
        current_app.config,
        {
            "QUERY_LOGGER": None,
            "SQL_QUERY_MUTATOR": None,
            "SQL_MAX_ROW": None,
            "MUTATE_AFTER_SPLIT": False,
            "DISALLOWED_SQL_FUNCTIONS": {},
            "DISALLOWED_SQL_TABLES": {},
            "SQLLAB_PAYLOAD_MAX_MB": None,
        },
    )
    return mock_query


def make_cursor() -> MagicMock:
    """Reuse one cursor, replacing its native ID and rows on every execute."""
    cursor = MagicMock()
    cursor.sfqid = "stale-query-before-reuse"
    cursor.connection.session_id = 700
    cursor.description = [("value", None, None, None, None, None, None)]
    cursor.rowcount = 1
    executions: list[str] = []

    def execute(sql: str) -> None:
        executions.append(sql)
        cursor.sfqid = f"native-query-{len(executions)}"
        cursor.description = (
            None
            if sql.upper().startswith("INSERT")
            else [("value", None, None, None, None, None, None)]
        )
        cursor.fetchall.return_value = [(len(executions),)]

    cursor.execute.side_effect = execute
    return cursor


def finished(events: list[dict[str, Any]], name: str) -> list[dict[str, Any]]:
    """Select completed operations from their serialized telemetry envelopes."""
    return [
        event["metadata"] for event in events if event["message"] == f"{name}.finished"
    ]


@pytest.mark.parametrize("custom", [False, True])
def test_statement_native_ids_timings_results_and_context(
    mocker: MockerFixture,
    observation_query: MagicMock,
    mock_db_session: MagicMock,
    sql_events: list[dict[str, Any]],
    custom: bool,
) -> None:
    """Every physical statement retains its own native ID and original SQL timing."""
    cursor = make_cursor()
    database = observation_query.database
    default_execute = mocker.spy(NativeIdEngineSpec, "execute")
    custom_execute = MagicMock(side_effect=lambda cursor, sql: cursor.execute(sql))
    mocker.patch(
        "superset.sql.execution.executor.time",
        SimpleNamespace(time=MagicMock(side_effect=[10.0, 10.125, 20.0, 20.25])),
    )
    sql = ["SELECT 'private-sql-payload'", "SELECT 2"]
    with telemetry_context(
        source="report",
        request_actor_user_id=7,
        effective_user_id=9,
        snowflake_query_id="stale-inherited-native-id",
    ):
        before = get_context()
        results = execute_sql_with_cursor(
            database,
            cursor,
            sql,
            observation_query,
            execute_fn=custom_execute if custom else None,
        )
        assert get_context() == before

    assert [row[0] for row in results] == sql
    assert [row[2] for row in results] == [125.0, 250.0]
    assert [row[3] for row in results] == [1, 1]
    for index, (_, result_set, _, _) in enumerate(results):
        assert result_set is not None
        assert result_set.to_pandas_df()["value"].tolist() == [index + 1]
    assert default_execute.call_count == (0 if custom else 2)
    assert custom_execute.call_count == (2 if custom else 0)
    assert observation_query.progress == 100
    assert mock_db_session.commit.call_count == 2
    assert observation_query.extra["progress"] == "Running statement 2 of 2"

    statements = finished(sql_events, "sql.statement")
    assert [item["statement_index"] for item in statements] == [0, 1]
    assert [item["snowflake_query_id"] for item in statements] == [
        "native-query-1",
        "native-query-2",
    ]
    assert len({item["statement_id"] for item in statements}) == 2
    for phase in ("sql.execute", "sql.fetch"):
        operations = finished(sql_events, phase)
        assert [item["snowflake_query_id"] for item in operations] == [
            "native-query-1",
            "native-query-2",
        ]
        assert [item["statement_id"] for item in operations] == [
            item["statement_id"] for item in statements
        ]
    for item in statements:
        assert item["source"] == "report"
        assert item["request_actor_user_id"] == 7
        assert item["effective_user_id"] == 42
        assert item["sql_lab_query_id"] == 123
        assert item["sql_lab_client_id"] == "sql-client-123"
        assert item["snowflake_session_id"] == "700"
    assert [item["rows"] for item in finished(sql_events, "sql.fetch")] == [1, 1]
    assert "private-sql-payload" not in json.dumps(sql_events)


@pytest.mark.parametrize(
    ("phase", "error_native_id"),
    [("execute", "failed-native-query"), ("execute", None), ("fetch", None)],
)
def test_statement_failure_preserves_exception_progress_and_fresh_ids(
    observation_query: MagicMock,
    mock_db_session: MagicMock,
    sql_events: list[dict[str, Any]],
    phase: str,
    error_native_id: str | None,
) -> None:
    """A failing second statement never inherits the first statement's native ID."""
    cursor = make_cursor()
    error = NativeFailureError(error_native_id)
    original_execute = cursor.execute.side_effect

    def execute(sql: str) -> None:
        if sql == "SELECT 2" and phase == "execute":
            raise error
        original_execute(sql)
        if sql == "SELECT 2" and phase == "fetch":
            cursor.fetchall.side_effect = error

    with telemetry_context(source="warmup", request_actor_user_id=7):
        before = get_context()
        with pytest.raises(NativeFailureError) as exc_info:
            execute_sql_with_cursor(
                observation_query.database,
                cursor,
                ["SELECT 1", "SELECT 2"],
                observation_query,
                execute_fn=lambda cursor, sql: execute(sql),
            )
        assert exc_info.value is error
        assert get_context() == before
    assert observation_query.progress == 50
    assert mock_db_session.commit.call_count == 1
    statements = finished(sql_events, "sql.statement")
    assert len(statements) == 2
    assert statements[0]["snowflake_query_id"] == "native-query-1"
    expected_id = "native-query-2" if phase == "fetch" else error_native_id
    assert statements[1].get("snowflake_query_id") == expected_id
    failed_phase = finished(sql_events, f"sql.{phase}")[-1]
    assert failed_phase["outcome"] == "error"
    assert failed_phase.get("snowflake_query_id") == expected_id
    assert "private-driver-error-message" not in json.dumps(sql_events)


@pytest.mark.parametrize("mutation", [False, True])
def test_legacy_multistatement_ids_results_and_cancellation_handle(
    mocker: MockerFixture,
    observation_query: MagicMock,
    sql_events: list[dict[str, Any]],
    mutation: bool,
) -> None:
    """Preserve results/commits without treating the session as a query ID."""
    cursor = make_cursor()
    connection = MagicMock()
    connection.cursor.return_value = cursor
    database = observation_query.database
    database.get_raw_connection.return_value.__enter__.return_value = connection
    mocker.patch("superset.sql_lab.get_query", return_value=observation_query)
    mocker.patch.object(
        NativeIdEngineSpec, "get_cancel_query_id", return_value="cancel-session-handle"
    )
    sql = "SELECT 1; INSERT INTO t VALUES (2)" if mutation else "SELECT 1; SELECT 2"
    with telemetry_context(source="report", request_actor_user_id=7):
        before = get_context()
        payload = execute_sql_statements(123, sql, True, False, None, False, None)
        assert get_context() == before
    assert payload["status"] == LegacyQueryStatus.SUCCESS
    assert payload["data"] == ([] if mutation else [{"value": 2}])
    assert observation_query.rows == (0 if mutation else 1)
    assert observation_query.progress == 100
    assert observation_query.extra[QUERY_CANCEL_KEY] == "cancel-session-handle"
    assert connection.commit.call_count == int(mutation)
    database.get_raw_connection.assert_called_once()
    statements = finished(sql_events, "sql.statement")
    assert [item["statement_index"] for item in statements] == [0, 1]
    assert [item["snowflake_query_id"] for item in statements] == [
        "native-query-1",
        "native-query-2",
    ]
    assert len(finished(sql_events, "sql.execute")) == 2
    assert len(finished(sql_events, "sql.fetch")) == 2
    assert len(finished(sql_events, "sql.resultset")) == 2
    assert len(finished(sql_events, "sql.dataframe")) == 1
    assert finished(sql_events, "sql.assembly")
    assert "cancel-session-handle" not in json.dumps(sql_events)


@pytest.mark.parametrize("failure", ["driver", "stopped", "timeout", "oauth"])
def test_legacy_error_translation_and_oauth_identity(
    observation_query: MagicMock,
    sql_events: list[dict[str, Any]],
    failure: str,
) -> None:
    """Driver observation precedes, but does not alter, legacy error handling."""
    cursor = make_cursor()
    observation_query.executed_sql = "SELECT 'private-sql-payload'"
    error: Exception
    expected: type[Exception]
    if failure == "oauth":
        error = OAuth2RedirectError(
            "https://private-token/", "tab", "https://redirect/"
        )
        expected = OAuth2RedirectError
    elif failure == "timeout":
        error = SoftTimeLimitExceeded()
        expected = SupersetErrorException
    else:
        error = NativeFailureError()
        expected = (
            SqlLabQueryStoppedException if failure == "stopped" else SqlLabException
        )
    if failure == "stopped":
        observation_query.status = LegacyQueryStatus.STOPPED

    def execute(sql: str) -> None:
        cursor.sfqid = "failed-native-query"
        raise error

    cursor.execute.side_effect = execute
    with telemetry_context(source="report", request_actor_user_id=7):
        before = get_context()
        with pytest.raises(expected) as exc_info:
            execute_query(observation_query, cursor, statement_index=4)
        assert get_context() == before
    if failure == "oauth":
        assert exc_info.value is error
    else:
        assert exc_info.value.__cause__ is error
    if failure == "timeout":
        assert observation_query.status == LegacyQueryStatus.TIMED_OUT
    execute_operation = finished(sql_events, "sql.execute")[0]
    assert execute_operation["statement_index"] == 4
    assert execute_operation["snowflake_query_id"] == "failed-native-query"
    assert execute_operation["outcome"] in {"error", "timeout"}
    assert not finished(sql_events, "sql.fetch")
    encoded = json.dumps(sql_events)
    assert "private-driver-error-message" not in encoded
    assert "private-token" not in encoded
    assert "private-sql-payload" not in encoded


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("cache_write_success", [False, True])
def test_executor_assembly_and_cache_keep_query_identity_and_payload(
    mocker: MockerFixture,
    observation_query: MagicMock,
    sql_events: list[dict[str, Any]],
    enabled: bool,
    cache_write_success: bool,
) -> None:
    """Connection, dataframe and cache phases retain query identity."""
    from superset.extensions import cache_manager

    if not enabled:
        mocker.patch("superset.observability.enabled", return_value=False)
        mocker.patch("superset.observability.events.enabled", return_value=False)
        mocker.patch.dict(current_app.config, {"OBSERVABILITY_ENABLED": False})
        mocker.patch(
            "superset.sql.execution.executor.dataframe_attributes",
            side_effect=AssertionError("disabled dataframe instrumentation"),
        )
    cursor = make_cursor()
    database = observation_query.database
    connection = MagicMock()
    connection.cursor.return_value = cursor
    acquisition_contexts: list[dict[str, Any]] = []

    def acquire() -> MagicMock:
        acquisition_contexts.append(get_context())
        return connection

    database.get_raw_connection.return_value.__enter__.side_effect = acquire
    executor = SQLExecutor(database)
    script = SQLScript("SELECT 1; SELECT 2", engine="snowflake")
    mocker.patch.object(
        executor, "_prepare_sql", return_value=(script, script, None, None)
    )
    mocker.patch.object(
        executor, "_create_query_record", return_value=observation_query
    )
    mocker.patch.object(
        executor, "_generate_cache_key", return_value="original-cache-key"
    )
    cache_get = mocker.patch.object(cache_manager.data_cache, "get", return_value=None)
    cache_set = mocker.patch.object(
        cache_manager.data_cache, "set", return_value=cache_write_success
    )
    with telemetry_context(source="warmup", request_actor_user_id=7):
        before = get_context()
        result = executor.execute(
            "SELECT 1; SELECT 2", QueryOptions(cache=CacheOptions(timeout=60))
        )
        assert get_context() == before
    assert result.status == QueryStatus.SUCCESS
    assert [statement.data["value"].tolist() for statement in result.statements] == [
        [1],
        [2],
    ]
    assert acquisition_contexts[0]["request_actor_user_id"] == 7
    cache_get.assert_called_once_with("original-cache-key")
    assert cache_set.call_args.args[0] == "original-cache-key"
    cached_data = cache_set.call_args.args[1]
    assert set(cached_data) == {"statements", "total_execution_time_ms"}
    assert [statement["data"] for statement in cached_data["statements"]] == [
        [{"value": 1}],
        [{"value": 2}],
    ]
    assert cache_set.call_args.kwargs == {"timeout": 60}
    if not enabled:
        assert acquisition_contexts == [before]
        assert sql_events == []
        return
    assert acquisition_contexts[0]["sql_lab_query_id"] == 123
    assert acquisition_contexts[0]["effective_user_id"] == 42
    assert finished(sql_events, "cache.lookup")[0]["cache_hit"] is False
    assert finished(sql_events, "cache.write")[0]["sql_lab_query_id"] == 123
    assert finished(sql_events, "cache.write")[0]["outcome"] == (
        "success" if cache_write_success else "not_persisted"
    )
    dataframes = finished(sql_events, "sql.dataframe")
    assert [item["statement_index"] for item in dataframes] == [0, 1]
    assert [item["rows"] for item in dataframes] == [1, 1]
    assert all(item["result_bytes"] > 0 for item in dataframes)
    assert all(item["source"] == "warmup" for item in dataframes)


def test_oauth_retry_observes_distinct_attempts_without_native_id_reuse(
    mocker: MockerFixture,
    observation_query: MagicMock,
    sql_events: list[dict[str, Any]],
) -> None:
    """The existing OAuth retry re-acquires once and gets independent statement IDs."""
    database = observation_query.database
    database.is_oauth2_enabled.return_value = True
    database.get_oauth2_config.return_value = SimpleNamespace()
    mocker.patch(
        "superset.utils.oauth2.g", SimpleNamespace(user=SimpleNamespace(id=42))
    )
    mocker.patch(
        "superset.utils.oauth2.refresh_oauth2_token", return_value="private-token"
    )
    mocker.patch.object(NativeIdEngineSpec, "needs_oauth2", return_value=True)
    first_cursor = make_cursor()
    first_cursor.execute.side_effect = NativeFailureError()
    second_cursor = make_cursor()
    connections = [MagicMock(), MagicMock()]
    connections[0].cursor.return_value = first_cursor
    connections[1].cursor.return_value = second_cursor
    database.get_raw_connection.return_value.__enter__.side_effect = connections
    script = SQLScript("SELECT 1", engine="snowflake")
    with telemetry_context(source="report", request_actor_user_id=7):
        before = get_context()
        results = SQLExecutor(database)._execute_statements(
            script, script, None, None, observation_query
        )
        assert get_context() == before
    assert len(results) == 1
    assert results[0].data["value"].tolist() == [1]
    assert database.get_raw_connection.call_count == 2
    database.start_oauth2_dance.assert_not_called()
    statements = finished(sql_events, "sql.statement")
    assert [item["statement_index"] for item in statements] == [0, 0]
    assert statements[0].get("snowflake_query_id") is None
    assert statements[1]["snowflake_query_id"] == "native-query-1"
    assert statements[0]["statement_id"] != statements[1]["statement_id"]
    assert "private-token" not in json.dumps(sql_events)


@pytest.mark.parametrize("source", [None, "chart", "report", "warmup"])
def test_query_context_defaults_source_without_inventing_actor(
    observation_query: MagicMock,
    sql_events: list[dict[str, Any]],
    source: str | None,
) -> None:
    """Safe query metadata merges locally and restores on exceptional exits."""
    observation_query.client_id = "unsafe client token=private"
    with telemetry_context(source=source, request_actor_user_id=None):
        before = get_context()
        with pytest.raises(RuntimeError, match="scope-exit"):  # noqa: PT012
            with sql_lab_query_context(observation_query):
                context = get_context()
                assert context["source"] == (source or "sql_lab")
                assert context.get("request_actor_user_id") is None
                assert context["effective_user_id"] == 42
                assert context["sql_lab_query_id"] == 123
                assert "sql_lab_client_id" not in context
                raise RuntimeError("scope-exit")
        assert get_context() == before


@pytest.mark.parametrize("custom", [False, True])
def test_disabled_statement_path_avoids_metadata_and_preserves_errors(
    mocker: MockerFixture,
    observation_query: MagicMock,
    sql_events: list[dict[str, Any]],
    custom: bool,
) -> None:
    """Disabled observation adds no identity reads, native reads or result traversal."""
    from superset.sql_lab import _serialize_and_expand_data

    mocker.patch("superset.observability.enabled", return_value=False)
    mocker.patch("superset.observability.events.enabled", return_value=False)
    mocker.patch.dict(current_app.config, {"OBSERVABILITY_ENABLED": False})
    mocker.patch(
        "superset.observability.sql.sql_fingerprint",
        side_effect=AssertionError("disabled SQL fingerprint"),
    )
    mocker.patch(
        "superset.sql_lab.dataframe_attributes",
        side_effect=AssertionError("disabled dataframe traversal"),
    )
    cursor = make_cursor()
    cursor.execute.side_effect = None
    cursor.fetchall.return_value = [(1,)]
    for attribute in ("sfqid", "connection"):
        mocker.patch.object(
            type(cursor),
            attribute,
            create=True,
            new_callable=PropertyMock,
            side_effect=AssertionError("disabled native attribute read"),
        )
    query = SimpleNamespace(schema="public", progress=0, set_extra_json_key=MagicMock())
    database = observation_query.database
    execute_fn = (lambda cursor, sql: cursor.execute(sql)) if custom else None
    with telemetry_context(source="report", effective_user_id=7):
        before = get_context()
        with sql_lab_query_context(object()):
            assert get_context() == before
        results = execute_sql_with_cursor(
            database, cursor, ["SELECT 1"], query, execute_fn=execute_fn
        )
        assert get_context() == before
        result_set = results[0][1]
        assert result_set is not None
        data, _, _, _ = _serialize_and_expand_data(result_set, NativeIdEngineSpec)
        assert data == [{"value": 1}]
        error = NativeFailureError()
        cursor.execute.side_effect = error
        with pytest.raises(NativeFailureError) as exc_info:
            execute_sql_with_cursor(
                database, cursor, ["SELECT 2"], query, execute_fn=execute_fn
            )
        assert exc_info.value is error
        assert get_context() == before
    assert cursor.execute.call_count == 2
    assert sql_events == []


def test_expired_query_identity_never_loads_orm_descriptors(
    sql_events: list[dict[str, Any]],
) -> None:
    """An expired detached row supplies its PK without requiring a metastore read."""
    from sqlalchemy.orm import make_transient_to_detached
    from sqlalchemy.orm.exc import DetachedInstanceError

    from superset.models.sql_lab import Query

    query = Query(id=321)
    make_transient_to_detached(query)
    with pytest.raises(DetachedInstanceError):
        _ = query.client_id
    with telemetry_context(
        source="report", database_id=1, effective_user_id=42, request_actor_user_id=7
    ):
        before = get_context()
        with sql_lab_query_context(query):
            context = get_context()
            assert context["sql_lab_query_id"] == 321
            assert context["database_id"] == 1
            assert context["effective_user_id"] == 42
            assert context["request_actor_user_id"] == 7
            assert context.get("sql_lab_client_id") is None
        assert get_context() == before


def test_created_query_identity_survives_commit_expiration(
    mocker: MockerFixture,
    observation_query: MagicMock,
    mock_db_session: MagicMock,
    sql_events: list[dict[str, Any]],
) -> None:
    """A primitive creation snapshot preserves non-PK IDs without an ORM reload."""
    from sqlalchemy.orm import make_transient_to_detached
    from sqlalchemy.orm.exc import DetachedInstanceError

    mocker.patch(
        "superset.sql.execution.executor.g",
        SimpleNamespace(user=SimpleNamespace(get_id=lambda: 42)),
    )

    def commit() -> None:
        query = mock_db_session.add.call_args.args[0]
        query.id = 321
        for field in ("database_id", "client_id", "user_id"):
            query.__dict__.pop(field)
        make_transient_to_detached(query)

    mock_db_session.commit.side_effect = commit
    query = SQLExecutor(observation_query.database)._create_query_record(
        "SELECT 1", QueryOptions(), None, None, QueryStatus.RUNNING
    )
    with pytest.raises(DetachedInstanceError):
        _ = query.client_id
    with telemetry_context(source="report", request_actor_user_id=7):
        before = get_context()
        with sql_lab_query_context(query):
            context = get_context()
            assert context["sql_lab_query_id"] == 321
            assert context["database_id"] == 1
            assert context["effective_user_id"] == 42
            assert context["request_actor_user_id"] == 7
            client_id = context["sql_lab_client_id"]
            assert isinstance(client_id, str)
            assert len(client_id) == 11
        assert get_context() == before
    assert mock_db_session.commit.call_count == 1


def test_async_submission_keeps_originating_actor_and_query_identity(
    mocker: MockerFixture,
    observation_query: MagicMock,
    sql_events: list[dict[str, Any]],
) -> None:
    """Producer headers carry query IDs without treating the owner as actor."""
    from superset.observability import wire_context

    executor = SQLExecutor(observation_query.database)
    script = SQLScript("SELECT 1", engine="snowflake")
    mocker.patch.object(
        executor, "_prepare_sql", return_value=(script, script, None, None)
    )
    mocker.patch.object(executor, "_try_get_cached_result", return_value=None)
    mocker.patch.object(
        executor, "_create_query_record", return_value=observation_query
    )
    submitted: list[dict[str, Any]] = []

    def submit(query: Any, sql: str) -> None:
        submitted.append(wire_context())

    mocker.patch.object(executor, "_submit_query_to_celery", side_effect=submit)
    with telemetry_context(source="export", request_actor_user_id=7):
        before = get_context()
        handle = executor.execute_async("SELECT 1")
        assert get_context() == before
    assert handle.query_id == 123
    assert submitted[0]["source"] == "export"
    assert submitted[0]["request_actor_user_id"] == 7
    assert submitted[0]["effective_user_id"] == 42
    assert submitted[0]["sql_lab_query_id"] == 123
    assert submitted[0]["sql_lab_client_id"] == "sql-client-123"


def test_cache_hit_reports_lookup_without_physical_sql(
    mocker: MockerFixture,
    observation_query: MagicMock,
    sql_events: list[dict[str, Any]],
) -> None:
    """Cache hits preserve payloads without emitting physical SQL events."""
    from superset.extensions import cache_manager

    executor = SQLExecutor(observation_query.database)
    mocker.patch.object(
        executor, "_generate_cache_key", return_value="original-cache-key"
    )
    cached = {
        "statements": [
            {
                "original_sql": "SELECT 1",
                "executed_sql": "SELECT 1",
                "data": [{"value": 1}],
                "row_count": 1,
                "execution_time_ms": 12.0,
            }
        ],
        "total_execution_time_ms": 15.0,
    }
    cache_get = mocker.patch.object(
        cache_manager.data_cache, "get", return_value=cached
    )
    with telemetry_context(source="report", request_actor_user_id=7):
        before = get_context()
        result = executor._try_get_cached_result(False, "SELECT 1", QueryOptions())
        assert get_context() == before
    assert result is not None
    assert result.is_cached is True
    assert result.query_id is None
    assert result.total_execution_time_ms == 15.0
    assert result.statements[0].data == [{"value": 1}]
    cache_get.assert_called_once_with("original-cache-key")
    assert finished(sql_events, "cache.lookup")[0]["cache_hit"] is True
    assert not finished(sql_events, "sql.statement")
    assert not finished(sql_events, "sql.execute")
    observation_query.database.get_raw_connection.assert_not_called()


def test_legacy_failed_cache_write_is_not_observed_as_persisted(
    mocker: MockerFixture,
    observation_query: MagicMock,
    sql_events: list[dict[str, Any]],
) -> None:
    """Failed cache writes preserve inline results and report non-persistence."""
    cursor = make_cursor()
    connection = MagicMock()
    connection.cursor.return_value = cursor
    connection_manager = observation_query.database.get_raw_connection.return_value
    connection_manager.__enter__.return_value = connection
    backend = MagicMock()
    backend.set.return_value = False
    mocker.patch("superset.sql_lab.results_backend", backend)
    mocker.patch("superset.sql_lab.results_backend_use_msgpack", False)
    mocker.patch("superset.sql_lab.get_query", return_value=observation_query)
    with telemetry_context(source="sql_lab", request_actor_user_id=7):
        before = get_context()
        payload = execute_sql_statements(123, "SELECT 1", True, True, None, False, None)
        assert get_context() == before
    assert payload["status"] == LegacyQueryStatus.SUCCESS
    assert payload["data"] == [{"value": 1}]
    assert observation_query.results_key is None
    assert observation_query.status == LegacyQueryStatus.SUCCESS
    backend.set.assert_called_once()
    write = finished(sql_events, "cache.write")[0]
    assert write["cache_backend"] == "results"
    assert write["outcome"] == "not_persisted"
    assert write["result_bytes"] > 0
