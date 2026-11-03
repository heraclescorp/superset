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
"""Isolated driver-boundary tests: pytest --noconftest <this file>.

Load the standalone module directly so these tests need neither create_app,
Superset's application fixtures, a metastore, nor real Snowflake credentials.
"""

from __future__ import annotations

import importlib.util
import math
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from superset.utils import json

BASE = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
COLUMNS = (
    "query_id",
    "session_id",
    "warehouse_name",
    "execution_status",
    "query_type",
    "start_time",
    "end_time",
    "total_elapsed_time",
    "compilation_time",
    "execution_time",
    "queued_overload_time",
    "queued_provisioning_time",
    "queued_repair_time",
    "transaction_blocked_time",
    "safe_tag",
)


@pytest.fixture
def warehouse(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Import only the standalone module, not the Superset app package."""
    path = Path(__file__).resolve().parents[3] / "superset/observability/warehouse.py"
    spec = importlib.util.spec_from_file_location("warehouse_under_test", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def telemetry(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """Record the real event/metric boundary without a collector or app."""
    calls = SimpleNamespace(events=[], gauges=[], roles=[], enabled=True)
    package = ModuleType("superset")
    package.__path__ = []
    foundation = ModuleType("superset.observability")
    foundation.emit_event = lambda name, **attrs: calls.events.append((name, attrs))
    foundation.gauge = lambda name, value, **attrs: calls.gauges.append(
        (name, value, attrs)
    )
    foundation.init_process = lambda role=None: calls.roles.append(role)
    foundation.is_enabled = lambda: calls.enabled
    monkeypatch.setitem(sys.modules, "superset", package)
    monkeypatch.setitem(sys.modules, "superset.observability", foundation)
    return calls


def config(warehouse: Any, **changes: Any) -> Any:
    """Use deliberately recognizable secrets so leaks are detectable."""
    return warehouse.WarehouseConfig(
        account="private-account",
        user="private-user",
        password="private-password",  # noqa: S106
        role="private-role",
        database="private-database",
        warehouse="private-warehouse",
        **{"lookback_seconds": 100, "window_seconds": 100, "lag_seconds": 0, **changes},
    )


def row(
    query_id: str = "native-1",
    seconds: float = 40,
    tag: dict[str, Any] | None = None,
    **changes: Any,
) -> dict[str, Any]:
    """Model only the projected native metadata returned by the connector."""
    return {
        "query_id": query_id,
        "session_id": 12345,
        "warehouse_name": "DASH_WH",
        "execution_status": "SUCCESS",
        "query_type": "SELECT",
        "start_time": BASE + timedelta(seconds=1),
        "end_time": BASE + timedelta(seconds=seconds),
        "total_elapsed_time": 2450,
        "compilation_time": 150,
        "execution_time": 1000,
        "queued_overload_time": 100,
        "queued_provisioning_time": 1000,
        "queued_repair_time": 200,
        "transaction_blocked_time": 0,
        "safe_tag": json.dumps(tag or {"superset_observability": 1, "source": "chart"}),
        **changes,
    }


class FakeConnection:
    """A capped, completion-time-filtered driver, not a fake enrichment path."""

    def __init__(self, rows: list[dict[str, Any]], cap: int = 10_000) -> None:
        self.rows = rows
        self.cap = cap
        self.calls: list[tuple[str, tuple[str, str]]] = []
        self.closed = False

    def cursor(self) -> Any:
        connection = self

        class Cursor:
            description = [(column.upper(),) for column in COLUMNS]

            def __enter__(self) -> Any:
                return self

            def __exit__(self, *args: Any) -> None:
                return None

            def execute(self, sql: str, bounds: tuple[str, str]) -> None:
                connection.calls.append((sql, bounds))
                lower, upper = map(datetime.fromisoformat, bounds)
                matches = [
                    item
                    for item in connection.rows
                    if lower <= item["end_time"] < upper
                ]
                matches.sort(key=lambda item: item["end_time"], reverse=True)
                self.result = [
                    tuple(item.get(column) for column in COLUMNS)
                    for item in matches[: connection.cap]
                ]

            def fetchall(self) -> list[tuple[Any, ...]]:
                return self.result

        return Cursor()

    def close(self) -> None:
        self.closed = True


def test_cap_is_partitioned_without_truncating_10001_queries(warehouse: Any) -> None:
    rows = [row(f"query-{index}", (index + 1) / 101) for index in range(10_001)]
    connection = FakeConnection(rows)
    result = warehouse.collect_history(
        connection, config(warehouse), BASE + timedelta(seconds=100)
    )
    assert len(result.records) == 10_001
    assert set(result.records) == {item["query_id"] for item in rows}
    assert result.history_query_count == 3
    assert len(connection.calls) == 3


def test_cap_is_checked_before_monitoring_queries_are_excluded(warehouse: Any) -> None:
    monitoring = {"superset_observability": 1, "source": "observability"}
    rows = [row(f"monitor-{index}", index / 200, monitoring) for index in range(10_000)]
    rows.append(row("actual-query", 60))
    connection = FakeConnection(rows)
    result = warehouse.collect_history(
        connection, config(warehouse), BASE + timedelta(seconds=100)
    )
    assert set(result.records) == {"actual-query"}
    assert len(connection.calls) == 5


def test_unsplittable_native_timestamp_cap_fails_explicitly(warehouse: Any) -> None:
    connection = FakeConnection([row("same-time", 0)] * 10_000)
    with pytest.raises(
        warehouse.HistoryCapacityError, match="partition remains capped"
    ):
        warehouse.collect_history(
            connection,
            config(warehouse, lookback_seconds=1),
            BASE + timedelta(seconds=1),
        )


def test_request_budget_never_publishes_a_partial_window(
    warehouse: Any,
    telemetry: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = FakeConnection([row("capped", 40)] * 10_000)
    settings = config(warehouse, max_history_requests=1)
    monkeypatch.setattr(warehouse.WarehouseConfig, "from_env", lambda: settings)
    monkeypatch.setattr(warehouse, "_connect", lambda _: connection)
    collect = warehouse.collect_history
    monkeypatch.setattr(
        warehouse,
        "collect_history",
        lambda conn, cfg: collect(conn, cfg, BASE + timedelta(seconds=100)),
    )
    assert warehouse.main() == 1
    assert connection.closed
    assert not telemetry.gauges
    assert [name for name, _ in telemetry.events] == ["warehouse.enrichment.failed"]
    assert telemetry.events[0][1]["error_category"] == "history_capacity"


def test_overlaps_and_boundary_queries_have_stable_event_ids(
    warehouse: Any,
    telemetry: SimpleNamespace,
) -> None:
    connection = FakeConnection(
        [row("at-start", 0), row("shared", 50), row("at-end", 100)]
    )
    first = warehouse.collect_history(
        connection, config(warehouse, window_seconds=50), BASE + timedelta(seconds=100)
    )
    second = warehouse.collect_history(
        connection, config(warehouse), BASE + timedelta(seconds=150)
    )
    assert set(first.records) == {"at-start", "shared"}
    assert set(second.records) == {"shared", "at-end"}
    assert first.records["shared"]["event_id"] == second.records["shared"]["event_id"]
    assert first.records["shared"]["event_id"] != first.records["at-start"]["event_id"]
    warehouse.publish_window(first)
    warehouse.publish_window(second)
    shared = [
        attrs
        for name, attrs in telemetry.events
        if name == "warehouse.query.enriched"
        and attrs["snowflake_query_id"] == "shared"
    ]
    assert len(shared) == 2
    assert shared[0] == shared[1]


def test_tag_parser_only_accepts_versioned_allowlisted_primitives(
    warehouse: Any,
) -> None:
    value = {
        "superset_observability": 1,
        "source": "sql_lab",
        "statement_id": "attempt-1",
        "execution_id": "execution-1",
        "chart_id": "23",
        "effective_user_id": 7,
        "request_actor_user_id": True,
        "database_id": -1,
        "query_index": 0,
        "force_refresh": False,
        "async_execution": "true",
        "task_id": {"secret": "x"},
        "request_id": "unbounded-" + "x" * 128,
        "sql_fingerprint": "a" * 64,
        "query_text": "SELECT private_value",
        "password": "private-password",
        "filters": ["private filter"],
    }
    assert warehouse.parse_tag(json.dumps(value)) == {
        "source": "sql_lab",
        "statement_id": "attempt-1",
        "execution_id": "execution-1",
        "chart_id": 23,
        "effective_user_id": 7,
        "query_index": 0,
        "force_refresh": False,
        "sql_fingerprint": "a" * 64,
    }


@pytest.mark.parametrize(
    "tag",
    [
        None,
        "not-json",
        "[]",
        "{}",
        "x" * 8193,
        '{"superset_observability":true,"source":"chart"}',
        '{"superset_observability":2,"source":"chart"}',
    ],
)
def test_invalid_tags_never_become_context(warehouse: Any, tag: object) -> None:
    assert warehouse.parse_tag(tag) == {}


def test_driver_projection_and_events_do_not_leak_query_or_result_data(
    warehouse: Any,
    telemetry: SimpleNamespace,
) -> None:
    private = "raw-sql-result-token-secret"
    tag = {
        "superset_observability": 1,
        "source": "chart",
        "chart_id": 5,
        "query_text": private,
        "query_result": private,
        "request_id": private + " unsafe",
    }
    connection = FakeConnection(
        [row(tag=tag, query_text=private, result_rows=[private])]
    )
    result = warehouse.collect_history(
        connection, config(warehouse), BASE + timedelta(seconds=100)
    )
    warehouse.publish_window(result)
    payload = json.dumps({"events": telemetry.events, "gauges": telemetry.gauges})
    assert private not in payload
    query = connection.calls[0][0].lower()
    assert "query_text" not in query
    assert "result_rows" not in query
    assert "information_schema.query_history_by_user" in query
    assert "account_usage" not in query
    assert "user_name" not in query
    assert " as safe_tag" in query
    assert "select *" not in query
    assert "superset_observability" in query


def test_native_timings_are_distinct_seconds_and_not_app_latency(
    warehouse: Any,
) -> None:
    record = warehouse._record(row())
    assert record is not None
    assert {
        key: value for key, value in record.items() if key.startswith("warehouse_")
    } == {
        "warehouse_name": "DASH_WH",
        "warehouse_total_elapsed_seconds": 2.45,
        "warehouse_compilation_seconds": 0.15,
        "warehouse_execution_seconds": 1.0,
        "warehouse_queued_overload_seconds": 0.1,
        "warehouse_queued_provisioning_seconds": 1.0,
        "warehouse_queued_repair_seconds": 0.2,
        "warehouse_transaction_blocked_seconds": 0.0,
    }
    assert "duration_seconds" not in record
    assert record["native_status"] == "success"
    assert record["snowflake_session_id"] == "12345"


@pytest.mark.parametrize(
    "milliseconds,expected",
    [
        (None, None),
        (True, None),
        ("100", None),
        (-1, None),
        (float("nan"), None),
        (float("inf"), None),
        (Decimal("NaN"), None),
        (0, 0),
        (Decimal("1250.5"), 1.2505),
    ],
)
def test_unknown_native_timings_are_not_zero(
    warehouse: Any,
    milliseconds: object,
    expected: float | None,
) -> None:
    assert warehouse._seconds(milliseconds) == expected


def test_empty_and_unknown_windows_emit_no_fake_duration_sums(
    warehouse: Any,
    telemetry: SimpleNamespace,
) -> None:
    empty = warehouse.collect_history(
        FakeConnection([]), config(warehouse), BASE + timedelta(seconds=100)
    )
    warehouse.publish_window(empty)
    assert not [
        name for name, _ in telemetry.events if name == "warehouse.query.enriched"
    ]
    assert [
        (name, value)
        for name, value, _ in telemetry.gauges
        if name == "warehouse.own_queries.window_count"
    ] == [("warehouse.own_queries.window_count", 0)]
    assert all(value == 0 for _, value, _ in telemetry.gauges)
    assert not [
        name
        for name, _, _ in telemetry.gauges
        if name == "warehouse.own_queries.window_duration_seconds"
    ]
    summary = telemetry.events[-1][1]
    assert summary["window_seconds"] == 100
    assert summary["window_start"] == BASE.isoformat()
    assert summary["window_end"] == (BASE + timedelta(seconds=100)).isoformat()
    assert summary["query_count"] == 0
    unknown = row(**dict.fromkeys(warehouse.TIMINGS.values()))
    record = warehouse._record(unknown)
    assert record is not None
    assert not any(key.endswith("_seconds") for key in record)


def test_window_gauges_have_bounded_dimensions_and_known_observation_counts(
    warehouse: Any,
    telemetry: SimpleNamespace,
) -> None:
    connection = FakeConnection([row("one"), row("two", execution_time=None)])
    window = warehouse.collect_history(
        connection, config(warehouse), BASE + timedelta(seconds=100)
    )
    warehouse.publish_window(window)
    execution = {
        (name, attrs.get("phase")): value for name, value, attrs in telemetry.gauges
    }
    assert execution[("warehouse.own_queries.window_count", None)] == 2
    assert (
        execution[("warehouse.own_queries.window_duration_seconds", "execution")] == 1
    )
    assert (
        execution[("warehouse.own_queries.window_duration_known_count", "execution")]
        == 1
    )
    assert (
        execution[("warehouse.own_queries.window_duration_seconds", "compilation")]
        == 0.3
    )
    for _, value, attributes in telemetry.gauges:
        assert math.isfinite(value)
        assert set(attributes) <= {
            "source",
            "db_system",
            "metric_scope",
            "window_seconds",
            "phase",
            "warehouse_name",
            "database_id",
        }
        assert attributes["metric_scope"] == "own_user"
        assert attributes["window_seconds"] == 100
        assert attributes["warehouse_name"] == "DASH_WH"


def test_missing_query_id_never_uses_session_id(warehouse: Any) -> None:
    with pytest.raises(warehouse.HistoryDataError, match="identifier is missing"):
        warehouse._record(row(query_id=None, session_id=12345))


def test_missing_timestamps_fail_instead_of_guessing(warehouse: Any) -> None:
    with pytest.raises(warehouse.HistoryDataError):
        warehouse._record(row(start_time=datetime(2026, 1, 1)))


def test_env_bounds_and_credential_repr(warehouse: Any) -> None:
    environment = {
        f"SNOWFLAKE_{key.upper()}": f"private-{key}"
        for key in ("account", "user", "password", "role", "database", "warehouse")
    }
    settings = warehouse.WarehouseConfig.from_env(environment)
    assert settings.lookback_seconds == 900
    assert settings.window_seconds == 300
    assert settings.lag_seconds == 60
    assert settings.max_history_requests == 128
    assert "private-" not in repr(settings)
    for key in environment:
        with pytest.raises(warehouse.ConfigurationError, match=key):
            warehouse.WarehouseConfig.from_env(
                {name: value for name, value in environment.items() if name != key}
            )
    for suffix, bad in (
        ("LOOKBACK_SECONDS", "518401"),
        ("WINDOW_SECONDS", "0"),
        ("LAG_SECONDS", "-1"),
        ("MAX_HISTORY_REQUESTS", "1025"),
        ("WINDOW_SECONDS", "secret-invalid-value"),
    ):
        with pytest.raises(warehouse.ConfigurationError) as error:
            warehouse.WarehouseConfig.from_env(
                {**environment, f"SUPERSET_WAREHOUSE_{suffix}": bad}
            )
        assert bad not in str(error.value)


def test_connection_is_monitoring_tagged_and_uses_bounded_timeouts(
    warehouse: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []
    snowflake = ModuleType("snowflake")
    snowflake.__path__ = []
    connector = ModuleType("snowflake.connector")
    connector.connect = lambda **kwargs: calls.append(kwargs) or "connection"
    snowflake.connector = connector
    monkeypatch.setitem(sys.modules, "snowflake", snowflake)
    monkeypatch.setitem(sys.modules, "snowflake.connector", connector)
    assert warehouse._connect(config(warehouse)) == "connection"
    options = calls[0]
    assert options["password"] == "private-password"  # noqa: S105
    assert (
        options["login_timeout"]
        == options["network_timeout"]
        == options["socket_timeout"]
        == 15
    )
    assert options["session_parameters"]["STATEMENT_TIMEOUT_IN_SECONDS"] == 30
    assert json.loads(options["session_parameters"]["QUERY_TAG"]) == {
        "superset_observability": 1,
        "source": "observability",
    }


@pytest.mark.parametrize(
    "sqlstate,errno,category",
    [
        ("28000", 0, "authentication"),
        (None, 390100, "authentication"),
        ("42501", 0, "permissions"),
        (None, 2003, "permissions"),
        (None, 0, "snowflake_connection_or_query"),
    ],
)
def test_credential_grant_errors_are_explicit_and_never_leak_or_fallback(
    warehouse: Any,
    telemetry: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sqlstate: str | None,
    errno: int,
    category: str,
) -> None:
    class DriverError(Exception):
        pass

    error = DriverError("private-password raw SQL private-account")
    error.sqlstate = sqlstate
    error.errno = errno

    def fail(_: Any) -> None:
        raise error

    monkeypatch.setattr(
        warehouse.WarehouseConfig, "from_env", lambda: config(warehouse)
    )
    monkeypatch.setattr(warehouse, "_connect", fail)
    assert warehouse.main() == 1
    assert telemetry.roles == ["warehouse-enricher"]
    assert not telemetry.gauges
    assert telemetry.events[0][0] == "warehouse.enrichment.failed"
    assert telemetry.events[0][1]["error_category"] == category
    output = capsys.readouterr()
    assert category in output.err
    assert "private-" not in output.err + output.out + json.dumps(telemetry.events)


def test_successful_main_is_one_finite_run_and_closes_connection(
    warehouse: Any,
    telemetry: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = FakeConnection([])
    monkeypatch.setattr(
        warehouse.WarehouseConfig, "from_env", lambda: config(warehouse)
    )
    monkeypatch.setattr(warehouse, "_connect", lambda _: connection)
    assert warehouse.main() == 0
    assert connection.closed
    assert len(connection.calls) == 1
    assert telemetry.roles == ["warehouse-enricher"]
    assert telemetry.events[-1][0] == "warehouse.enrichment.window"


def test_read_grant_failure_closes_connection_without_partial_publication(
    warehouse: Any,
    telemetry: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class DriverPermissionError(Exception):
        sqlstate = "42501"

    class DeniedConnection(FakeConnection):
        def cursor(self) -> Any:
            raise DriverPermissionError("private-password SQL table private-user")

    connection = DeniedConnection([])
    monkeypatch.setattr(
        warehouse.WarehouseConfig, "from_env", lambda: config(warehouse)
    )
    monkeypatch.setattr(warehouse, "_connect", lambda _: connection)
    assert warehouse.main() == 1
    assert connection.closed
    assert not telemetry.gauges
    assert telemetry.events[0][1]["error_category"] == "permissions"
    assert "private-" not in capsys.readouterr().err + json.dumps(telemetry.events)


def test_lag_and_chunk_boundaries_cover_only_requested_completion_window(
    warehouse: Any,
) -> None:
    settings = config(warehouse, window_seconds=30, lag_seconds=20)
    connection = FakeConnection(
        [
            row("before", -1),
            row("start", 0),
            row("chunk", 30),
            row("last", 99),
            row("lagged", 100),
        ]
    )
    window = warehouse.collect_history(
        connection, settings, BASE + timedelta(seconds=120)
    )
    assert window.start == BASE
    assert window.end == BASE + timedelta(seconds=100)
    assert set(window.records) == {"start", "chunk", "last"}
    bounds = [tuple(map(datetime.fromisoformat, args)) for _, args in connection.calls]
    assert len(bounds) == 4
    assert bounds[0][0] == window.start
    assert bounds[-1][1] == window.end
    assert all(
        previous[1] == following[0]
        for previous, following in zip(bounds, bounds[1:], strict=False)
    )


def test_unversioned_monitoring_tag_cannot_hide_an_unrelated_native_query(
    warehouse: Any,
) -> None:
    record = warehouse._record(row(tag={"source": "observability", "chart_id": 19}))
    assert record is not None
    assert record["source"] == "system"
    assert "chart_id" not in record


def test_disabled_cli_never_validates_credentials_or_opens_a_connection(
    warehouse: Any,
    telemetry: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    telemetry.enabled = False

    def unexpected(*args: Any, **kwargs: Any) -> None:
        raise AssertionError(
            "Disabled enrichment must not touch Snowflake or credentials"
        )

    monkeypatch.setattr(warehouse.WarehouseConfig, "from_env", unexpected)
    monkeypatch.setattr(warehouse, "_connect", unexpected)
    assert warehouse.main() == 0
    assert telemetry.roles == ["warehouse-enricher"]
    assert not telemetry.events
    assert not telemetry.gauges


def test_window_gauges_do_not_collapse_warehouses_or_tagged_databases(
    warehouse: Any,
    telemetry: SimpleNamespace,
) -> None:
    def tag(database_id: int) -> dict[str, Any]:
        return {
            "superset_observability": 1,
            "source": "chart",
            "database_id": database_id,
        }

    connection = FakeConnection(
        [
            row("a-one", tag=tag(1), warehouse_name="WAREHOUSE_A", execution_time=1000),
            row("a-two", tag=tag(1), warehouse_name="WAREHOUSE_A", execution_time=2000),
            row("b-one", tag=tag(1), warehouse_name="WAREHOUSE_B", execution_time=4000),
            row(
                "a-other-database",
                tag=tag(2),
                warehouse_name="WAREHOUSE_A",
                execution_time=8000,
            ),
            row("unknown", warehouse_name=None, execution_time=None),
        ]
    )
    window = warehouse.collect_history(
        connection, config(warehouse), BASE + timedelta(seconds=100)
    )
    warehouse.publish_window(window)
    counts = {
        (attrs.get("warehouse_name"), attrs.get("database_id")): value
        for name, value, attrs in telemetry.gauges
        if name == "warehouse.own_queries.window_count"
    }
    assert counts == {
        ("WAREHOUSE_A", 1): 2,
        ("WAREHOUSE_B", 1): 1,
        ("WAREHOUSE_A", 2): 1,
        (None, None): 1,
    }
    durations = {
        (attrs.get("warehouse_name"), attrs.get("database_id")): value
        for name, value, attrs in telemetry.gauges
        if name == "warehouse.own_queries.window_duration_seconds"
        and attrs["phase"] == "execution"
    }
    assert durations == {
        ("WAREHOUSE_A", 1): 3,
        ("WAREHOUSE_B", 1): 4,
        ("WAREHOUSE_A", 2): 8,
    }
    known = {
        (attrs.get("warehouse_name"), attrs.get("database_id")): value
        for name, value, attrs in telemetry.gauges
        if name == "warehouse.own_queries.window_duration_known_count"
        and attrs["phase"] == "execution"
    }
    assert known == {
        ("WAREHOUSE_A", 1): 2,
        ("WAREHOUSE_B", 1): 1,
        ("WAREHOUSE_A", 2): 1,
        (None, None): 0,
    }
    assert all(
        "warehouse_name" not in attrs and "database_id" not in attrs
        for _, _, attrs in telemetry.gauges
        if attrs.get("warehouse_name") is None
    )
    assert telemetry.events[-1][1]["query_count"] == 5


@pytest.mark.parametrize(
    "name",
    [
        "",
        "private\nwarehouse",
        "private\x00warehouse",
        "x" * 129,
        "warehouse;query",
        'warehouse"quote',
        "https://private",
    ],
)
def test_invalid_native_warehouse_names_are_not_metric_dimensions(
    warehouse: Any, name: str
) -> None:
    record = warehouse._record(row(warehouse_name=name))
    assert record is not None
    assert "warehouse_name" not in record


def test_native_warehouse_names_preserve_safe_spaces_case_and_punctuation(
    warehouse: Any,
    telemetry: SimpleNamespace,
) -> None:
    name = "Analytics Warehouse (XL)+1-test.prod"
    window = warehouse.collect_history(
        FakeConnection([row(warehouse_name=name)]),
        config(warehouse),
        BASE + timedelta(seconds=100),
    )
    warehouse.publish_window(window)
    assert window.records["native-1"]["warehouse_name"] == name
    assert all(attrs["warehouse_name"] == name for _, _, attrs in telemetry.gauges)


@pytest.mark.parametrize(
    "native_kind,normalized",
    [
        ("SELECT", "select"),
        ("CREATE_TABLE_AS_SELECT", "create_table_as_select"),
        ("ALTER_SESSION", "alter_session"),
        ("SHOW", "show"),
        ("CALL", "call"),
        (None, None),
        ("SELECT private_column FROM private_table", None),
        ("query-kind-with-payload", None),
        ("x" * 65, None),
    ],
)
def test_native_query_type_is_projected_and_emitted_without_sql_inference(
    warehouse: Any,
    telemetry: SimpleNamespace,
    native_kind: str | None,
    normalized: str | None,
) -> None:
    connection = FakeConnection([row(query_type=native_kind, query_text="private SQL")])
    window = warehouse.collect_history(
        connection, config(warehouse), BASE + timedelta(seconds=100)
    )
    warehouse.publish_window(window)
    event = next(
        attrs for name, attrs in telemetry.events if name == "warehouse.query.enriched"
    )
    assert event.get("query_type") == normalized
    if normalized is None:
        assert "query_type" not in event
    assert "query_type" in connection.calls[0][0]
    assert "private SQL" not in json.dumps(telemetry.events)
