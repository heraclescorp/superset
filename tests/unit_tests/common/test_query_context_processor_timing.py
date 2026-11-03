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
import re
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any, cast, Iterator
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from flask import Flask
from flask_caching import Cache

from superset.common.chart_data import ChartDataResultFormat, ChartDataResultType
from superset.common.chart_data_timing import QueryDataResult, QueryTiming
from superset.common.db_query_status import QueryStatus
from superset.common.query_context import QueryContext
from superset.common.query_context_processor import (
    chart_data_context,
    QueryContextProcessor,
)
from superset.common.query_object import QueryObject
from superset.common.query_serialization import serialize_query
from superset.constants import CacheRegion
from superset.models.core import Database
from superset.models.helpers import QueryResult
from superset.observability import get_context, telemetry_context
from superset.utils import json


def _query_timing() -> QueryTiming:
    return QueryTiming(
        query_planning_ns=1_000_000,
        cache_resolution_ns=2_000_000,
        data_acquisition_ns=3_000_000,
        payload_assembly_ns=4_000_000,
        total_ns=12_000_000,
    )


def _query_obj() -> MagicMock:
    query_obj = MagicMock()
    query_obj.columns = ["col1"]
    query_obj.column_names = ["col1"]
    query_obj.metrics = []
    query_obj.metric_names = []
    query_obj.from_dttm = None
    query_obj.to_dttm = None
    query_obj.annotation_layers = []
    query_obj.filter = []
    return query_obj


def _processor() -> QueryContextProcessor:
    query_context = MagicMock()
    query_context.force = False
    query_context.form_data = {}
    query_context.cache_values = {"queries": [{}]}
    query_context.queries = [_query_obj()]
    query_context.prepare_contribution_totals.return_value = ([], None)

    processor = QueryContextProcessor.__new__(QueryContextProcessor)
    processor._query_context = query_context
    processor._qc_datasource = MagicMock()
    processor._qc_datasource.uid = "test_uid"
    processor._qc_datasource.column_names = ["col1"]
    processor._qc_datasource.data = {}
    return processor


def test_public_projection_is_explicit_and_versioned() -> None:
    assert _query_timing().as_public_dict() == {
        "version": 1,
        "query": {
            "query_planning_ms": 1.0,
            "cache_resolution_ms": 2.0,
            "data_acquisition_ms": 3.0,
            "payload_assembly_ms": 4.0,
            "total_ms": 12.0,
        },
    }


def test_public_projection_uses_null_for_non_applicable_phases() -> None:
    timing = QueryTiming(
        query_planning_ns=None,
        cache_resolution_ns=None,
        data_acquisition_ns=None,
        payload_assembly_ns=None,
        total_ns=1,
    )

    assert timing.as_public_dict()["query"] == {
        "query_planning_ms": None,
        "cache_resolution_ms": None,
        "data_acquisition_ms": None,
        "payload_assembly_ms": None,
        "total_ms": 0.0,
    }


@patch("superset.common.query_context_processor.QueryCacheManager")
def test_dataframe_payload_result_keeps_timing_outside_payload(
    cache_manager: MagicMock,
) -> None:
    cache = MagicMock()
    cache.is_loaded = True
    cache.is_cached = True
    cache.df = pd.DataFrame({"col1": [1]})
    cache.cache_dttm = "2026-01-01T00:00:00"
    cache.queried_dttm = "2026-01-01T00:00:00"
    cache.applied_template_filters = []
    cache.applied_filter_columns = []
    cache.rejected_filter_columns = []
    cache.annotation_data = {}
    cache.error_message = None
    cache.query = "SELECT 1"
    cache.status = "success"
    cache.stacktrace = None
    cache.sql_rowcount = 1
    cache.bq_memory_limited = False
    cache.bq_memory_limited_row_count = 0
    cache_manager.get.return_value = cache

    processor = _processor()
    with (
        patch.object(processor, "query_cache_key", return_value="key"),
        patch.object(processor, "get_cache_timeout", return_value=300),
    ):
        result = processor.get_df_payload_result(_query_obj())

    assert "timing" not in result.payload
    assert result.payload["query"] == "SELECT 1"
    assert result.timing.query_planning_ns >= 0
    assert result.timing.cache_resolution_ns >= 0
    assert result.timing.data_acquisition_ns is None
    assert result.timing.payload_assembly_ns >= 0


@patch("superset.common.query_context_processor.QueryCacheManager")
def test_dataframe_payload_result_measures_non_overlapping_stages(
    cache_manager: MagicMock,
) -> None:
    cache = MagicMock()
    cache.is_loaded = False
    cache.df = pd.DataFrame({"col1": [1]})
    cache.cache_dttm = None
    cache.queried_dttm = "2026-01-01T00:00:00"
    cache.applied_template_filters = []
    cache.applied_filter_columns = []
    cache.rejected_filter_columns = []
    cache.annotation_data = {}
    cache.error_message = None
    cache.query = "SELECT 1"
    cache.status = QueryStatus.SUCCESS
    cache.stacktrace = None
    cache.sql_rowcount = 1
    cache.bq_memory_limited = False
    cache.bq_memory_limited_row_count = 0
    cache_manager.get.return_value = cache

    processor = _processor()
    with (
        patch.object(processor, "query_cache_key", return_value="key"),
        patch.object(processor, "get_cache_timeout", return_value=300),
        patch.object(processor, "get_query_result", return_value=MagicMock()),
        patch.object(processor, "get_annotation_data", return_value={}),
        patch(
            "superset.common.query_context_processor.time.perf_counter_ns",
            side_effect=[100, 110, 120, 130, 140, 170, 180, 195],
        ),
    ):
        result = processor.get_df_payload_result(_query_obj())

    assert result.timing.query_planning_ns == 10
    assert result.timing.cache_resolution_ns == 10
    assert result.timing.data_acquisition_ns == 30
    assert result.timing.payload_assembly_ns == 15
    cache.set_query_result.assert_called_once()


@patch("superset.common.query_context_processor.QueryCacheManager")
def test_cache_resolution_includes_loaded_cache_compatibility_policy(
    cache_manager: MagicMock,
) -> None:
    """A legacy cached value becomes a miss before cache resolution completes."""
    cache = MagicMock()
    cache.is_loaded = True
    cache.is_cached = True
    cache.df = pd.DataFrame({"col1": [1]})
    cache.cache_dttm = "2026-01-01T00:00:00"
    cache.queried_dttm = "2026-01-01T00:00:00"
    cache.applied_template_filters = []
    cache.applied_filter_columns = []
    cache.rejected_filter_columns = []
    cache.annotation_data = {}
    cache.error_message = None
    cache.query = "SELECT 1"
    cache.status = QueryStatus.SUCCESS
    cache.stacktrace = None
    cache.sql_rowcount = 1
    cache.bq_memory_limited = False
    cache.bq_memory_limited_row_count = 0
    cache_manager.get.return_value = cache

    query_obj = _query_obj()
    query_obj.filter = [{"col": "col1", "op": "==", "val": 1}]
    processor = _processor()
    clock_values = iter((100, 110, 120, 130, 140, 170, 180, 195))
    clock_calls = 0

    def perf_counter_ns() -> int:
        nonlocal clock_calls
        clock_calls += 1
        if clock_calls == 4:
            assert cache.is_loaded is False
        return next(clock_values)

    with (
        patch.object(processor, "query_cache_key", return_value="key"),
        patch.object(processor, "get_cache_timeout", return_value=300),
        patch.object(processor, "get_query_result", return_value=MagicMock()),
        patch.object(processor, "get_annotation_data", return_value={}),
        patch(
            "superset.common.query_context_processor.time.perf_counter_ns",
            side_effect=perf_counter_ns,
        ),
    ):
        result = processor.get_df_payload_result(query_obj)

    assert result.timing.cache_resolution_ns == 10
    assert result.timing.data_acquisition_ns == 30
    cache.set_query_result.assert_called_once()


def test_get_payload_preserves_legacy_shape_without_timing() -> None:
    processor = _processor()
    query_payload = {"data": [{"col1": 1}]}

    with (
        patch(
            "superset.common.query_context_processor.get_query_results_with_timing",
            return_value=QueryDataResult(query_payload, _query_timing()),
        ),
    ):
        result = processor.get_payload()

    assert result == {"queries": [query_payload]}
    assert "timing" not in result["queries"][0]


def test_get_payload_result_keeps_timing_sidecar() -> None:
    processor = _processor()
    query_payload = {"data": [{"col1": 1}]}

    with (
        patch(
            "superset.common.query_context_processor.get_query_results_with_timing",
            return_value=QueryDataResult(query_payload, _query_timing()),
        ),
    ):
        result = processor.get_payload_result()

    assert result.queries[0].payload == query_payload
    assert result.queries[0].timing.total_ns == 12_000_000
    assert "timing" not in result.queries[0].payload


@pytest.fixture
def observed_processor(monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[Any, ...]]:
    """Exercise real query/cache/context operations with an in-memory backend."""
    from superset.common import query_context_processor as processor_module
    from superset.common.utils import query_cache_manager as cache_module
    from superset.observability import events, metrics, runtime

    records: list[dict[str, Any]] = []
    monkeypatch.setattr(runtime, "_CONFIG", {"OBSERVABILITY_ENABLED": True})
    monkeypatch.setattr(events, "get_runtime", lambda: None)
    monkeypatch.setattr(metrics, "get_runtime", lambda: None)
    monkeypatch.setattr(events, "configure_event_logger", lambda: None)
    monkeypatch.setattr(events, "observe_operation", lambda *args: None)
    monkeypatch.setattr(
        events._LOGGER, "info", lambda message: records.append(json.loads(message))
    )
    monkeypatch.setattr(processor_module, "get_user_id", lambda: 71)
    security = MagicMock()
    security.get_rls_cache_key.return_value = []
    monkeypatch.setattr(processor_module, "security_manager", security)

    app = Flask(__name__)
    app.config.update(
        STATS_LOGGER=MagicMock(),
        STORE_CACHE_KEYS_IN_METADATA_DB=False,
        CACHE_DEFAULT_TIMEOUT=300,
        DATA_CACHE_CONFIG={},
        HASH_ALGORITHM="sha256",
    )
    backend = Cache(app, config={"CACHE_TYPE": "SimpleCache"})
    monkeypatch.setattr(cache_module, "_cache", {CacheRegion.DATA: backend})
    monkeypatch.setattr(processor_module.cache_manager, "_data_cache", backend)

    datasource = MagicMock()
    datasource.id = 7
    datasource.database_id = 8
    datasource.database = Database(
        id=8,
        database_name="observability-fixture",
        sqlalchemy_uri="sqlite://",
        extra="{}",
    )
    datasource.type = "table"
    datasource.uid = "7__table"
    datasource.changed_on = datetime(2026, 1, 1)
    datasource.column_names = ["col1"]
    datasource.cache_timeout = 300
    datasource.get_extra_cache_keys.return_value = []
    datasource.get_query_result.return_value = QueryResult(
        df=pd.DataFrame({"col1": ["private-result", "private-result"]}),
        query="SELECT 'private-sql' AS col1",
        duration=timedelta(),
        applied_filter_columns=["col1"],
    )
    filters = [{"col": "col1", "op": "==", "val": "private-filter"}]
    raw_query = {"columns": ["col1"], "metrics": [], "filters": filters}
    query = QueryObject(columns=["col1"], filters=filters, datasource=datasource)
    context = QueryContext(
        datasource=datasource,
        queries=[query],
        slice_=None,
        form_data={"slice_id": 31, "dashboardId": 32},
        result_type=ChartDataResultType.FULL,
        result_format=ChartDataResultFormat.JSON,
        cache_values={
            "datasource": {"id": 7, "type": "table"},
            "queries": [raw_query],
        },
    )
    with app.app_context():
        yield context._processor, backend, records


def test_chart_context_and_cache_identity_are_independent(
    observed_processor: tuple[Any, ...],
) -> None:
    """Different request assets/actors share the same authorized functional cache."""
    processor, backend, records = observed_processor
    context = processor._query_context
    query = context.queries[0]
    serialized = deepcopy(serialize_query(context, 0))
    cache_values = deepcopy(context.cache_values)
    cache_key = processor.query_cache_key(query)
    context_cache_key = processor.cache_key()
    before = get_context()

    for source, chart_id, actor in (("report", 101, 19), ("warmup", 102, 20)):
        with telemetry_context(
            source=source,
            chart_id=chart_id,
            dashboard_id=202,
            request_actor_user_id=actor,
            query_index=3,
        ):
            result = processor.get_df_payload_result(query)
            assert processor.query_cache_key(query) == cache_key
            assert processor.cache_key() == context_cache_key
            assert serialize_query(context, 0) == serialized
            assert context.cache_values == cache_values
            assert get_context()["request_actor_user_id"] == actor
        assert result.payload["query"] == "SELECT 'private-sql' AS col1"

    context.datasource.get_query_result.assert_called_once_with(query)
    assert result.payload["is_cached"] is True
    assert get_context() == before
    assert "request_actor_user_id" not in backend.get(cache_key)
    finished = [
        record["metadata"]
        for record in records
        if record["message"] == "chart.query.finished"
    ]
    assert [
        (
            event["source"],
            event["chart_id"],
            event["request_actor_user_id"],
            event["cache_hit"],
        )
        for event in finished
    ] == [
        ("report", 101, 19, False),
        ("warmup", 102, 20, True),
    ]
    for event in finished:
        assert event["effective_user_id"] == 71
        assert event["dataset_id"] == 7
        assert event["database_id"] == 8
        assert event["dashboard_id"] == 202
        assert event["query_index"] == 3
        assert event["rows"] == 2
        assert event["result_bytes"] > 0
        assert event["duration_seconds"] >= 0
    names = {record["message"] for record in records}
    assert {
        "chart.query.plan.finished",
        "chart.cache.resolve.finished",
        "chart.data.acquire.finished",
        "chart.payload.assemble.finished",
        "cache.lookup.finished",
        "cache.write.finished",
    } <= names
    assert "private-sql" not in json.dumps(records)
    assert "private-result" not in json.dumps(records)
    assert "private-filter" not in json.dumps(records)


def test_forced_refresh_observes_one_recompute_per_nonce(
    observed_processor: tuple[Any, ...],
) -> None:
    """Telemetry does not bypass the existing forced-refresh read-back marker."""
    processor, _, records = observed_processor
    context = processor._query_context
    context.force = True
    context.force_nonce = "refresh-1"
    query = context.queries[0]
    query.force_nonce = "query-task-1"
    cache_key = processor.query_cache_key(query)

    first = processor.get_df_payload_result(query)
    read_back = processor.get_df_payload_result(query)
    query.force_nonce = "query-task-2"
    second_refresh = processor.get_df_payload_result(query)

    assert (
        first.payload["cache_key"]
        == read_back.payload["cache_key"]
        == second_refresh.payload["cache_key"]
        == cache_key
    )
    assert context.datasource.get_query_result.call_count == 2
    assert read_back.payload["is_cached"] is True
    finished = [
        record["metadata"]
        for record in records
        if record["message"] == "chart.query.finished"
    ]
    assert [event["force_refresh"] for event in finished] == [True, True, True]
    assert [event["cache_hit"] for event in finished] == [False, True, False]
    assert "query-task-1" not in json.dumps(records)


def test_chart_error_restores_context_and_preserves_exception(
    observed_processor: tuple[Any, ...],
) -> None:
    """Error telemetry captures only a class and does not replace the driver error."""
    processor, backend, records = observed_processor
    context = processor._query_context
    query = context.queries[0]
    failure = ValueError("private-driver-error SELECT private-sql")
    context.datasource.get_query_result.side_effect = failure
    before = get_context()
    with telemetry_context(source="export", request_actor_user_id=19):
        with pytest.raises(ValueError, match=re.escape(str(failure))) as raised:
            processor.get_df_payload_result(query)
        assert raised.value is failure
        assert get_context()["source"] == "export"
        assert "dataset_id" not in get_context()
    assert get_context() == before
    assert backend.get(processor.query_cache_key(query)) is None
    finished = [
        record["metadata"]
        for record in records
        if record["message"] == "chart.query.finished"
    ]
    assert len(finished) == 1
    assert finished[0]["outcome"] == "error"
    assert finished[0]["error_type"] == "ValueError"
    assert "private-driver-error" not in json.dumps(records)
    assert "private-sql" not in json.dumps(records)


def test_disabled_chart_context_does_not_read_actor_or_query(
    observed_processor: tuple[Any, ...],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Disabling telemetry preserves real query/cache calls without context reads."""
    from superset.common import query_context_processor as processor_module
    from superset.observability import runtime

    processor, _, records = observed_processor
    monkeypatch.setattr(runtime, "_CONFIG", {"OBSERVABILITY_ENABLED": False})
    monkeypatch.setattr(
        processor_module,
        "get_user_id",
        lambda: pytest.fail("actor read while disabled"),
    )
    monkeypatch.setattr(
        processor_module,
        "get_context",
        lambda: pytest.fail("context read while disabled"),
    )
    with chart_data_context(cast(QueryContext, None)):
        pass
    query = processor._query_context.queries[0]
    key = processor.query_cache_key(query)
    first = processor.get_df_payload_result(query)
    cached = processor.get_df_payload_result(query)
    assert first.payload["cache_key"] == cached.payload["cache_key"] == key
    assert cached.payload["is_cached"] is True
    processor._query_context.datasource.get_query_result.assert_called_once()
    assert records == []


@pytest.mark.parametrize("source", ["report", "warmup", "export"])
def test_background_source_does_not_fabricate_request_actor(
    observed_processor: tuple[Any, ...],
    source: str,
) -> None:
    """An autonomous effective user is not an originating human request actor."""
    processor, _, _ = observed_processor
    with telemetry_context(source=source):
        with chart_data_context(processor._query_context):
            context = get_context()
            assert context["source"] == source
            assert context["effective_user_id"] == 71
            assert context["request_actor_user_id"] is None
