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
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from flask import Flask

from superset.common.utils.query_cache_manager import QueryCacheManager
from superset.constants import CacheRegion
from superset.exceptions import CacheLoadError
from superset.utils import json


class TestQueryCacheManagerGet:
    def test_get_cache_miss(self):
        """A regular cache miss returns an empty, non-loaded QueryCacheManager."""
        mock_cache = MagicMock()
        mock_cache.get.return_value = None

        with patch(
            "superset.common.utils.query_cache_manager._cache",
            {CacheRegion.DEFAULT: mock_cache},
        ):
            result = QueryCacheManager.get(key="some-key")

        assert result.is_loaded is False
        assert result.cache_value is None

    def test_get_cache_backend_error_fails_open(self):
        """
        If the cache backend raises on read (e.g. a Redis connection or
        timeout error), QueryCacheManager.get() should not propagate the
        exception. It should behave exactly like a cache miss so callers
        fall back to querying live data.
        """
        mock_cache = MagicMock()
        mock_cache.get.side_effect = ConnectionError("connection refused")

        with patch(
            "superset.common.utils.query_cache_manager._cache",
            {CacheRegion.DEFAULT: mock_cache},
        ):
            result = QueryCacheManager.get(key="some-key")

        miss_result = QueryCacheManager()
        assert result.is_loaded == miss_result.is_loaded
        assert result.cache_value == miss_result.cache_value
        assert result.status == miss_result.status


@pytest.fixture
def cache_events(monkeypatch: pytest.MonkeyPatch, app: Flask) -> list[dict[str, Any]]:
    """Capture actual sanitized telemetry envelopes without starting an exporter."""
    from superset.observability import events, metrics, runtime

    records: list[dict[str, Any]] = []
    monkeypatch.setattr(runtime, "_CONFIG", {"OBSERVABILITY_ENABLED": True})
    monkeypatch.setitem(app.config, "OBSERVABILITY_ENABLED", True)
    monkeypatch.setattr(events, "get_runtime", lambda: None)
    monkeypatch.setattr(metrics, "get_runtime", lambda: None)
    monkeypatch.setattr(events, "configure_event_logger", lambda: None)
    monkeypatch.setattr(events, "observe_operation", lambda *args: None)
    monkeypatch.setattr(
        events._LOGGER, "info", lambda message: records.append(json.loads(message))
    )
    return records


@pytest.mark.parametrize("force_cached", [False, True])
def test_cache_read_failure_preserves_fallback_and_safe_error(
    cache_events: list[dict[str, Any]],
    force_cached: bool,
) -> None:
    """A measured outage remains a cache miss, or the existing force-cache error."""
    backend = MagicMock()
    backend.get.side_effect = ConnectionError("private-backend-error")
    with patch(
        "superset.common.utils.query_cache_manager._cache",
        {CacheRegion.DEFAULT: backend},
    ):
        if force_cached:
            with pytest.raises(CacheLoadError):
                QueryCacheManager.get("private-cache-key", force_cached=True)
        else:
            assert QueryCacheManager.get("private-cache-key").is_loaded is False
    finished = [
        event["metadata"]
        for event in cache_events
        if event["message"] == "cache.lookup.finished"
    ]
    assert len(finished) == 1
    assert finished[0]["outcome"] == "error"
    assert finished[0]["error_type"] == "ConnectionError"
    assert "private-backend-error" not in json.dumps(cache_events)
    assert "private-cache-key" not in json.dumps(cache_events)


def test_force_query_bypasses_backend_and_cache_read_telemetry(
    cache_events: list[dict[str, Any]],
) -> None:
    """A forced source query performs no cache lookup or invented cache hit."""
    backend = MagicMock()
    with patch(
        "superset.common.utils.query_cache_manager._cache",
        {CacheRegion.DEFAULT: backend},
    ):
        result = QueryCacheManager.get("query-key", force_query=True)
    backend.get.assert_not_called()
    assert result.is_loaded is False
    assert cache_events == []


def test_cache_write_does_not_claim_failed_persistence_succeeded(
    cache_events: list[dict[str, Any]],
) -> None:
    """The backend result is unchanged and a failed write is not logged as success."""
    backend = MagicMock()
    backend.set.return_value = False
    app = Flask(__name__)
    value = {"df": pd.DataFrame({"col1": [1]}), "query": "private-sql"}
    with (
        app.app_context(),
        patch(
            "superset.common.utils.query_cache_manager._cache",
            {CacheRegion.DEFAULT: backend},
        ),
    ):
        assert QueryCacheManager.set("private-cache-key", value, timeout=300) is False
    finished = [
        event["metadata"]
        for event in cache_events
        if event["message"] == "cache.write.finished"
    ]
    assert len(finished) == 1
    assert finished[0]["outcome"] == "not_persisted"
    assert finished[0]["rows"] == 1
    assert finished[0]["result_bytes"] > 0
    assert "private-sql" not in json.dumps(cache_events)
    assert "private-cache-key" not in json.dumps(cache_events)
