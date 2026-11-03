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
"""Finite, read-only enrichment: ``python -m superset.observability.warehouse``.

Set SUPERSET_OBSERVABILITY_ENABLED=true to enable the standalone process;
disabled runs exit without credentials, connections or instrumentation.

Requires SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ROLE,
SNOWFLAKE_DATABASE and SNOWFLAKE_WAREHOUSE. Only the authenticated user's
INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER is read; no ACCOUNT_USAGE/MONITOR
privilege, Superset app initialization, or metastore access is needed.

SUPERSET_WAREHOUSE_LOOKBACK_SECONDS (900), WINDOW_SECONDS (300), LAG_SECONDS
(60), and MAX_HISTORY_REQUESTS (128), each with the SUPERSET_WAREHOUSE_ prefix,
control bounded, overlapping completion-time windows. Supported lookbacks are
60, 300, 600, 900, 1800, 3600, 7200, 21600, 43200 or 86400 seconds, matching
the finite metric dimension registry. Information Schema may lag; the overlap
is deliberate, not a guarantee against arbitrary upstream delay. Each completed
native query has a stable versioned event ID for downstream deduplication.
Aggregate metrics are gauges over the entire explicit lookback window, grouped
by the actual warehouse and tagged Superset database ID when available, never
replayed cumulative counters. Unknown warehouse/database IDs stay absent.
Missing native durations stay unknown, distinct from application latency.
Native query_type preserves statement kind (including session plumbing) without
inferring from or retrieving SQL text.

The 10,000-row function limit applies BEFORE filtering. Capped windows are
recursively bisected before monitoring queries are excluded. Unresolvable caps
or exhausted request budgets fail explicitly, without publishing an incomplete
window. No SQL text, arbitrary query tags, error messages or result data leave
Snowflake. Collector/log consumers must deduplicate enrichment by event_id.
"""

from __future__ import annotations

import hashlib
import math
import os
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from superset.utils import json

HISTORY_LIMIT = 10_000
TAG_VERSION = 1
TAG_MARKER = "superset_observability"
TAG_IDENTIFIER_KEYS = (
    "statement_id",
    "request_id",
    "execution_id",
    "guest_actor_id",
    "sql_lab_client_id",
    "task_id",
    "celery_task_id",
    "report_execution_id",
    "warmup_run_id",
)
TAG_INTEGER_KEYS = (
    "statement_index",
    "request_actor_user_id",
    "effective_user_id",
    "chart_id",
    "dashboard_id",
    "dataset_id",
    "database_id",
    "sql_lab_query_id",
    "report_schedule_id",
    "query_index",
)
TAG_BOOLEAN_KEYS = ("force_refresh", "async_execution")
LOOKBACK_SECONDS = frozenset({60, 300, 600, 900, 1800, 3600, 7200, 21600, 43200, 86400})
TAG_KEYS = (
    TAG_MARKER,
    "source",
    *TAG_IDENTIFIER_KEYS,
    *TAG_INTEGER_KEYS,
    *TAG_BOOLEAN_KEYS,
    "sql_fingerprint",
)
SOURCES = frozenset(
    (
        "chart",
        "sql_lab",
        "report",
        "warmup",
        "export",
        "metadata",
        "system",
        "browser",
        "observability",
    )
)
TOKEN = re.compile(r"[A-Za-z0-9_.:\-]{1,128}\Z")
WAREHOUSE_NAME = re.compile(r"[A-Za-z0-9_ .()+-]{1,128}\Z", re.ASCII)
TIMINGS = {
    "total_elapsed": "total_elapsed_time",
    "compilation": "compilation_time",
    "execution": "execution_time",
    "queued_overload": "queued_overload_time",
    "queued_provisioning": "queued_provisioning_time",
    "queued_repair": "queued_repair_time",
    "transaction_blocked": "transaction_blocked_time",
}
# Project ONLY our known keys server-side, never retrieve the raw QUERY_TAG.
_TAG_PROJECTION = ",\n        ".join(
    f"'{key}', TRY_PARSE_JSON(query_tag):{key}" for key in TAG_KEYS
)
HISTORY_SQL = f"""
SELECT query_id, session_id, warehouse_name, execution_status, query_type,
       start_time, end_time, {", ".join(TIMINGS.values())},
       TO_JSON(OBJECT_CONSTRUCT_KEEP_NULL(
        {_TAG_PROJECTION}
       )) AS safe_tag
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER(
    END_TIME_RANGE_START => TO_TIMESTAMP_LTZ(%s),
    END_TIME_RANGE_END => DATEADD(nanosecond, -1, TO_TIMESTAMP_LTZ(%s)),
    RESULT_LIMIT => {HISTORY_LIMIT}
))
"""  # noqa: S608


class ConfigurationError(ValueError):
    """Required environment is absent or outside the documented bounds."""


class HistoryCapacityError(RuntimeError):
    """The history cannot be read completely within bounded requests/windows."""


class HistoryDataError(RuntimeError):
    """A required native identifier or completion timestamp is invalid."""


def _env_integer(
    env: Mapping[str, str], suffix: str, default: int, minimum: int, maximum: int
) -> int:
    """Read bounded controls without putting their values into exceptions."""
    name = f"SUPERSET_WAREHOUSE_{suffix}"
    try:
        value = int(env.get(name, str(default)))
    except (TypeError, ValueError):
        raise ConfigurationError(f"{name} must be an integer") from None
    if not minimum <= value <= maximum:
        raise ConfigurationError(f"{name} is outside its supported bounds")
    return value


@dataclass(frozen=True, repr=False)
class WarehouseConfig:
    """Environment-only settings; credentials are deliberately not repr-able."""

    account: str
    user: str
    password: str
    role: str
    database: str
    warehouse: str
    lookback_seconds: int = 900
    window_seconds: int = 300
    lag_seconds: int = 60
    max_history_requests: int = 128

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> WarehouseConfig:
        """Validate credentials and controls without echoing credential values."""
        env = os.environ if env is None else env
        credentials = {}
        for name in ("account", "user", "password", "role", "database", "warehouse"):
            key = f"SNOWFLAKE_{name.upper()}"
            value = env.get(key)
            if not value or not value.strip():
                raise ConfigurationError(f"{key} is required")
            credentials[name] = value
        lookback = _env_integer(env, "LOOKBACK_SECONDS", 900, 60, 86400)
        if lookback not in LOOKBACK_SECONDS:
            raise ConfigurationError(
                "SUPERSET_WAREHOUSE_LOOKBACK_SECONDS is unsupported"
            )
        return cls(
            **credentials,
            lookback_seconds=lookback,
            window_seconds=_env_integer(env, "WINDOW_SECONDS", 300, 1, 3600),
            lag_seconds=_env_integer(env, "LAG_SECONDS", 60, 0, 3600),
            max_history_requests=_env_integer(
                env, "MAX_HISTORY_REQUESTS", 128, 1, 1024
            ),
        )


def parse_tag(value: object) -> dict[str, str | int | bool]:  # noqa: C901
    """Accept only versioned, bounded, primitive execution metadata."""
    if isinstance(value, str):
        if len(value) > 8192:
            return {}
        try:
            value = json.loads(value)
        except (ValueError, RecursionError):
            return {}
    if not isinstance(value, dict):
        return {}
    marker = value.get(TAG_MARKER)
    if type(marker) is not int or marker != TAG_VERSION:
        return {}
    result: dict[str, str | int | bool] = {}
    source = value.get("source")
    if isinstance(source, str) and source in SOURCES:
        result["source"] = source
    for key in TAG_IDENTIFIER_KEYS:
        item = value.get(key)
        if isinstance(item, str) and TOKEN.fullmatch(item):
            result[key] = item
    for key in TAG_INTEGER_KEYS:
        item = value.get(key)
        if type(item) is int and 0 <= item <= 2**63 - 1:
            result[key] = item
        elif isinstance(item, str) and re.fullmatch(r"[0-9]{1,19}", item):
            number = int(item)
            if number <= 2**63 - 1:
                result[key] = number
    for key in TAG_BOOLEAN_KEYS:
        item = value.get(key)
        if type(item) is bool:
            result[key] = item
    fingerprint = value.get("sql_fingerprint")
    if isinstance(fingerprint, str) and re.fullmatch(r"[a-f0-9]{64}", fingerprint):
        result["sql_fingerprint"] = fingerprint
    return result


def query_event_id(query_id: str) -> str:
    """Versioned identity independent of collection run or overlapping window."""
    return "warehouse-query-v1-" + hashlib.sha256(query_id.encode("utf-8")).hexdigest()


def _timestamp(value: object) -> datetime:
    """Require timezone-aware native timestamps; never guess local timezone."""
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise HistoryDataError("Native history timestamp is invalid")
    return value.astimezone(timezone.utc)


def _seconds(value: object) -> float | None:
    """Convert native milliseconds; null/invalid values are not observed zeros."""
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        return None
    try:
        result = float(value) / 1000
    except (OverflowError, ValueError):
        return None
    return result if math.isfinite(result) and result >= 0 else None


def _native_token(value: object) -> str | None:
    """Retain bounded native identifiers, not arbitrary driver values."""
    if type(value) is int and value >= 0:
        value = str(value)
    return value if isinstance(value, str) and TOKEN.fullmatch(value) else None


def _record(row: Mapping[str, Any]) -> dict[str, Any] | None:
    """Normalize one private projection, excluding the monitoring session."""
    context = parse_tag(row.get("safe_tag"))
    if context.get("source") == "observability":
        return None
    query_id = _native_token(row.get("query_id"))
    if not query_id:
        raise HistoryDataError("Native query identifier is missing")
    event: dict[str, Any] = {
        **context,
        "event_id": query_event_id(query_id),
        "snowflake_query_id": query_id,
        "source": context.get("source", "system"),
        "db_system": "snowflake",
        "metric_scope": "own_user",
        "started_at": _timestamp(row.get("start_time")).isoformat(),
        "finished_at": _timestamp(row.get("end_time")).isoformat(),
    }
    session_id = _native_token(row.get("session_id"))
    if session_id is not None:
        event["snowflake_session_id"] = session_id
    warehouse = row.get("warehouse_name")
    if isinstance(warehouse, str) and WAREHOUSE_NAME.fullmatch(warehouse):
        event["warehouse_name"] = warehouse
    status = row.get("execution_status")
    if isinstance(status, str) and status.lower() in (
        "success",
        "fail",
        "failed_with_error",
        "failed_with_incident",
        "canceled",
        "cancelled",
        "failed",
        "incident",
    ):
        event["native_status"] = status.lower()
    query_type = row.get("query_type")
    if isinstance(query_type, str) and re.fullmatch(
        r"[A-Za-z][A-Za-z0-9_]{0,63}", query_type
    ):
        event["query_type"] = query_type.lower()
    for phase, column in TIMINGS.items():
        seconds = _seconds(row.get(column))
        if seconds is not None:
            event[f"warehouse_{phase}_seconds"] = seconds
    return event


@dataclass(frozen=True)
class HistoryWindow:
    """A completely collected window; detailed events are native-query keyed."""

    start: datetime
    end: datetime
    records: dict[str, dict[str, Any]]
    history_query_count: int


def collect_history(
    connection: Any, config: WarehouseConfig, now: datetime | None = None
) -> HistoryWindow:
    """Recursively partition capped own-user history without partial emission."""
    end = _timestamp(now or datetime.now(timezone.utc)) - timedelta(
        seconds=config.lag_seconds
    )
    start = end - timedelta(seconds=config.lookback_seconds)
    requests = 0
    records: dict[str, dict[str, Any]] = {}

    def read_window(lower: datetime, upper: datetime) -> None:
        nonlocal requests
        if requests >= config.max_history_requests:
            raise HistoryCapacityError("History request budget exhausted")
        requests += 1
        with connection.cursor() as cursor:
            cursor.execute(HISTORY_SQL, (lower.isoformat(), upper.isoformat()))
            columns = [column[0].lower() for column in cursor.description]
            rows = cursor.fetchall()
        # Count BEFORE filtering by source, tags, or half-open boundaries.
        if len(rows) >= HISTORY_LIMIT:
            midpoint = lower + (upper - lower) // 2
            if midpoint <= lower or midpoint >= upper:
                raise HistoryCapacityError("History timestamp partition remains capped")
            # Discard the capped parent, never use a potentially truncated subset.
            del rows
            read_window(lower, midpoint)
            read_window(midpoint, upper)
            return
        for values in rows:
            row = dict(zip(columns, values, strict=True))
            completed = _timestamp(row.get("end_time"))
            if lower <= completed < upper:
                record = _record(row)
                if record is not None:
                    records[record["snowflake_query_id"]] = record

    lower = start
    while lower < end:
        upper = min(lower + timedelta(seconds=config.window_seconds), end)
        read_window(lower, upper)
        lower = upper
    return HistoryWindow(start, end, records, requests)


def publish_window(window: HistoryWindow) -> None:
    """Emit stable enrichment records and bounded, explicitly windowed gauges."""
    from superset.observability import emit_event, gauge  # noqa: PLC0415

    duration = int((window.end - window.start).total_seconds())
    dimensions = {
        "source": "observability",
        "db_system": "snowflake",
        "metric_scope": "own_user",
        "window_seconds": duration,
    }
    groups: dict[tuple[str | None, int | None], list[dict[str, Any]]] = {}
    for record in window.records.values():
        emit_event("warehouse.query.enriched", **record)
        group = (record.get("warehouse_name"), record.get("database_id"))
        groups.setdefault(group, []).append(record)
    if not groups:
        groups[(None, None)] = []
    for (warehouse_name, database_id), records in groups.items():
        group_dimensions = dict(dimensions)
        if warehouse_name is not None:
            group_dimensions["warehouse_name"] = warehouse_name
        if database_id is not None:
            group_dimensions["database_id"] = database_id
        gauge("warehouse.own_queries.window_count", len(records), **group_dimensions)
        for phase in TIMINGS:
            field = f"warehouse_{phase}_seconds"
            observations = [record[field] for record in records if field in record]
            gauge(
                "warehouse.own_queries.window_duration_known_count",
                len(observations),
                phase=phase,
                **group_dimensions,
            )
            if observations:
                gauge(
                    "warehouse.own_queries.window_duration_seconds",
                    math.fsum(observations),
                    phase=phase,
                    **group_dimensions,
                )
    emit_event(
        "warehouse.enrichment.window",
        **dimensions,
        window_start=window.start.isoformat(),
        window_end=window.end.isoformat(),
        query_count=len(window.records),
        history_query_count=window.history_query_count,
    )


def _connect(config: WarehouseConfig) -> Any:
    """Open a dedicated monitoring-tagged connection with bounded I/O."""
    import snowflake.connector  # noqa: PLC0415

    return snowflake.connector.connect(
        account=config.account,
        user=config.user,
        password=config.password,
        role=config.role,
        database=config.database,
        warehouse=config.warehouse,
        login_timeout=15,
        network_timeout=15,
        socket_timeout=15,
        session_parameters={
            "QUERY_TAG": json.dumps(
                {TAG_MARKER: TAG_VERSION, "source": "observability"}
            ),
            "TIMEZONE": "UTC",
            "STATEMENT_TIMEOUT_IN_SECONDS": 30,
        },
    )


def _failure_category(error: Exception) -> str:
    """Classify failures using codes only, never credential-bearing messages."""
    if isinstance(error, ConfigurationError):
        return "configuration"
    if isinstance(error, HistoryCapacityError):
        return "history_capacity"
    if isinstance(error, HistoryDataError):
        return "invalid_history"
    if isinstance(error, ImportError):
        return "connector_unavailable"
    state = getattr(error, "sqlstate", None)
    if state == "28000" or getattr(error, "errno", None) in (390100, 390101, 390144):
        return "authentication"
    if state == "42501" or getattr(error, "errno", None) in (2003, 3001):
        return "permissions"
    return "snowflake_connection_or_query"


def main() -> int:
    """Run once, reporting failures explicitly without credentials/tracebacks."""
    from superset.observability import (  # noqa: PLC0415
        emit_event,
        init_process,
        is_enabled,
    )

    init_process(role="warehouse-enricher")
    if not is_enabled():
        return 0
    try:
        config = WarehouseConfig.from_env()
        connection = _connect(config)
        try:
            window = collect_history(connection, config)
        finally:
            connection.close()
        publish_window(window)
    except Exception as error:  # pylint: disable=broad-except
        category = _failure_category(error)
        error_type = type(error).__name__
        emit_event(
            "warehouse.enrichment.failed",
            source="observability",
            db_system="snowflake",
            metric_scope="own_user",
            error_category=category,
            error_type=error_type if TOKEN.fullmatch(error_type) else "Exception",
            outcome="error",
        )
        print(
            f"Warehouse enrichment failed: {category}; check environment, credentials, "
            "own-user history access, and window limits.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
