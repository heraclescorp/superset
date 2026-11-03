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
"""SQLAlchemy connection/pool health only; never SQL/bind/URL instrumentation."""

from __future__ import annotations

import os
import threading
import weakref
from typing import Any

from superset.observability.diagnostics import record_failure
from superset.observability.events import emit_event, exception_attributes
from superset.observability.metrics import gauge, increment
from superset.observability.runtime import enabled

_ENGINES: weakref.WeakSet[Any] = weakref.WeakSet()
_LOCK = threading.RLock()


def instrument_engine(  # noqa: C901
    engine: Any, *, role: str, database_id: int | None = None
) -> None:
    """Attach once without retaining an engine or modifying exception/transactions.

    Connection acquisition latency is measured by the caller's explicit
    db.connection.acquire scope, not guessed from checkout notification time.
    No driver statement spans are created here. NullPool has no meaningful
    size/utilization and therefore produces no fake utilization observations.
    """
    if not enabled():
        return
    from sqlalchemy import event
    from sqlalchemy.pool import QueuePool

    try:
        with _LOCK:
            if engine in _ENGINES:
                return
            _ENGINES.add(engine)
        engine_ref = weakref.ref(engine)
        attributes = {
            "role": role,
            "db_system": engine.dialect.name,
            "database_id": database_id,
        }
        if role == "metastore":
            attributes["warehouse_name"] = None
        event_attributes = {**attributes, "database_id": database_id}

        def pool_metrics() -> None:
            try:
                current = engine_ref()
                if current is not None and isinstance(current.pool, QueuePool):
                    gauge(
                        "db.pool.checked_out", current.pool.checkedout(), **attributes
                    )
                    gauge("db.pool.size", current.pool.size(), **attributes)
                    gauge(
                        "db.pool.overflow",
                        max(0, current.pool.overflow()),
                        **attributes,
                    )
            except Exception:  # pylint: disable=broad-except
                record_failure()

        def connect(connection: Any, record: Any) -> None:
            try:
                reconnect = (
                    record.record_info.get("observability_connected_pid") == os.getpid()
                )
                record.record_info["observability_connected_pid"] = os.getpid()
                increment("db.connections", **attributes)
                if reconnect:
                    increment("db.reconnects", **attributes)
                emit_event(
                    "db.connection.connected", **event_attributes, outcome="success"
                )
                pool_metrics()
            except Exception:  # pylint: disable=broad-except
                record_failure()

        def checkout(connection: Any, record: Any, proxy: Any) -> None:
            increment("db.checkouts", **attributes)
            pool_metrics()

        def checkin(connection: Any, record: Any) -> None:
            increment("db.checkins", **attributes)
            pool_metrics()

        def invalidate(
            connection: Any, record: Any, error: BaseException | None
        ) -> None:
            try:
                increment("db.invalidations", **attributes)
                safe_error = exception_attributes(error) if error else {}
                emit_event(
                    "db.connection.invalidated",
                    **{**event_attributes, **safe_error, "outcome": "invalidated"},
                )
                pool_metrics()
            except Exception:  # pylint: disable=broad-except
                record_failure()

        def handle_error(context: Any) -> None:
            try:
                error = context.original_exception
                safe_error = (
                    exception_attributes(error)
                    if isinstance(error, BaseException)
                    else {}
                )
                increment(
                    "db.errors",
                    **attributes,
                    error_category="connection"
                    if context.is_disconnect
                    else "database",
                )
                emit_event(
                    "db.connection.error",
                    **{
                        **event_attributes,
                        **safe_error,
                        "error_category": "connection"
                        if context.is_disconnect
                        else "database",
                        "is_disconnect": bool(context.is_disconnect),
                    },
                )
            except Exception:  # pylint: disable=broad-except
                record_failure()

        event.listen(engine, "connect", connect)
        event.listen(engine, "checkout", checkout)
        event.listen(engine, "checkin", checkin)
        event.listen(engine, "invalidate", invalidate)
        event.listen(engine, "soft_invalidate", invalidate)
        event.listen(engine, "handle_error", handle_error)
    except Exception:  # pylint: disable=broad-except
        record_failure()


def _after_fork() -> None:
    global _LOCK
    _LOCK = threading.RLock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork)
