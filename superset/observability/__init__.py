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
"""Superset observability's dependency-light, fail-open public instrumentation API.

Events and traces may contain explicitly allowlisted execution/asset IDs; native
cumulative metrics exclude user/query IDs and bound database/warehouse assets.
This context must not be serialized into functional query payloads/cache/dedup
keys. Standalone tools call init_process without constructing Flask or syncing
the metadata DB. App bootstrap installs hooks; eventual PIDs own all exporters.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from superset.observability.context import (
    get_context,
    restore_context,
    telemetry_context,
    TelemetryValue,
    wire_context,
)
from superset.observability.engine import instrument_engine
from superset.observability.events import (
    configure_event_logger,
    emit_event,
    Operation,
    operation,
)
from superset.observability.http import register_http, signed_execution_headers
from superset.observability.legacy import register_mcp_tools
from superset.observability.metrics import gauge, increment, observe_duration
from superset.observability.runtime import configure, enabled, init_process

if TYPE_CHECKING:
    from flask import Flask

__all__ = [
    "TelemetryValue",
    "Operation",
    "telemetry_context",
    "get_context",
    "emit_event",
    "operation",
    "increment",
    "gauge",
    "observe_duration",
    "register_mcp_tools",
    "initialize",
    "init_process",
    "is_enabled",
    "signed_execution_headers",
    "wire_context",
    "restore_context",
    "instrument_engine",
]


def is_enabled() -> bool:
    """Return the effective app/process setting without initializing exporters."""
    return enabled()


def initialize(app: Flask) -> None:
    """Install app hooks after normal Superset initialization, without exporters."""
    if app.extensions.get("superset_observability"):
        return
    configure(app.config)
    configure_event_logger()
    register_http(app)

    # These explicit owner entrypoints import no Flask app and never create an
    # SDK provider. Importing them here also keeps standalone warehouse tools
    # independent of Superset's Celery/ORM initialization.
    from superset.observability.browser import register_browser
    from superset.observability.lifecycle import register_lifecycle

    register_lifecycle(app)
    register_browser(app)
    if enabled():
        from superset.extensions import celery_app, db, stats_logger_manager
        from superset.observability.metrics import configure_metric_queues
        from superset.observability.stats import ObservabilityStatsLogger

        configure_metric_queues(celery_app.amqp.queues)

        if not isinstance(app.config["STATS_LOGGER"], ObservabilityStatsLogger):
            app.config["STATS_LOGGER"] = ObservabilityStatsLogger(
                app.config["STATS_LOGGER"]
            )
            stats_logger_manager.init_app(app)
        with app.app_context():
            instrument_engine(db.engine, role="metastore")
    app.extensions["superset_observability"] = True
