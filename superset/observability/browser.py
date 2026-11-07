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
"""Untrusted browser timings on the existing authenticated, CSRF-protected log route.

No collector endpoint is exposed. The route's existing access decorator runs before
``browser_log``; browser batches bypass the legacy payload logger (which accepts
arbitrary log fields). Every asset is independently authorized, actor identity is
server-derived, and only bounded scalar identifiers/timings enter the dedicated
observability logger. Authorized terminal records also count/observe their reported
client seconds in browser-scoped native metrics, never ingestion wall time or
warehouse work. Started records remain events only; all observations are untrusted.
"""

from __future__ import annotations

import functools
import math
import re
from collections.abc import Callable
from typing import Any, ParamSpec, TypeAlias

from flask import current_app, Flask, g, request, Response
from flask_wtf.csrf import CSRFError

from superset import db, security_manager
from superset.daos.datasource import DatasourceDAO
from superset.extensions import csrf
from superset.models.dashboard import Dashboard
from superset.models.slice import Slice
from superset.observability import (
    emit_event,
    get_context,
    is_enabled,
    observe_duration,
    telemetry_context,
)
from superset.observability.diagnostics import record_failure
from superset.security.guest_token import GuestUser
from superset.utils import json

MAX_BODY_BYTES = 64 * 1024
MAX_EVENTS = 64
MAX_DURATION_SECONDS = 24 * 60 * 60
IDENTIFIER = re.compile(r"[A-Za-z0-9_-]{8,64}\Z")
EVENT_PHASES = {
    "chart.load": {"load", "data", "render"},
    "chart.data_ready": {"data"},
    "chart.render": {"render"},
    "chart.request": {"request"},
    "dashboard.load": {"load"},
}
OUTCOMES = {"started", "success", "error", "abandoned"}
NAVIGATION_TYPES = {"navigate", "reload", "back_forward", "prerender", "unknown"}
ROUTE_KINDS = {"dashboard", "explore", "embedded", "other"}
VIEWPORT_CLASSES = {"small", "medium", "large"}
CONNECTION_TYPES = {"slow-2g", "2g", "3g", "4g"}
CLIENT_VERSION = re.compile(r"[A-Za-z0-9._+-]{1,64}\Z")
P = ParamSpec("P")
_AssetKey: TypeAlias = tuple[bool, int | None, int | None, int | None, str | None]


def register_browser(app: Flask) -> None:
    """Enable the safe log-route branch without registering any public route."""
    app.extensions["observability_browser"] = True


def _positive_id(value: object) -> int | None:
    if type(value) is int and 0 < value <= 2**53 - 1:
        return value
    return None


def _identifier(value: object) -> str | None:
    return value if isinstance(value, str) and IDENTIFIER.fullmatch(value) else None


def _actor() -> dict[str, Any] | None:
    """Separate the trusted producer initiator from the authenticated executor."""
    user = getattr(g, "user", None)
    if user is None:
        return None
    user_id = _positive_id(getattr(user, "id", None))
    trusted = get_context()
    # Foundation admits this context only from server identity or a verified
    # purpose-signed producer header, never browser JSON or arbitrary headers.
    initiator = (
        _positive_id(trusted["request_actor_user_id"])
        if "request_actor_user_id" in trusted
        else user_id
    )
    if security_manager.is_guest_user(user):
        # Foundation owns this request pseudoprincipal; it does not identify a
        # distinct human and must match query/worker event correlation exactly.
        guest_id = _identifier(trusted.get("guest_actor_id"))
        if guest_id is None:
            return None
        return {
            "request_actor_user_id": initiator,
            "effective_user_id": None,
            "guest_actor_id": guest_id,
        }
    if not getattr(user, "is_authenticated", False) or user_id is None:
        return None
    return {
        "request_actor_user_id": initiator,
        "effective_user_id": user_id,
        "guest_actor_id": None,
    }


def _parse_event(value: object) -> dict[str, Any] | None:  # noqa: C901
    """Build a fresh allowlisted record, never forward client-supplied metadata."""
    if not isinstance(value, dict):
        return None
    event_name = value.get("event_name")
    phase = value.get("phase")
    outcome = value.get("outcome")
    if (
        not isinstance(event_name, str)
        or event_name not in EVENT_PHASES
        or not isinstance(phase, str)
        or phase not in EVENT_PHASES[event_name]
        or not isinstance(outcome, str)
        or outcome not in OUTCOMES
    ):
        return None
    event_id = _identifier(value.get("browser_event_id"))
    view_id = _identifier(value.get("browser_view_id"))
    duration = value.get("duration_seconds")
    if (
        event_id is None
        or view_id is None
        or type(duration) not in (int, float)
        or not math.isfinite(duration)
        or not 0 <= duration <= MAX_DURATION_SECONDS
    ):
        return None
    parsed: dict[str, Any] = {
        "event_name": event_name,
        "browser_event_id": event_id,
        "browser_view_id": view_id,
        "phase": phase,
        "outcome": outcome,
        "duration_seconds": float(duration),
    }
    active_duration = value.get("active_duration_seconds")
    if active_duration is not None:
        if (
            type(active_duration) not in (int, float)
            or not math.isfinite(active_duration)
            or not 0 <= active_duration <= MAX_DURATION_SECONDS
        ):
            return None
        parsed["active_duration_seconds"] = float(active_duration)
    hidden_duration = value.get("hidden_duration_seconds")
    if hidden_duration is not None:
        if (
            type(hidden_duration) not in (int, float)
            or not math.isfinite(hidden_duration)
            or not 0 <= hidden_duration <= MAX_DURATION_SECONDS
            or hidden_duration > float(duration) + 1e-6
        ):
            return None
        parsed["hidden_duration_seconds"] = float(hidden_duration)
    for key in ("browser_load_id", "request_id"):
        if (identifier := _identifier(value.get(key))) is not None:
            parsed[key] = identifier
    for key in ("chart_id", "dashboard_id", "dataset_id"):
        if key in value:
            if (identifier_id := _positive_id(value[key])) is None:
                return None
            parsed[key] = identifier_id
    for key in ("expected_charts", "completed_charts"):
        count = value.get(key)
        if type(count) is int and 0 <= count <= 10000:
            parsed[key] = count
    for key in (
        "cache_hit",
        "async_execution",
        "force_refresh",
        "backgrounded",
        "fully_backgrounded",
        "automation",
    ):
        if key in value:
            if type(value[key]) is not bool:
                return None
            parsed[key] = value[key]
    for key, allowed in (
        ("navigation_type", NAVIGATION_TYPES),
        ("route_kind", ROUTE_KINDS),
        ("viewport_class", VIEWPORT_CLASSES),
        ("connection_type", CONNECTION_TYPES),
    ):
        if key in value:
            if not isinstance(value[key], str) or value[key] not in allowed:
                return None
            parsed[key] = value[key]
    if "client_version" in value:
        version = value["client_version"]
        if not isinstance(version, str) or not CLIENT_VERSION.fullmatch(version):
            return None
        parsed["client_version"] = version
    if "http_status_code" in value:
        status = value["http_status_code"]
        if type(status) is not int or not 100 <= status <= 599:
            return None
        parsed["http_status_code"] = status
    if event_name.startswith("chart.") and "browser_load_id" not in parsed:
        return None
    return parsed


def _asset_key(value: dict[str, Any], parsed: dict[str, Any]) -> _AssetKey | None:
    """Key only validated claims, separating dashboard from chart observations."""
    datasource_type = None
    if parsed.get("dataset_id") is not None:
        datasource_type = value.get("datasource_type")
        if (
            not isinstance(datasource_type, str)
            or datasource_type not in DatasourceDAO.sources
        ):
            return None
    return (
        parsed["event_name"] == "dashboard.load",
        parsed.get("chart_id"),
        parsed.get("dashboard_id"),
        parsed.get("dataset_id"),
        datasource_type,
    )


def _authorized_assets(value: dict[str, Any], parsed: dict[str, Any]) -> bool:  # noqa: C901
    """Authorize each claimed asset, including dashboard/chart membership."""
    dashboard = None
    chart = None
    if dashboard_id := parsed.get("dashboard_id"):
        dashboard = db.session.query(Dashboard).filter_by(id=dashboard_id).one_or_none()
        if dashboard is None:
            return False
        security_manager.raise_for_access(dashboard=dashboard)
    if chart_id := parsed.get("chart_id"):
        chart = db.session.query(Slice).filter_by(id=chart_id).one_or_none()
        if chart is None:
            return False
        security_manager.raise_for_access(chart=chart)
        if dashboard is not None and not any(
            item.id == chart.id for item in dashboard.slices
        ):
            return False
    if parsed["event_name"] == "dashboard.load":
        return dashboard is not None and chart is None and "dataset_id" not in parsed
    if dataset_id := parsed.get("dataset_id"):
        datasource_type = value.get("datasource_type")
        if (
            not isinstance(datasource_type, str)
            or datasource_type not in DatasourceDAO.sources
        ):
            return False
        if chart is not None:
            if (
                chart.datasource_id != dataset_id
                or chart.datasource_type != datasource_type
            ):
                return False
        else:
            # Resolve directly: get_datasource mutates virtual dataset SQL for
            # attribution, which must never happen as a side effect of telemetry.
            model = DatasourceDAO.sources[datasource_type]
            datasource = db.session.query(model).filter_by(id=dataset_id).one_or_none()
            if datasource is None or security_manager.is_guest_user():
                return False
            security_manager.raise_for_access(datasource=datasource)
        parsed["datasource_type"] = datasource_type
    elif (
        chart is not None
        and _positive_id(chart.datasource_id) is not None
        and chart.datasource_type in DatasourceDAO.sources
    ):
        parsed["dataset_id"] = chart.datasource_id
        parsed["datasource_type"] = chart.datasource_type
    return chart is not None or "dataset_id" in parsed


def ingest_browser_events(events: object) -> None:  # noqa: C901
    """Fail open for business requests, fail closed for invalid telemetry."""
    if (
        not is_enabled()
        or not current_app.extensions.get("observability_browser")
        or not isinstance(events, list)
        or len(events) > MAX_EVENTS
    ):
        return
    try:
        actor = _actor()
        if actor is None:
            return
        # Deduplicate within a batch only; client IDs are never globally trusted.
        seen: set[str] = set()
        # One principal and one bounded payload only: never reuse authorization
        # across requests. All chart phases share the same asset authorization.
        authorized: dict[_AssetKey, dict[str, int | str] | None] = {}
        for value in events:
            parsed = _parse_event(value)
            if parsed is None or parsed["browser_event_id"] in seen:
                continue
            seen.add(parsed["browser_event_id"])
            try:
                key = _asset_key(value, parsed)
                if key is None:
                    continue
                if key not in authorized:
                    # Cache denial too; repeated invalid phases cannot amplify
                    # metadata reads or retry a failed access check in this batch.
                    authorized[key] = None
                    if _authorized_assets(value, parsed):
                        authorized[key] = {
                            field: parsed[field]
                            for field in (
                                "chart_id",
                                "dashboard_id",
                                "dataset_id",
                                "datasource_type",
                            )
                            if field in parsed
                        }
                canonical_assets = authorized[key]
                if canonical_assets is None:
                    continue
                parsed.update(canonical_assets)
                event_name = parsed.pop("event_name")
                # The ingestion HTTP request is not the chart request. No supplied
                # source/actor/error/SQL/filter/token fields can escape this scope.
                with telemetry_context(
                    source="browser",
                    request_id=None,
                    request_actor_user_id=None,
                    effective_user_id=None,
                    guest_actor_id=None,
                ):
                    if parsed["outcome"] != "started":
                        try:
                            observe_duration(
                                f"browser.{event_name}",
                                parsed.get(
                                    "active_duration_seconds",
                                    parsed["duration_seconds"],
                                ),
                                source="browser",
                                phase=parsed["phase"],
                                outcome=parsed["outcome"],
                                trust="client_untrusted",
                                **{
                                    key: parsed[key]
                                    for key in (
                                        "cache_hit",
                                        "async_execution",
                                        "force_refresh",
                                        "automation",
                                        "navigation_type",
                                        "route_kind",
                                        "viewport_class",
                                        "connection_type",
                                    )
                                    if key in parsed
                                },
                            )
                        except Exception:  # pylint: disable=broad-except
                            # Independent event/metric failure isolation.
                            record_failure()
                    emit_event(
                        f"browser.{event_name}",
                        **parsed,
                        **actor,
                        source="browser",
                        trust="client_untrusted",
                    )
            except Exception:  # pylint: disable=broad-except
                # No raw error or payload logging, including authorization failures.
                record_failure()
                continue
    except Exception:  # pylint: disable=broad-except
        return


def _protect_browser_csrf() -> None:
    """Protect ambient credentials; admit only a verified selected guest bearer.

    The legacy log route is globally CSRF-exempt. A supplied guest credential
    must never exempt an authenticated cookie user: Flask-Login selects an
    existing session before its request loader considers guest credentials.
    Independently verify the explicit bearer (including type and revocation),
    and compare all claims with the selected GuestUser, not only its username.
    """
    if not current_app.config.get("WTF_CSRF_ENABLED", True):
        return
    user = getattr(g, "user", None)
    if isinstance(user, GuestUser) and security_manager.is_guest_user(user):
        verified = security_manager.get_guest_user_from_request(request)
        if isinstance(verified, GuestUser) and verified.guest_token == user.guest_token:
            return
    csrf.protect()


def browser_log(func: Callable[P, Any]) -> Callable[P, Any]:  # noqa: C901
    """Intercept only performance batches after the existing route access gate.

    Keeping this outside ``event_logger.log_this`` avoids persisting arbitrary
    attacker fields or the form's guest credential in the legacy request log.
    The ``observability=browser`` selector isolates bounded telemetry decoding;
    ordinary legacy logging (including its batch-size behavior) is unchanged.
    """

    @functools.wraps(func)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> Any:  # noqa: C901
        if request.args.get("observability") != "browser":
            return func(*args, **kwargs)
        if not is_enabled():
            return Response(status=200)
        # Bound decoding before restoring CSRF for this globally exempt route.
        # The existing authenticated route gate has already run.
        if request.content_length is None or request.content_length > MAX_BODY_BYTES:
            return Response(status=200)
        try:
            _protect_browser_csrf()
        except CSRFError:
            return Response("CSRF validation failed", status=400)
        if request.is_json:
            try:
                body = request.get_json(silent=True)
            except (ValueError, RecursionError):
                return Response(status=200)
            if isinstance(body, dict) and "observability_events" in body:
                ingest_browser_events(body["observability_events"])
                return Response(status=200)
        elif "observability_events" in request.form:
            raw = request.form["observability_events"]
            if len(raw.encode("utf-8")) <= MAX_BODY_BYTES:
                try:
                    ingest_browser_events(json.loads(raw))
                except (ValueError, RecursionError):
                    record_failure()
            return Response(status=200)
        return Response(status=200)

    return wrapped
