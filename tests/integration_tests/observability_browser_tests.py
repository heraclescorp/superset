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
"""Real log-route CSRF enforcement with authenticated sessions and guest bearers.

The legacy log route is globally exempt in Superset configuration, so a minimal
Flask view without that exemption cannot prove browser-selector protection.
These tests retain real authentication, CSRF extension, route decorators, asset
lookup and authorization; only telemetry sinks are replaced to avoid export I/O.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from types import SimpleNamespace
from uuid import uuid4

import pytest
from flask.ctx import AppContext
from flask.testing import FlaskClient
from pytest_mock import MockerFixture

from superset import db, security_manager
from superset.daos.dashboard import EmbeddedDashboardDAO
from superset.extensions import csrf
from superset.models.dashboard import Dashboard
from superset.observability import browser
from superset.utils import json
from tests.integration_tests.conftest import with_feature_flags
from tests.integration_tests.test_app import app

BROWSER_LOG = "/log/?observability=browser"

# The integration configuration disables CSRF at app creation and therefore skips
# Superset's configure_wtf entirely. Install the real extension once at collection
# before requests, preserving the real exemption list and singleton app identity.
# The default disabled flag stays unchanged outside the scoped fixtures below.
_TEST_CSRF_INSTALLED = "csrf" not in app.extensions
_MISSING_CSRF_GLOBAL = object()
if _TEST_CSRF_INSTALLED:
    _original_before = frozenset(app.before_request_funcs.get(None, []))
    _original_processors = frozenset(app.template_context_processors.get(None, []))
    _original_views = csrf._exempt_views.copy()
    _original_config_keys = frozenset(app.config)
    _original_csrf_global = app.jinja_env.globals.get(
        "csrf_token", _MISSING_CSRF_GLOBAL
    )
    csrf.init_app(app)
    for exempt_view in app.config["WTF_CSRF_EXEMPT_LIST"]:
        csrf.exempt(exempt_view)
    _added_before = set(app.before_request_funcs.get(None, [])) - _original_before
    _added_processors = (
        set(app.template_context_processors.get(None, [])) - _original_processors
    )
    _added_views = csrf._exempt_views - _original_views
    _added_config_keys = set(app.config) - _original_config_keys


@pytest.fixture(scope="module", autouse=True)
def restore_real_test_csrf() -> Iterator[None]:
    """Restore shared app registries after using the real CSRF extension."""
    try:
        yield
    finally:
        if _TEST_CSRF_INSTALLED:
            app.before_request_funcs[None][:] = [
                hook
                for hook in app.before_request_funcs[None]
                if hook not in _added_before
            ]
            app.template_context_processors[None][:] = [
                hook
                for hook in app.template_context_processors[None]
                if hook not in _added_processors
            ]
            if _original_csrf_global is _MISSING_CSRF_GLOBAL:
                app.jinja_env.globals.pop("csrf_token", None)
            else:
                app.jinja_env.globals["csrf_token"] = _original_csrf_global
            csrf._exempt_views.difference_update(_added_views)
            for key in _added_config_keys:
                app.config.pop(key, None)
            app.extensions.pop("csrf", None)


@pytest.fixture
def browser_ingress(
    app_context: AppContext,
    test_client: FlaskClient,
    login_as: Callable[..., None],
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
) -> Iterator[SimpleNamespace]:
    """Log in using the existing fixture before enabling the genuine CSRF gate."""
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
    login_as("admin")
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", True)
    monkeypatch.setitem(app.config, "OBSERVABILITY_ENABLED", True)
    monkeypatch.setitem(
        app.config, "GUEST_TOKEN_JWT_AUDIENCE", "browser-csrf-regression"
    )
    browser.register_browser(app)
    mocker.patch.object(browser, "is_enabled", return_value=True)
    emitted = mocker.patch.object(browser, "emit_event")
    observed = mocker.patch.object(browser, "observe_duration")
    mocker.patch("superset.observability.events.get_runtime", return_value=None)
    mocker.patch("superset.observability.metrics.get_runtime", return_value=None)
    mocker.patch("superset.observability.events.emit_event")
    mocker.patch("superset.utils.log.DBEventLogger.log")

    dashboard = Dashboard(dashboard_title=f"browser-csrf-{uuid4().hex}", published=True)
    db.session.add(dashboard)
    db.session.commit()
    embedded = EmbeddedDashboardDAO.upsert(dashboard, [])
    log_permission = security_manager.find_permission_view_menu("can_log", "Superset")
    assert log_permission is not None
    guest_role = security_manager.add_role(
        f"BrowserTelemetryCsrf-{uuid4().hex}", permissions=[log_permission]
    )
    monkeypatch.setitem(app.config, "GUEST_ROLE_NAME", guest_role.name)
    db.session.commit()
    raw_guest_token = security_manager.create_guest_access_token(
        {"username": "browser-csrf-guest"},
        [{"type": "dashboard", "id": str(embedded.uuid)}],
        [],
    )
    guest_token = (
        raw_guest_token.decode("utf-8")
        if isinstance(raw_guest_token, bytes)
        else raw_guest_token
    )
    csrf_response = test_client.get("/api/v1/security/csrf_token/")
    assert csrf_response.status_code == 200
    csrf_token = csrf_response.get_json()["result"]
    payload = {
        "event_name": "dashboard.load",
        "browser_event_id": "browser-csrf-event-0001",
        "browser_view_id": "browser-csrf-view-0001",
        "dashboard_id": dashboard.id,
        "phase": "load",
        "outcome": "success",
        "duration_seconds": 0.25,
    }
    try:
        yield SimpleNamespace(
            client=test_client,
            csrf_token=csrf_token,
            guest_token=guest_token,
            payload=payload,
            emitted=emitted,
            observed=observed,
        )
    finally:
        db.session.delete(dashboard)
        db.session.delete(guest_role)
        db.session.commit()


@pytest.mark.parametrize("transport", ["json", "form"])
@pytest.mark.parametrize("token", [None, "invalid-csrf-token"])
def test_browser_selector_rejects_session_csrf_missing_or_invalid(
    browser_ingress: SimpleNamespace, transport: str, token: str | None
) -> None:
    headers = {"Origin": "https://sibling.example.test"}
    if transport == "json":
        if token is not None:
            headers["X-CSRFToken"] = token
        response = browser_ingress.client.post(
            BROWSER_LOG,
            json={"observability_events": [browser_ingress.payload]},
            headers=headers,
        )
    else:
        data = {"observability_events": json.dumps([browser_ingress.payload])}
        if token is not None:
            data["csrf_token"] = token
        response = browser_ingress.client.post(
            BROWSER_LOG, data=data, headers=headers, content_type="multipart/form-data"
        )
    assert response.status_code == 400
    browser_ingress.emitted.assert_not_called()
    browser_ingress.observed.assert_not_called()


@pytest.mark.parametrize("transport", ["json", "form"])
def test_browser_selector_accepts_session_csrf_header_and_beacon_form(
    browser_ingress: SimpleNamespace, transport: str
) -> None:
    if transport == "json":
        response = browser_ingress.client.post(
            BROWSER_LOG,
            json={"observability_events": [browser_ingress.payload]},
            headers={"X-CSRFToken": browser_ingress.csrf_token},
        )
    else:
        response = browser_ingress.client.post(
            BROWSER_LOG,
            data={
                "observability_events": json.dumps([browser_ingress.payload]),
                "csrf_token": browser_ingress.csrf_token,
            },
            content_type="multipart/form-data",
        )
    assert response.status_code == 200
    browser_ingress.emitted.assert_called_once()
    browser_ingress.observed.assert_called_once()
    fields = browser_ingress.emitted.call_args.kwargs
    assert fields["request_actor_user_id"] == security_manager.find_user("admin").id
    assert fields["effective_user_id"] == security_manager.find_user("admin").id


def test_legacy_log_retains_its_existing_csrf_exemption(
    browser_ingress: SimpleNamespace,
) -> None:
    assert (
        browser_ingress.client.post("/log/", data={"events": "[]"}).status_code == 200
    )
    browser_ingress.emitted.assert_not_called()
    browser_ingress.observed.assert_not_called()


@with_feature_flags(EMBEDDED_SUPERSET=True)
@pytest.mark.parametrize("transport", ["header", "form"])
def test_verified_guest_bearer_works_without_ambient_cookie_or_session_csrf(
    browser_ingress: SimpleNamespace, transport: str
) -> None:
    # A separate client proves this identity was selected by the explicit bearer,
    # not by the authenticated administrator's ambient session from the fixture.
    with app.app_context():
        client = app.test_client(use_cookies=False)
        if transport == "header":
            response = client.post(
                BROWSER_LOG,
                json={"observability_events": [browser_ingress.payload]},
                headers={
                    app.config["GUEST_TOKEN_HEADER_NAME"]: browser_ingress.guest_token
                },
            )
        else:
            response = client.post(
                BROWSER_LOG,
                data={
                    "observability_events": json.dumps([browser_ingress.payload]),
                    "guest_token": browser_ingress.guest_token,
                },
                content_type="multipart/form-data",
            )
    assert response.status_code == 200
    browser_ingress.emitted.assert_called_once()
    fields = browser_ingress.emitted.call_args.kwargs
    assert fields["request_actor_user_id"] is None
    assert fields["effective_user_id"] is None
    assert len(fields["guest_actor_id"]) == 64


@with_feature_flags(EMBEDDED_SUPERSET=True)
@pytest.mark.parametrize("valid_guest", [False, True])
@pytest.mark.parametrize("transport", ["header", "form"])
def test_guest_credential_cannot_exempt_an_ambient_cookie_principal(
    browser_ingress: SimpleNamespace, valid_guest: bool, transport: str
) -> None:
    token = browser_ingress.guest_token if valid_guest else "invalid-guest-token"
    if transport == "header":
        response = browser_ingress.client.post(
            BROWSER_LOG,
            json={"observability_events": [browser_ingress.payload]},
            headers={app.config["GUEST_TOKEN_HEADER_NAME"]: token},
        )
    else:
        response = browser_ingress.client.post(
            BROWSER_LOG,
            data={
                "observability_events": json.dumps([browser_ingress.payload]),
                "guest_token": token,
            },
            content_type="multipart/form-data",
        )
    assert response.status_code == 400
    browser_ingress.emitted.assert_not_called()
    browser_ingress.observed.assert_not_called()


@with_feature_flags(EMBEDDED_SUPERSET=True)
def test_invalid_guest_bearer_without_session_cannot_emit(
    browser_ingress: SimpleNamespace,
) -> None:
    response = app.test_client(use_cookies=False).post(
        BROWSER_LOG,
        json={"observability_events": [browser_ingress.payload]},
        headers={app.config["GUEST_TOKEN_HEADER_NAME"]: "invalid-guest-token"},
    )
    assert response.status_code in {400, 401, 403}
    browser_ingress.emitted.assert_not_called()
    browser_ingress.observed.assert_not_called()


def test_real_csrf_extension_protects_non_exempt_dashboard_api(
    browser_ingress: SimpleNamespace,
) -> None:
    assert app.extensions["csrf"] is csrf
    # The real production exemption is deliberately still present for /log/.
    assert "superset.views.core.log" in csrf._exempt_views
    response = browser_ingress.client.post(
        "/api/v1/dashboard/", json={"dashboard_title": "must-not-be-created"}
    )
    assert response.status_code == 400
    browser_ingress.emitted.assert_not_called()
    browser_ingress.observed.assert_not_called()


@with_feature_flags(EMBEDDED_SUPERSET=True)
def test_valid_csrf_with_guest_credential_keeps_cookie_actor(
    browser_ingress: SimpleNamespace,
) -> None:
    response = browser_ingress.client.post(
        BROWSER_LOG,
        json={"observability_events": [browser_ingress.payload]},
        headers={
            "X-CSRFToken": browser_ingress.csrf_token,
            app.config["GUEST_TOKEN_HEADER_NAME"]: browser_ingress.guest_token,
        },
    )
    assert response.status_code == 200
    fields = browser_ingress.emitted.call_args.kwargs
    assert fields["request_actor_user_id"] == security_manager.find_user("admin").id
    assert fields["guest_actor_id"] is None
