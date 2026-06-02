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
from __future__ import annotations

from pathlib import Path
from typing import Any, Literal
from urllib.parse import urljoin

import prison
import requests
from requests import Response, Session


class SupersetApiError(RuntimeError):
    """Raised when Superset returns a non-success API response."""

    def __init__(self, message: str, response: Response | None = None) -> None:
        super().__init__(message)
        self.response = response


class SupersetApiClient:
    """Small session-aware client for Superset's current REST API."""

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        *,
        timeout: float = 30.0,
        verify: bool | str = True,
    ) -> None:
        self.base_url = base_url.rstrip("/") + "/"
        self.username = username
        self.password = password
        self.timeout = timeout
        self.verify = verify
        self.session: Session = requests.Session()
        self.csrf_token: str | None = None

    def authenticate(self) -> None:
        """Authenticate with browser-session login and fetch a CSRF token."""

        response = self.session.post(
            self.url("/login/"),
            data={
                "username": self.username,
                "password": self.password,
                "provider": "db",
            },
            allow_redirects=False,
            timeout=self.timeout,
            verify=self.verify,
        )
        if response.status_code < 200 or response.status_code >= 400:
            raise SupersetApiError(
                f"Superset login failed: {self._response_message(response)}",
                response,
            )

        csrf_response = self.get_json("/api/v1/security/csrf_token/")
        token = csrf_response.get("result")
        if not isinstance(token, str) or not token:
            raise SupersetApiError("Superset did not return a CSRF token")
        self.csrf_token = token

    def url(self, endpoint: str) -> str:
        """Return an absolute URL for a Superset endpoint."""

        return urljoin(self.base_url, endpoint.lstrip("/"))

    def request(
        self,
        method: Literal["GET", "POST", "PUT", "DELETE"],
        endpoint: str,
        *,
        json_body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
        expect_json: bool = True,
    ) -> dict[str, Any] | bytes:
        """Send a request and return decoded JSON or raw bytes."""

        headers = {"Accept": "application/json"}
        if method in {"POST", "PUT", "DELETE"} and self.csrf_token:
            headers["X-CSRFToken"] = self.csrf_token

        response = self.session.request(
            method,
            self.url(endpoint),
            json=json_body,
            params=params,
            data=data,
            files=files,
            headers=headers,
            timeout=self.timeout,
            verify=self.verify,
        )
        if response.status_code < 200 or response.status_code >= 300:
            raise SupersetApiError(
                f"{method} {endpoint} failed: {self._response_message(response)}",
                response,
            )

        if not expect_json:
            return response.content
        try:
            payload = response.json()
        except ValueError as ex:
            raise SupersetApiError(
                f"{method} {endpoint} did not return JSON",
                response,
            ) from ex
        if not isinstance(payload, dict):
            raise SupersetApiError(
                f"{method} {endpoint} returned an unexpected JSON payload",
                response,
            )
        return payload

    def get_json(
        self,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Send a GET request and return a JSON object."""

        return self.request("GET", endpoint, params=params)  # type: ignore[return-value]

    def post_json(
        self,
        endpoint: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Send a POST request and return a JSON object."""

        return self.request("POST", endpoint, json_body=payload)  # type: ignore[return-value]

    def put_json(
        self,
        endpoint: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Send a PUT request and return a JSON object."""

        return self.request("PUT", endpoint, json_body=payload)  # type: ignore[return-value]

    def find_one(
        self,
        resource: Literal["database", "dataset", "chart", "dashboard"],
        filters: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Find zero or one resource by exact-match API filters."""

        query = {
            "filters": [
                {"col": col, "opr": "eq", "value": value}
                for col, value in filters.items()
                if value is not None
            ],
            "page_size": 2,
        }
        payload = self.get_json(
            f"/api/v1/{resource}/", params={"q": prison.dumps(query)}
        )
        result = payload.get("result", {})
        if isinstance(result, dict):
            data = result.get("data", [])
        else:
            data = result
        if not isinstance(data, list):
            raise SupersetApiError(
                f"Unexpected list payload while looking up {resource}: {payload}"
            )
        if len(data) > 1:
            raise SupersetApiError(f"Multiple {resource} rows matched {filters}")
        return data[0] if data else None

    def export_dashboard(self, dashboard_id: int, output_path: Path) -> Path:
        """Export a dashboard ZIP snapshot to disk."""

        query = prison.dumps([dashboard_id])
        content = self.request(
            "GET",
            "/api/v1/dashboard/export/",
            params={"q": query},
            expect_json=False,
        )
        if not isinstance(content, bytes):
            raise SupersetApiError("Dashboard export returned an unexpected payload")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(content)
        return output_path

    def import_dashboard_zip(self, zip_path: Path, *, overwrite: bool = True) -> None:
        """Import a dashboard ZIP snapshot, optionally overwriting existing assets."""

        with zip_path.open("rb") as file_obj:
            self.request(
                "POST",
                "/api/v1/dashboard/import/",
                data={"overwrite": "true" if overwrite else "false"},
                files={"formData": file_obj},
            )

    def health(self) -> dict[str, Any] | bytes:
        """Return the Superset health endpoint response."""

        return self.request("GET", "/health", expect_json=False)

    @staticmethod
    def _response_message(response: Response) -> str:
        try:
            payload = response.json()
        except ValueError:
            return response.text
        return str(payload)
