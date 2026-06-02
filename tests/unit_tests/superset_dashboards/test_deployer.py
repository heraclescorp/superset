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

import pytest
from superset_dashboards import json_utils as json
from superset_dashboards.deployer import DashboardDeployer


class FakeClient:
    def __init__(
        self,
        existing: dict[tuple[str, str, Any], dict[str, Any]] | None = None,
    ) -> None:
        self.existing = existing or {}
        self.posts: list[tuple[str, dict[str, Any]]] = []
        self.puts: list[tuple[str, dict[str, Any]]] = []
        self.gets: list[str] = []
        self.exports: list[tuple[int, Path]] = []
        self.imports: list[Path] = []
        self.fail_put_endpoint: str | None = None

    def find_one(
        self,
        resource: Literal["database", "dataset", "chart", "dashboard"],
        filters: dict[str, Any],
    ) -> dict[str, Any] | None:
        [(field, value)] = list(filters.items())
        return self.existing.get((resource, field, value))

    def post_json(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.posts.append((endpoint, payload))
        if endpoint == "/api/v1/database/":
            return {"result": {"id": 10}}
        if endpoint == "/api/v1/dataset/get_or_create/":
            return {"result": {"table_id": 20}}
        if endpoint == "/api/v1/dashboard/":
            return {"result": {"id": 30}}
        if endpoint == "/api/v1/chart/":
            return {"result": {"id": 40, "uuid": "chart-uuid"}}
        return {"result": {"id": 999}}

    def put_json(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        if endpoint == self.fail_put_endpoint:
            raise RuntimeError("write failed")
        self.puts.append((endpoint, payload))
        return {"result": {"id": int(endpoint.rstrip("/").rsplit("/", 1)[-1])}}

    def get_json(
        self,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.gets.append(endpoint)
        return {"result": {"id": 1}}

    def export_dashboard(self, dashboard_id: int, output_path: Path) -> Path:
        self.exports.append((dashboard_id, output_path))
        return output_path

    def import_dashboard_zip(self, zip_path: Path, *, overwrite: bool = True) -> None:
        self.imports.append(zip_path)


def minimal_spec() -> dict[str, Any]:
    return {
        "database": {
            "database_name": "analytics",
            "sqlalchemy_uri": "postgresql://service-account",
            "expose_in_sqllab": True,
        },
        "dataset": {"schema": "public", "table_name": "loan_applications"},
        "dashboard": {
            "title": "Staging - Automation Smoke Test",
            "slug": "staging-automation-smoke-test",
            "published": False,
        },
        "charts": [
            {
                "slice_name": "Smoke - Latest Rows",
                "viz_type": "table",
                "params": {"datasource": "__DATASET_ID__", "row_limit": 50},
            }
        ],
    }


def test_dry_run_records_actions_without_writes() -> None:
    client = FakeClient()
    deployer = DashboardDeployer(client, dry_run=True)

    result = deployer.deploy(minimal_spec())

    assert result.dashboard_id == 0
    assert client.posts == []
    assert client.puts == []
    assert "Creating database analytics" in result.actions
    assert "Creating chart Smoke - Latest Rows" in result.actions


def test_deploy_builds_current_superset_rest_payloads() -> None:
    client = FakeClient()
    deployer = DashboardDeployer(client, snapshot_dir=Path("snapshots"))

    result = deployer.deploy(minimal_spec())

    assert result.database_id == 10
    assert result.dataset_id == 20
    assert result.dashboard_id == 30
    assert result.chart_ids == {"Smoke - Latest Rows": 40}
    chart_payload = dict(client.posts)["/api/v1/chart/"]
    assert chart_payload["datasource_id"] == 20
    assert chart_payload["datasource_type"] == "table"
    assert chart_payload["dashboards"] == [30]
    assert json.loads(chart_payload["params"]) == {
        "datasource": "20__table",
        "row_limit": 50,
    }

    dashboard_layout_payload = client.puts[-1][1]
    position = json.loads(dashboard_layout_payload["position_json"])
    assert position["CHART-1-Smoke-Latest-Rows"]["meta"]["chartId"] == 40
    assert dashboard_layout_payload["is_managed_externally"] is True


def test_existing_dashboard_is_snapshot_and_rolled_back_on_failure() -> None:
    existing_dashboard = {
        ("dashboard", "slug", "staging-automation-smoke-test"): {
            "id": 30,
            "slug": "staging-automation-smoke-test",
            "dashboard_title": "Staging - Automation Smoke Test",
        },
        ("database", "database_name", "analytics"): {
            "id": 10,
            "database_name": "analytics",
        },
    }
    client = FakeClient(existing_dashboard)
    client.fail_put_endpoint = "/api/v1/database/10"
    deployer = DashboardDeployer(client, snapshot_dir=Path("snapshots"))

    with pytest.raises(RuntimeError, match="write failed"):
        deployer.deploy(minimal_spec())

    assert client.exports[0][0] == 30
    assert client.imports == [client.exports[0][1]]


def test_skip_upsert_runs_smoke_tests_against_existing_assets() -> None:
    existing = {
        ("dashboard", "slug", "staging-automation-smoke-test"): {"id": 30},
        ("database", "database_name", "analytics"): {"id": 10},
        ("dataset", "table_name", "loan_applications"): {"id": 20},
        ("chart", "slice_name", "Smoke - Latest Rows"): {"id": 40},
    }
    client = FakeClient(existing)
    deployer = DashboardDeployer(client, skip_upsert=True, run_smoke_tests=True)

    result = deployer.deploy(minimal_spec())

    assert result.dashboard_id == 30
    assert result.chart_ids == {"Smoke - Latest Rows": 40}
    assert client.posts == []
    assert client.puts == []
    assert client.gets == ["/api/v1/dashboard/30", "/api/v1/chart/40/data/"]
