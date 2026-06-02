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

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Protocol

from superset_dashboards.layout import build_dashboard_position, ChartLayout
from superset_dashboards.spec import (
    ensure_json_string,
    pick_fields,
    stable_suffix,
    substitute_chart_placeholders,
    substitute_dataset_placeholders,
    title_from_dashboard_spec,
)


class SupersetClientProtocol(Protocol):
    """Protocol implemented by the REST client and tests' fake clients."""

    def find_one(
        self,
        resource: Literal["database", "dataset", "chart", "dashboard"],
        filters: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Find a single resource."""

    def post_json(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        """POST JSON to Superset."""

    def put_json(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        """PUT JSON to Superset."""

    def get_json(
        self, endpoint: str, *, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """GET JSON from Superset."""

    def export_dashboard(self, dashboard_id: int, output_path: Path) -> Path:
        """Export a dashboard snapshot."""

    def import_dashboard_zip(self, zip_path: Path, *, overwrite: bool = True) -> None:
        """Import a dashboard snapshot."""


@dataclass
class DeploymentResult:
    """Summary of a dashboard automation deployment."""

    actions: list[str] = field(default_factory=list)
    database_id: int | None = None
    dataset_id: int | None = None
    dashboard_id: int | None = None
    chart_ids: dict[str, int] = field(default_factory=dict)
    snapshot_path: Path | None = None


class DashboardDeployer:
    """Deploy a YAML dashboard spec through Superset's REST API."""

    DATABASE_FIELDS = {
        "database_name",
        "sqlalchemy_uri",
        "configuration_method",
        "cache_timeout",
        "expose_in_sqllab",
        "allow_run_async",
        "allow_file_upload",
        "allow_ctas",
        "allow_cvas",
        "allow_dml",
        "extra",
        "uuid",
        "is_managed_externally",
        "external_url",
    }
    DATASET_FIELDS = {
        "table_name",
        "catalog",
        "schema",
        "sql",
        "owners",
        "normalize_columns",
        "always_filter_main_dttm",
        "template_params",
        "uuid",
        "is_managed_externally",
        "external_url",
    }
    CHART_FIELDS = {
        "slice_name",
        "description",
        "viz_type",
        "owners",
        "params",
        "query_context",
        "cache_timeout",
        "certified_by",
        "certification_details",
        "is_managed_externally",
        "external_url",
        "uuid",
    }
    DASHBOARD_FIELDS = {
        "slug",
        "owners",
        "roles",
        "css",
        "theme_id",
        "published",
        "certified_by",
        "certification_details",
        "is_managed_externally",
        "external_url",
        "uuid",
    }

    def __init__(
        self,
        client: SupersetClientProtocol,
        *,
        dry_run: bool = False,
        skip_upsert: bool = False,
        run_smoke_tests: bool = False,
        snapshot_dir: Path | None = None,
        rollback_on_failure: bool = True,
    ) -> None:
        self.client = client
        self.dry_run = dry_run
        self.skip_upsert = skip_upsert
        self.run_smoke_tests = run_smoke_tests
        self.snapshot_dir = snapshot_dir
        self.rollback_on_failure = rollback_on_failure
        self.result = DeploymentResult()

    def deploy(self, spec: dict[str, Any]) -> DeploymentResult:
        """Deploy or validate a dashboard automation spec."""

        existing_dashboard = self._find_dashboard(spec["dashboard"])
        if self.skip_upsert:
            self._record("Skipping upsert phase")
            self._resolve_existing_assets(spec, existing_dashboard)
            if self.run_smoke_tests:
                self._run_smoke_tests()
            return self.result

        self._snapshot_existing_dashboard(existing_dashboard)
        try:
            database_id = self._upsert_database(spec["database"])
            dataset_id = self._upsert_dataset(spec["dataset"], database_id)
            dashboard_id = self._upsert_dashboard_shell(
                spec["dashboard"], existing_dashboard
            )
            chart_layouts = self._upsert_charts(
                spec["charts"], dataset_id, dashboard_id
            )
            self._update_dashboard_position(
                spec["dashboard"], dashboard_id, chart_layouts
            )

            if self.run_smoke_tests:
                self._run_smoke_tests()
        except Exception:
            self._rollback()
            raise

        return self.result

    def _snapshot_existing_dashboard(
        self,
        existing_dashboard: dict[str, Any] | None,
    ) -> None:
        if not existing_dashboard:
            self._record("No existing dashboard snapshot needed")
            return
        dashboard_id = int(existing_dashboard["id"])
        if self.dry_run:
            self._record(f"Would export dashboard {dashboard_id} before writes")
            return
        if not self.snapshot_dir:
            self._record("No snapshot directory configured; skipping snapshot export")
            return

        timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        slug = existing_dashboard.get("slug") or existing_dashboard.get(
            "dashboard_title"
        )
        safe_name = stable_suffix(str(slug or dashboard_id))
        output_path = self.snapshot_dir / f"{safe_name}-{timestamp}.zip"
        self.result.snapshot_path = self.client.export_dashboard(
            dashboard_id, output_path
        )
        self._record(f"Exported pre-deploy dashboard snapshot to {output_path}")

    def _upsert_database(self, database: dict[str, Any]) -> int:
        payload = pick_fields(database, self.DATABASE_FIELDS)
        payload.setdefault("configuration_method", "sqlalchemy_form")
        existing = self._find_by_uuid_or_name(
            "database",
            database,
            "database_name",
        )
        if existing:
            database_id = int(existing["id"])
            self._record(
                f"Updating database {database['database_name']} ({database_id})"
            )
            if not self.dry_run:
                self.client.put_json(f"/api/v1/database/{database_id}", payload)
            self.result.database_id = database_id
            return database_id

        self._record(f"Creating database {database['database_name']}")
        if self.dry_run:
            self.result.database_id = 0
            return 0
        response = self.client.post_json("/api/v1/database/", payload)
        database_id = self._extract_id(response)
        self.result.database_id = database_id
        return database_id

    def _upsert_dataset(self, dataset: dict[str, Any], database_id: int) -> int:
        payload = pick_fields(dataset, self.DATASET_FIELDS)
        payload["database_id"] = database_id

        lookup_payload = {
            "database_id": database_id,
            "table_name": dataset["table_name"],
            "catalog": dataset.get("catalog"),
            "schema": dataset.get("schema"),
        }
        self._record(f"Resolving dataset {dataset['table_name']}")
        if self.dry_run:
            self.result.dataset_id = 0
            return 0

        response = self.client.post_json(
            "/api/v1/dataset/get_or_create/",
            {key: value for key, value in lookup_payload.items() if value is not None},
        )
        dataset_id = int(
            response.get("result", {}).get("table_id") or self._extract_id(response)
        )
        self.client.put_json(f"/api/v1/dataset/{dataset_id}", payload)
        self.result.dataset_id = dataset_id
        return dataset_id

    def _upsert_dashboard_shell(
        self,
        dashboard: dict[str, Any],
        existing_dashboard: dict[str, Any] | None,
    ) -> int:
        payload = self._dashboard_payload(dashboard)
        payload.setdefault("position_json", "{}")
        if existing_dashboard:
            dashboard_id = int(existing_dashboard["id"])
            self._record(
                f"Updating dashboard shell {title_from_dashboard_spec(dashboard)}"
            )
            if not self.dry_run:
                self.client.put_json(f"/api/v1/dashboard/{dashboard_id}", payload)
            self.result.dashboard_id = dashboard_id
            return dashboard_id

        self._record(f"Creating dashboard shell {title_from_dashboard_spec(dashboard)}")
        if self.dry_run:
            self.result.dashboard_id = 0
            return 0
        response = self.client.post_json("/api/v1/dashboard/", payload)
        dashboard_id = self._extract_id(response)
        self.result.dashboard_id = dashboard_id
        return dashboard_id

    def _upsert_charts(
        self,
        charts: list[dict[str, Any]],
        dataset_id: int,
        dashboard_id: int,
    ) -> list[ChartLayout]:
        chart_layouts: list[ChartLayout] = []
        for chart in charts:
            payload = pick_fields(chart, self.CHART_FIELDS)
            payload["datasource_id"] = dataset_id
            payload["datasource_type"] = "table"
            payload["dashboards"] = [dashboard_id]
            payload["params"] = ensure_json_string(
                substitute_dataset_placeholders(chart.get("params", {}), dataset_id)
            )
            if chart.get("query_context") is not None:
                payload["query_context"] = ensure_json_string(chart["query_context"])

            existing = self._find_by_uuid_or_name("chart", chart, "slice_name")
            if existing:
                chart_id = int(existing["id"])
                chart_uuid = existing.get("uuid")
                self._record(f"Updating chart {chart['slice_name']} ({chart_id})")
                if not self.dry_run:
                    self.client.put_json(f"/api/v1/chart/{chart_id}", payload)
            else:
                self._record(f"Creating chart {chart['slice_name']}")
                if self.dry_run:
                    chart_id = 0
                    chart_uuid = chart.get("uuid")
                else:
                    response = self.client.post_json("/api/v1/chart/", payload)
                    result = response.get("result", {})
                    chart_id = self._extract_id(response)
                    chart_uuid = (
                        result.get("uuid") if isinstance(result, dict) else None
                    )

            self.result.chart_ids[chart["slice_name"]] = chart_id
            chart_layouts.append(
                ChartLayout(
                    chart_id=chart_id,
                    slice_name=chart["slice_name"],
                    uuid=str(chart_uuid) if chart_uuid else None,
                    width=int(chart.get("width", 12)),
                    height=int(chart.get("height", 50)),
                )
            )
        return chart_layouts

    def _update_dashboard_position(
        self,
        dashboard: dict[str, Any],
        dashboard_id: int,
        chart_layouts: list[ChartLayout],
    ) -> None:
        chart_ids_by_name = {
            chart.slice_name: chart.chart_id for chart in chart_layouts
        }
        supplied_position = dashboard.get("position_json", dashboard.get("position"))
        position = supplied_position or build_dashboard_position(chart_layouts)
        position = substitute_chart_placeholders(position, chart_ids_by_name)
        payload = self._dashboard_payload(dashboard, position=position)
        self._record(f"Updating dashboard layout for dashboard {dashboard_id}")
        if not self.dry_run:
            self.client.put_json(f"/api/v1/dashboard/{dashboard_id}", payload)

    def _dashboard_payload(
        self,
        dashboard: dict[str, Any],
        *,
        position: Any | None = None,
    ) -> dict[str, Any]:
        payload = pick_fields(dashboard, self.DASHBOARD_FIELDS)
        payload["dashboard_title"] = title_from_dashboard_spec(dashboard)
        payload.setdefault("is_managed_externally", True)
        metadata = dashboard.get("json_metadata", dashboard.get("metadata", {}))
        payload["json_metadata"] = ensure_json_string(metadata)
        if position is not None:
            payload["position_json"] = ensure_json_string(position)
        return payload

    def _resolve_existing_assets(
        self,
        spec: dict[str, Any],
        existing_dashboard: dict[str, Any] | None,
    ) -> None:
        database = self._find_by_uuid_or_name(
            "database", spec["database"], "database_name"
        )
        dataset = self._find_by_uuid_or_name("dataset", spec["dataset"], "table_name")
        if existing_dashboard:
            self.result.dashboard_id = int(existing_dashboard["id"])
        if database:
            self.result.database_id = int(database["id"])
        if dataset:
            self.result.dataset_id = int(dataset["id"])
        for chart in spec["charts"]:
            existing_chart = self._find_by_uuid_or_name("chart", chart, "slice_name")
            if existing_chart:
                self.result.chart_ids[chart["slice_name"]] = int(existing_chart["id"])

    def _run_smoke_tests(self) -> None:
        if self.result.dashboard_id is None:
            raise RuntimeError("Cannot smoke-test without a dashboard ID")

        self._record(f"Smoke-testing dashboard {self.result.dashboard_id}")
        self.client.get_json(f"/api/v1/dashboard/{self.result.dashboard_id}")
        for slice_name, chart_id in self.result.chart_ids.items():
            self._record(f"Smoke-testing chart {slice_name} ({chart_id})")
            self.client.get_json(f"/api/v1/chart/{chart_id}/data/")

    def _rollback(self) -> None:
        if not self.rollback_on_failure or not self.result.snapshot_path:
            return
        self._record(f"Rolling back from snapshot {self.result.snapshot_path}")
        self.client.import_dashboard_zip(self.result.snapshot_path, overwrite=True)

    def _find_dashboard(self, dashboard: dict[str, Any]) -> dict[str, Any] | None:
        if dashboard.get("uuid"):
            existing = self.client.find_one("dashboard", {"uuid": dashboard["uuid"]})
            if existing:
                return existing
        if dashboard.get("slug"):
            existing = self.client.find_one("dashboard", {"slug": dashboard["slug"]})
            if existing:
                return existing
        return self.client.find_one(
            "dashboard",
            {"dashboard_title": title_from_dashboard_spec(dashboard)},
        )

    def _find_by_uuid_or_name(
        self,
        resource: Literal["database", "dataset", "chart"],
        spec: dict[str, Any],
        name_field: str,
    ) -> dict[str, Any] | None:
        if spec.get("uuid"):
            existing = self.client.find_one(resource, {"uuid": spec["uuid"]})
            if existing:
                return existing
        return self.client.find_one(resource, {name_field: spec[name_field]})

    def _record(self, message: str) -> None:
        self.result.actions.append(message)

    @staticmethod
    def _extract_id(response: dict[str, Any]) -> int:
        result = response.get("result", response)
        if isinstance(result, dict):
            value = result.get("id")
            if value is None:
                value = result.get("table_id")
        else:
            value = None
        if value is None:
            raise RuntimeError(f"Superset response did not include an id: {response}")
        return int(value)
