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

import re
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

from superset_dashboards import json_utils as json


class SpecError(ValueError):
    """Raised when a dashboard automation spec is invalid."""


def load_spec(
    path: Path,
    *,
    database_sqlalchemy_uri: str | None = None,
) -> dict[str, Any]:
    """Load and validate a dashboard automation YAML spec."""

    with path.open(encoding="utf-8") as file_obj:
        loaded = yaml.safe_load(file_obj)
    if not isinstance(loaded, dict):
        raise SpecError(f"{path} must contain a YAML object")

    spec = deepcopy(loaded)
    database = _required_mapping(spec, "database")
    dataset = _required_mapping(spec, "dataset")
    dashboard = _required_mapping(spec, "dashboard")
    charts = spec.get("charts")

    if database_sqlalchemy_uri:
        database["sqlalchemy_uri"] = database_sqlalchemy_uri

    _require_string(database, "database_name")
    _require_string(database, "sqlalchemy_uri")
    _require_string(dataset, "table_name")
    _require_string(dashboard, "title", aliases=("dashboard_title",))

    if not isinstance(charts, list) or not charts:
        raise SpecError("charts must be a non-empty list")
    for index, chart in enumerate(charts, start=1):
        if not isinstance(chart, dict):
            raise SpecError(f"charts[{index}] must be a YAML object")
        _require_string(chart, "slice_name")
        _require_string(chart, "viz_type")

    return spec


def ensure_json_string(value: Any) -> str:
    """Return a deterministic JSON string for REST fields that expect strings."""

    if value is None:
        return "{}"
    if isinstance(value, str):
        json.loads(value)
        return value
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def pick_fields(source: dict[str, Any], allowed_fields: set[str]) -> dict[str, Any]:
    """Copy allowed fields from a spec mapping, dropping keys set to None."""

    return {
        field: value
        for field, value in source.items()
        if field in allowed_fields and value is not None
    }


def substitute_dataset_placeholders(value: Any, dataset_id: int) -> Any:
    """Replace dataset placeholders in chart params with REST API datasource IDs."""

    replacements: dict[str, Any] = {
        "__DATASET_ID__": f"{dataset_id}__table",
        "__DATASET_ID_INT__": dataset_id,
    }
    return _substitute_placeholders(value, replacements)


def substitute_chart_placeholders(value: Any, chart_ids_by_name: dict[str, int]) -> Any:
    """Replace chart ID placeholders in imported layout templates."""

    replacements = {
        f"__CHART_ID:{name}__": chart_id for name, chart_id in chart_ids_by_name.items()
    }
    if chart_ids_by_name:
        replacements["__CHART_ID__"] = next(iter(chart_ids_by_name.values()))
    return _substitute_placeholders(value, replacements)


def title_from_dashboard_spec(dashboard: dict[str, Any]) -> str:
    """Return the dashboard title using either supported spec spelling."""

    title = dashboard.get("title", dashboard.get("dashboard_title"))
    if not isinstance(title, str) or not title.strip():
        raise SpecError("dashboard.title must be a non-empty string")
    return title


def stable_suffix(value: str) -> str:
    """Create a stable, position_json-safe suffix from a user-facing label."""

    suffix = re.sub(r"[^A-Za-z0-9]+", "-", value.strip()).strip("-")
    return suffix or "item"


def _substitute_placeholders(value: Any, replacements: dict[str, Any]) -> Any:
    if isinstance(value, dict):
        return {
            key: _substitute_placeholders(item, replacements)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_substitute_placeholders(item, replacements) for item in value]
    if isinstance(value, str):
        if value in replacements:
            return replacements[value]
        substituted = value
        for placeholder, replacement in replacements.items():
            substituted = substituted.replace(placeholder, str(replacement))
        return substituted
    return value


def _required_mapping(spec: dict[str, Any], key: str) -> dict[str, Any]:
    value = spec.get(key)
    if not isinstance(value, dict):
        raise SpecError(f"{key} must be a YAML object")
    return value


def _require_string(
    mapping: dict[str, Any],
    key: str,
    *,
    aliases: tuple[str, ...] = (),
) -> None:
    value = mapping.get(key)
    for alias in aliases:
        value = value or mapping.get(alias)
    if not isinstance(value, str) or not value.strip():
        names = ", ".join((key, *aliases))
        raise SpecError(f"Missing required string field: {names}")
