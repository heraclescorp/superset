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

import pytest
import yaml
from superset_dashboards import json_utils as json
from superset_dashboards.layout import build_dashboard_position, ChartLayout
from superset_dashboards.spec import (
    ensure_json_string,
    load_spec,
    SpecError,
    substitute_dataset_placeholders,
)


def test_load_spec_overrides_database_uri(tmp_path: Path) -> None:
    spec_path = tmp_path / "dashboard.yaml"
    spec_path.write_text(
        yaml.safe_dump(
            {
                "database": {
                    "database_name": "analytics",
                    "sqlalchemy_uri": "postgresql://placeholder",
                },
                "dataset": {"schema": "public", "table_name": "loans"},
                "dashboard": {"title": "Loan Ops", "slug": "loan-ops"},
                "charts": [{"slice_name": "Rows", "viz_type": "table"}],
            }
        ),
        encoding="utf-8",
    )

    spec = load_spec(
        spec_path,
        database_sqlalchemy_uri="postgresql://service-account",
    )

    assert spec["database"]["sqlalchemy_uri"] == "postgresql://service-account"


def test_load_spec_requires_charts(tmp_path: Path) -> None:
    spec_path = tmp_path / "dashboard.yaml"
    spec_path.write_text(
        yaml.safe_dump(
            {
                "database": {
                    "database_name": "analytics",
                    "sqlalchemy_uri": "postgresql://placeholder",
                },
                "dataset": {"table_name": "loans"},
                "dashboard": {"title": "Loan Ops"},
                "charts": [],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SpecError, match="charts"):
        load_spec(spec_path)


def test_ensure_json_string_is_deterministic() -> None:
    assert ensure_json_string({"b": 2, "a": 1}) == '{"a":1,"b":2}'
    assert json.loads(ensure_json_string('{"already":"json"}')) == {"already": "json"}


def test_substitute_dataset_placeholders() -> None:
    value = {
        "datasource": "__DATASET_ID__",
        "queries": [{"datasource_id": "__DATASET_ID_INT__"}],
    }

    assert substitute_dataset_placeholders(value, 42) == {
        "datasource": "42__table",
        "queries": [{"datasource_id": 42}],
    }


def test_build_dashboard_position_is_deterministic() -> None:
    position = build_dashboard_position(
        [ChartLayout(chart_id=7, slice_name="Smoke - Latest Rows", uuid="abc")]
    )

    assert position["GRID_ID"]["children"] == ["ROW-1-Smoke-Latest-Rows"]
    chart = position["CHART-1-Smoke-Latest-Rows"]
    assert chart["meta"] == {
        "chartId": 7,
        "height": 50,
        "sliceName": "Smoke - Latest Rows",
        "uuid": "abc",
        "width": 12,
    }
