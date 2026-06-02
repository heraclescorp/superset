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

from dataclasses import dataclass
from typing import Any

from superset_dashboards.spec import stable_suffix


@dataclass(frozen=True)
class ChartLayout:
    """Chart placement inputs for generated dashboard position_json."""

    chart_id: int
    slice_name: str
    uuid: str | None = None
    width: int = 12
    height: int = 50


def build_dashboard_position(charts: list[ChartLayout]) -> dict[str, Any]:
    """Build a deterministic one-chart-per-row dashboard layout."""

    position: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "parents": ["ROOT_ID"], "children": []},
    }

    row_ids: list[str] = []
    for index, chart in enumerate(charts, start=1):
        suffix = stable_suffix(chart.slice_name)
        row_id = f"ROW-{index}-{suffix}"
        chart_node_id = f"CHART-{index}-{suffix}"
        row_ids.append(row_id)

        position[row_id] = {
            "type": "ROW",
            "id": row_id,
            "parents": ["ROOT_ID", "GRID_ID"],
            "children": [chart_node_id],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        chart_meta: dict[str, Any] = {
            "chartId": chart.chart_id,
            "height": chart.height,
            "sliceName": chart.slice_name,
            "width": chart.width,
        }
        if chart.uuid:
            chart_meta["uuid"] = chart.uuid
        position[chart_node_id] = {
            "type": "CHART",
            "id": chart_node_id,
            "parents": ["ROOT_ID", "GRID_ID", row_id],
            "children": [],
            "meta": chart_meta,
        }

    position["GRID_ID"]["children"] = row_ids
    return position
