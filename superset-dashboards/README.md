<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Superset Dashboard Automation

This directory contains a deterministic, REST API driven dashboard deployer for
version-controlled Superset dashboard specs.

The runtime path is intentionally plain HTTP:

```text
YAML in git -> python deploy.py -> Superset REST API -> Superset instance
```

MCP or LLM tools can still help authors generate chart params or dashboard
layouts, but their output should be committed as static YAML/JSON and reviewed
before deployment.

## Spec format

Start with `dashboards/loan_ops_overview.yaml`. A spec contains:

- `database`: Superset database connection metadata.
- `dataset`: table or virtual dataset metadata.
- `dashboard`: title, slug, publishing state, metadata, and optional
  `position_json`/`position` template.
- `charts`: saved charts with `slice_name`, `viz_type`, and Explore `params`.

Use placeholders instead of environment-specific IDs:

- `__DATASET_ID__` becomes `<dataset id>__table` for chart params.
- `__DATASET_ID_INT__` becomes the numeric dataset ID.
- `__CHART_ID__` or `__CHART_ID:<slice_name>__` can be used in supplied
  dashboard layout templates.

If no dashboard layout is supplied, the deployer generates a deterministic
one-chart-per-row `position_json`.

## Required environment variables

```bash
export SUPERSET_BASE_URL="https://superset.example.com"
export SUPERSET_USERNAME="service-account"
export SUPERSET_PASSWORD="..."
export SUPERSET_DB_URI="postgresql+psycopg2://user:pass@host:5432/database"
```

`SUPERSET_DB_URI` overrides `database.sqlalchemy_uri` so secrets do not need to
be committed.

## Test sequence

From this directory:

```bash
# A) Dry run
python3 deploy.py \
  --spec dashboards/loan_ops_overview.yaml \
  --base-url "$SUPERSET_BASE_URL" \
  --username "$SUPERSET_USERNAME" \
  --password "$SUPERSET_PASSWORD" \
  --database-sqlalchemy-uri "$SUPERSET_DB_URI" \
  --dry-run

# B) Real deploy
python3 deploy.py \
  --spec dashboards/loan_ops_overview.yaml \
  --base-url "$SUPERSET_BASE_URL" \
  --username "$SUPERSET_USERNAME" \
  --password "$SUPERSET_PASSWORD" \
  --database-sqlalchemy-uri "$SUPERSET_DB_URI"

# C) Smoke checks only
python3 deploy.py \
  --spec dashboards/loan_ops_overview.yaml \
  --base-url "$SUPERSET_BASE_URL" \
  --username "$SUPERSET_USERNAME" \
  --password "$SUPERSET_PASSWORD" \
  --database-sqlalchemy-uri "$SUPERSET_DB_URI" \
  --skip-upsert \
  --run-smoke-tests
```

Run the real deploy twice to verify idempotency. Existing databases, charts, and
dashboards are looked up before writes; datasets use Superset's
`/api/v1/dataset/get_or_create/` endpoint.

## Rollback snapshots

Before mutating an existing dashboard, the deployer exports a ZIP snapshot via
`/api/v1/dashboard/export/` into `.superset-dashboard-snapshots/`. If a write
fails and rollback is enabled, it imports that ZIP back through
`/api/v1/dashboard/import/` with `overwrite=true`.

Store these ZIPs as CI artifacts for production deployments.
