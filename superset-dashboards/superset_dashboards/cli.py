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

import click

from superset_dashboards.client import SupersetApiClient
from superset_dashboards.deployer import DashboardDeployer
from superset_dashboards.spec import load_spec


@click.command()
@click.option(
    "--spec",
    "spec_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to a dashboard automation YAML spec.",
)
@click.option(
    "--base-url",
    required=True,
    envvar="SUPERSET_BASE_URL",
    help="Base URL for the target Superset instance.",
)
@click.option(
    "--username",
    required=True,
    envvar="SUPERSET_USERNAME",
    help="Superset service account username.",
)
@click.option(
    "--password",
    required=True,
    envvar="SUPERSET_PASSWORD",
    help="Superset service account password.",
)
@click.option(
    "--database-sqlalchemy-uri",
    envvar="SUPERSET_DB_URI",
    help="Override database.sqlalchemy_uri from the spec, keeping secrets out of git.",
)
@click.option(
    "--snapshot-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=Path(".superset-dashboard-snapshots"),
    show_default=True,
    help="Directory where pre-deploy dashboard export ZIPs are stored.",
)
@click.option(
    "--timeout",
    type=float,
    default=30.0,
    show_default=True,
    help="HTTP timeout in seconds.",
)
@click.option(
    "--dry-run", is_flag=True, help="Plan the deploy without mutating Superset."
)
@click.option(
    "--skip-upsert",
    is_flag=True,
    help="Skip all creates/updates; useful with --run-smoke-tests.",
)
@click.option(
    "--run-smoke-tests",
    is_flag=True,
    help="Check the dashboard and saved chart data endpoints after resolving assets.",
)
@click.option(
    "--no-rollback",
    is_flag=True,
    help="Do not re-import the pre-deploy snapshot if a write fails.",
)
def main(
    spec_path: Path,
    base_url: str,
    username: str,
    password: str,
    database_sqlalchemy_uri: str | None,
    snapshot_dir: Path,
    timeout: float,
    dry_run: bool,
    skip_upsert: bool,
    run_smoke_tests: bool,
    no_rollback: bool,
) -> None:
    """Deploy a version-controlled dashboard spec through Superset's REST API."""

    spec = load_spec(spec_path, database_sqlalchemy_uri=database_sqlalchemy_uri)
    client = SupersetApiClient(
        base_url,
        username,
        password,
        timeout=timeout,
    )
    client.authenticate()

    deployer = DashboardDeployer(
        client,
        dry_run=dry_run,
        skip_upsert=skip_upsert,
        run_smoke_tests=run_smoke_tests,
        snapshot_dir=snapshot_dir,
        rollback_on_failure=not no_rollback,
    )
    result = deployer.deploy(spec)

    for action in result.actions:
        click.echo(action)
    if result.snapshot_path:
        click.echo(f"Snapshot: {result.snapshot_path}")
    if result.dashboard_id is not None:
        click.echo(f"Dashboard ID: {result.dashboard_id}")


if __name__ == "__main__":
    main()
