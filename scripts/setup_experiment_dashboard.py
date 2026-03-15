#!/usr/bin/env python
"""
Setup script for the Experiment Dashboard.

Creates the virtual datasets and dashboards needed for the experiment
dashboard to function. Run once after database init:

    source venv/bin/activate
    python scripts/setup_experiment_dashboard.py

Requires:
  - Superset DB initialized (superset db upgrade && superset init)
  - A Snowflake database connection registered in Superset
"""
import json
import sys

from superset.app import create_app

app = create_app()

# Cutoff date: only show experiments starting after the ABTestOverride data fix.
# Adjust this date if needed.
EXPOSURE_CUTOFF = "2025-01-01"

EXPOSURES_SQL = f"""\
WITH exp_spec AS (
  SELECT id AS experiment_spec_id, start_time, end_time
  FROM postgres_rds_wal_public.experiment_spec
  WHERE start_time >= '{EXPOSURE_CUTOFF}'::DATE
)
SELECT
  e.EXPERIMENT_SPEC_ID,
  e.EXPERIMENT_GROUP,
  e.LOAN_APPLICATION_ID,
  dla.PHONE_NUMBER,
  exp_spec.START_TIME,
  exp_spec.END_TIME,
  dlae.ORIGINATION_STARTED,
  dlae.CLICK_BUTTON_SUBMIT_STATED_INCOME_FORM,
  dlae.VIEW_PRE_QUALIFICATION,
  dlae.EVENT_PRE_QUAL_OFFERED,
  dlae.CLICK_BUTTON_PRE_QUALIFICATION_CONTINUE,
  dlae.CLICK_BUTTON_SUBMIT_CHOSEN_PREQUAL_OFFER,
  dlae.VIEW_OFFER_PREVIEW,
  dlae.CLICK_BUTTON_ACCEPT_OFFER,
  dlae.EVENT_SCHEDULED_NOTARY_TIMESLOT,
  dlae.EVENT_SCHEDULED_IMMEDIATE_NOTARY_TIMESLOT,
  dlae.VIEW_APPLICANT_NOTARY_SESSION,
  dlae.PRIMARY_CREDIT_CARD_CREATED,
  dla.CORE_CARD_ACCOUNT_NUMBER,
  dla.INCOME_SUCCESSFUL_VERIFICATION_DATE,
  fu.FICO_SCORE_PRIMARY,
  fu.DTI
FROM etl_auto_gen_tables.dim_loan_app_ab_experiment_exposures_v2 e
JOIN exp_spec USING (experiment_spec_id)
LEFT JOIN etl_auto_gen_tables.dim_loan_app_event dlae ON e.LOAN_APPLICATION_ID = dlae.LOAN_APPLICATION_ID
LEFT JOIN etl_auto_gen_tables.dim_loan_app dla ON e.LOAN_APPLICATION_ID = dla.LOAN_APPLICATION_ID
LEFT JOIN etl_auto_gen_tables.fact_underwriting fu ON e.LOAN_APPLICATION_ID = fu.LOAN_APPLICATION_ID AND fu.IS_SELECTED_POLICY = TRUE
"""

EXPERIMENT_LIST_SQL = f"""\
SELECT
  es.ID,
  es.NAME,
  es.TYPE,
  es.HASH_TYPE,
  es.START_TIME,
  es.END_TIME,
  es.GROUPS,
  COALESCE(ex.EXPOSED_CNT, 0) AS EXPOSED_COUNT,
  CASE
    WHEN es.END_TIME IS NOT NULL AND es.END_TIME < CURRENT_TIMESTAMP() THEN 'completed'
    WHEN es.START_TIME IS NOT NULL AND es.START_TIME <= CURRENT_TIMESTAMP() THEN 'running'
    ELSE 'draft'
  END AS STATUS,
  ec.CHART_URL
FROM FIVETRAN_DATABASE.POSTGRES_RDS_WAL_PUBLIC.EXPERIMENT_SPEC es
INNER JOIN (
  SELECT EXPERIMENT_SPEC_ID, COUNT(*) AS EXPOSED_CNT
  FROM FIVETRAN_DATABASE.ETL_AUTO_GEN_TABLES.DIM_LOAN_APP_AB_EXPERIMENT_EXPOSURES_V2
  GROUP BY 1
) ex ON es.ID = ex.EXPERIMENT_SPEC_ID
LEFT JOIN (
  SELECT EXPERIMENT_SPEC_ID, CHART_URL
  FROM FIVETRAN_DATABASE.ETL_AUTO_GEN_TABLES.EXPERIMENT_CHARTS_V2
) ec ON es.ID = ec.EXPERIMENT_SPEC_ID
WHERE es.START_TIME IS NOT NULL
  AND es.START_TIME >= '{EXPOSURE_CUTOFF}'::DATE
  AND es.START_TIME <= CURRENT_TIMESTAMP()
ORDER BY es.START_TIME DESC
"""


def main():
    with app.app_context():
        from superset.connectors.sqla.models import SqlaTable
        from superset.extensions import db
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        # ── Find Snowflake database ──────────────────────────────────────
        from superset.models.core import Database

        sf_db = (
            db.session.query(Database)
            .filter(Database.sqlalchemy_uri.like("snowflake://%"))
            .first()
        )
        if not sf_db:
            print("ERROR: No Snowflake database found in Superset.")
            print("Add one via Admin → Databases first.")
            sys.exit(1)
        print(f"Using Snowflake database: {sf_db.database_name} (id={sf_db.id})")

        # ── Create virtual datasets ─────────────────────────────────────
        def get_or_create_dataset(name, sql, schema="FIVETRAN_DATABASE"):
            existing = (
                db.session.query(SqlaTable)
                .filter_by(table_name=name, database_id=sf_db.id)
                .first()
            )
            if existing:
                existing.sql = sql
                print(f"  Dataset '{name}' already exists (id={existing.id}), SQL updated")
                return existing
            ds = SqlaTable(
                table_name=name,
                database_id=sf_db.id,
                schema=schema,
                sql=sql,
            )
            db.session.add(ds)
            db.session.flush()
            print(f"  Created dataset '{name}' (id={ds.id})")
            return ds

        print("\nCreating virtual datasets...")
        ds_list = get_or_create_dataset(
            "experiment_list_v2", EXPERIMENT_LIST_SQL,
        )
        ds_exposures = get_or_create_dataset(
            "experiment_exposures_v2", EXPOSURES_SQL,
        )

        # Known columns for each dataset (avoids Snowflake SSO for column discovery)
        KNOWN_COLUMNS = {
            "experiment_list_v2": [
                "ID", "NAME", "TYPE", "HASH_TYPE", "START_TIME",
                "END_TIME", "GROUPS", "EXPOSED_COUNT", "STATUS", "CHART_URL",
            ],
            "experiment_exposures_v2": [
                "EXPERIMENT_SPEC_ID", "EXPERIMENT_GROUP", "LOAN_APPLICATION_ID",
                "PHONE_NUMBER", "START_TIME", "END_TIME",
                "ORIGINATION_STARTED", "CLICK_BUTTON_SUBMIT_STATED_INCOME_FORM",
                "VIEW_PRE_QUALIFICATION", "EVENT_PRE_QUAL_OFFERED",
                "CLICK_BUTTON_PRE_QUALIFICATION_CONTINUE",
                "CLICK_BUTTON_SUBMIT_CHOSEN_PREQUAL_OFFER",
                "VIEW_OFFER_PREVIEW", "CLICK_BUTTON_ACCEPT_OFFER",
                "EVENT_SCHEDULED_NOTARY_TIMESLOT", "EVENT_SCHEDULED_IMMEDIATE_NOTARY_TIMESLOT",
                "VIEW_APPLICANT_NOTARY_SESSION",
                "PRIMARY_CREDIT_CARD_CREATED", "CORE_CARD_ACCOUNT_NUMBER",
                "INCOME_SUCCESSFUL_VERIFICATION_DATE",
                "FICO_SCORE_PRIMARY", "DTI",
            ],
        }

        # Use existing DB columns or hardcoded fallback (no Snowflake calls)
        print("\nResolving dataset columns...")
        for ds in [ds_list, ds_exposures]:
            if ds.columns:
                print(f"  {ds.table_name}: {len(ds.columns)} columns from DB")
            elif ds.table_name in KNOWN_COLUMNS:
                from superset.connectors.sqla.models import TableColumn
                for col_name in KNOWN_COLUMNS[ds.table_name]:
                    ds.columns.append(TableColumn(
                        column_name=col_name,
                        type="VARCHAR",
                        table=ds,
                    ))
                print(f"  {ds.table_name}: {len(KNOWN_COLUMNS[ds.table_name])} columns from fallback")
            else:
                print(f"  WARNING: No columns for {ds.table_name}. Configure in Explore view.")

        def get_columns_for(ds):
            """Get column names for a dataset."""
            if ds.columns:
                return [c.column_name for c in ds.columns]
            return KNOWN_COLUMNS.get(ds.table_name, [])

        # ── Add saved metrics to exposure dataset ────────────────────────
        # Metric definitions match the production Prefect pipeline in
        # heracles/aven_python/jobs/jobs/etl/flows/supersetExperimentChartGeneration.py
        # Metrics use bare column names (no table prefixes) since they query the virtual dataset.
        print("\nConfiguring saved metrics...")
        from superset.connectors.sqla.models import SqlMetric

        FUNNEL_EVENTS = {
            "LEAD": "ORIGINATION_STARTED",
            "PII": "CLICK_BUTTON_SUBMIT_STATED_INCOME_FORM",
            "PQ": "COALESCE(VIEW_PRE_QUALIFICATION, EVENT_PRE_QUAL_OFFERED)",
            "PQA": "COALESCE(CLICK_BUTTON_PRE_QUALIFICATION_CONTINUE, CLICK_BUTTON_SUBMIT_CHOSEN_PREQUAL_OFFER)",
            "INCOME_VERIFIED": "INCOME_SUCCESSFUL_VERIFICATION_DATE",
            "OFFER": "VIEW_OFFER_PREVIEW",
            "ACCEPT": "CLICK_BUTTON_ACCEPT_OFFER",
            "SCHED": "COALESCE(EVENT_SCHEDULED_NOTARY_TIMESLOT, EVENT_SCHEDULED_IMMEDIATE_NOTARY_TIMESLOT)",
            "NOTARYSTARTED": "VIEW_APPLICANT_NOTARY_SESSION",
            "BOOKED": "COALESCE(PRIMARY_CREDIT_CARD_CREATED, CORE_CARD_ACCOUNT_NUMBER)",
        }

        def count_metric(event_sql):
            return f"COUNT(DISTINCT CASE WHEN {event_sql} IS NOT NULL THEN PHONE_NUMBER END)"

        def ratio_metric(num_key, den_key):
            return f"DIV0({count_metric(FUNNEL_EVENTS[num_key])}, {count_metric(FUNNEL_EVENTS[den_key])})"

        def avg_metric(event_sql, measure):
            return f"AVG(CASE WHEN {event_sql} IS NOT NULL THEN {measure} END)"

        SAVED_METRICS = {
            # Count metrics (COUNT DISTINCT phone_number, matching prod)
            **{f"UNQ_{k}_#": count_metric(v) for k, v in FUNNEL_EVENTS.items()},
            # Ratio metrics (stage-over-stage conversion rates)
            "UNQ_PII/LEAD_%": ratio_metric("PII", "LEAD"),
            "UNQ_PQ/PII_%": ratio_metric("PQ", "PII"),
            "UNQ_PQA/PQ_%": ratio_metric("PQA", "PQ"),
            "UNQ_INCOME/PQA_%": ratio_metric("INCOME_VERIFIED", "PQA"),
            "UNQ_OFFER/INCOME_%": ratio_metric("OFFER", "INCOME_VERIFIED"),
            "UNQ_ACCEPT/OFFER_%": ratio_metric("ACCEPT", "OFFER"),
            "UNQ_SCHED/ACCEPT_%": ratio_metric("SCHED", "ACCEPT"),
            "UNQ_NOTARYSTARTED/SCHED_%": ratio_metric("NOTARYSTARTED", "SCHED"),
            "UNQ_BOOKED/NOTARYSTARTED_%": ratio_metric("BOOKED", "NOTARYSTARTED"),
            "UNQ_SCHED/PQ_%": ratio_metric("SCHED", "PQ"),
            "UNQ_BOOKED/SCHED_%": ratio_metric("BOOKED", "SCHED"),
            # FICO avg metrics
            "Avg FICO_PII": avg_metric(FUNNEL_EVENTS["PII"], "IFF(FICO_SCORE_PRIMARY BETWEEN 300 AND 850, FICO_SCORE_PRIMARY, NULL)"),
            "Avg FICO_PQ": avg_metric(FUNNEL_EVENTS["PQ"], "FICO_SCORE_PRIMARY"),
            "Avg FICO_SCHED": avg_metric(FUNNEL_EVENTS["SCHED"], "FICO_SCORE_PRIMARY"),
            "Avg FICO_BOOKED": avg_metric(FUNNEL_EVENTS["BOOKED"], "FICO_SCORE_PRIMARY"),
            # DTI avg metrics
            "Avg PrelineDTI_PII": avg_metric(FUNNEL_EVENTS["PII"], "DTI"),
            "Avg PrelineDTI_PQ": avg_metric(FUNNEL_EVENTS["PQ"], "DTI"),
            "Avg PrelineDTI_SCHED": avg_metric(FUNNEL_EVENTS["SCHED"], "DTI"),
            "Avg PrelineDTI_BOOKED": avg_metric(FUNNEL_EVENTS["BOOKED"], "DTI"),
        }

        existing_metrics = {m.metric_name for m in ds_exposures.metrics}
        for name, expression in SAVED_METRICS.items():
            if name not in existing_metrics:
                ds_exposures.metrics.append(SqlMetric(
                    metric_name=name,
                    expression=expression,
                    metric_type="count_distinct" if name.endswith("_#") else "expression",
                    table=ds_exposures,
                ))
            else:
                # Update expression if metric already exists
                for m in ds_exposures.metrics:
                    if m.metric_name == name:
                        m.expression = expression
                        break
        print(f"  {ds_exposures.table_name}: {len(SAVED_METRICS)} saved metrics configured")

        # ── Create experiment list chart ─────────────────────────────────
        print("\nCreating charts...")

        def get_or_create_chart(name, viz_type, datasource_id, params=None):
            existing = (
                db.session.query(Slice)
                .filter_by(slice_name=name)
                .first()
            )
            if existing:
                # Update params if they changed
                existing.params = json.dumps(params or {})
                print(f"  Chart '{name}' already exists (id={existing.id}), params updated")
                return existing
            chart = Slice(
                slice_name=name,
                viz_type=viz_type,
                datasource_type="table",
                datasource_id=datasource_id,
                params=json.dumps(params or {}),
            )
            db.session.add(chart)
            db.session.flush()
            print(f"  Created chart '{name}' (id={chart.id})")
            return chart

        list_chart = get_or_create_chart(
            "Experiment List V2",
            "experiment_ab",
            ds_list.id,
            {
                "datasource": f"{ds_list.id}__table",
                "viz_type": "experiment_ab",
                "all_columns": [
                    "ID", "NAME", "TYPE", "HASH_TYPE", "START_TIME",
                    "END_TIME", "GROUPS", "EXPOSED_COUNT", "STATUS", "CHART_URL",
                ],
                "row_limit": 200,
            },
        )

        # ── Create dashboards ────────────────────────────────────────────
        print("\nCreating dashboards...")

        def get_or_create_dashboard(title, slug, charts, metadata=None):
            existing = (
                db.session.query(Dashboard)
                .filter_by(slug=slug)
                .first()
            )
            if existing:
                if metadata:
                    existing.json_metadata = json.dumps(metadata)
                print(f"  Dashboard '{title}' already exists (id={existing.id}), metadata updated")
                return existing
            position = {}
            for i, chart in enumerate(charts):
                position[f"CHART-{chart.id}"] = {
                    "type": "CHART",
                    "id": f"CHART-{chart.id}",
                    "children": [],
                    "meta": {
                        "width": 12,
                        "height": 80,
                        "chartId": chart.id,
                        "sliceName": chart.slice_name,
                    },
                }
            # Dashboard layout grid
            position["ROOT_ID"] = {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]}
            position["GRID_ID"] = {
                "type": "GRID",
                "id": "GRID_ID",
                "children": [f"ROW-{i}" for i in range(len(charts))],
            }
            for i, chart in enumerate(charts):
                position[f"ROW-{i}"] = {
                    "type": "ROW",
                    "id": f"ROW-{i}",
                    "children": [f"CHART-{chart.id}"],
                    "meta": {"background": "BACKGROUND_TRANSPARENT"},
                }
            position["HEADER_ID"] = {
                "type": "HEADER",
                "id": "HEADER_ID",
                "meta": {"text": title},
            }
            position["DASHBOARD_VERSION_KEY"] = "v2"

            dash = Dashboard(
                dashboard_title=title,
                slug=slug,
                position_json=json.dumps(position),
                json_metadata=json.dumps(metadata or {}),
            )
            dash.slices = list(charts)
            db.session.add(dash)
            db.session.flush()
            print(f"  Created dashboard '{title}' (id={dash.id}, slug={slug})")
            return dash

        list_dash = get_or_create_dashboard(
            "Experiments V2",
            "experiments",
            [list_chart],
        )

        db.session.commit()

        # ── Generate per-experiment charts ───────────────────────────────
        # Query Snowflake for experiment list and create one chart per
        # eligible experiment, storing the Explore URL as CHART_URL.
        print("\nGenerating per-experiment charts...")

        import re
        from datetime import datetime, timezone

        def camel_to_snake(name):
            return re.sub(r'(?<!^)[_\s]*([A-Z]+)', r'_\1', name).lower()

        def is_eligible(row):
            """Eligible = 2+ groups, or 1 group with <10 buckets. Not expired."""
            end_time = row.get("END_TIME")
            if end_time is not None and str(end_time).strip():
                try:
                    from dateutil import parser as dtparser
                    et = dtparser.parse(str(end_time))
                    if et.tzinfo is None:
                        et = et.replace(tzinfo=timezone.utc)
                    if et < datetime.now(timezone.utc):
                        return False
                except Exception:
                    pass
            groups_raw = row.get("GROUPS", "{}")
            try:
                groups = json.loads(groups_raw) if isinstance(groups_raw, str) else (groups_raw or {})
            except (json.JSONDecodeError, TypeError):
                groups = {}
            gc = len(groups)
            if gc == 0:
                return False
            if gc >= 2:
                return True
            total = sum(len(v) if isinstance(v, list) else 0 for v in groups.values())
            return total < 10

        # Fetch experiments via Superset's internal query runner (avoids extra SSO)
        try:
            from superset.sql_lab import execute_sql_statements
            from superset.common.db_query_status import QueryStatus

            exp_query = f"""
                SELECT ID, NAME, TYPE, GROUPS, END_TIME
                FROM FIVETRAN_DATABASE.POSTGRES_RDS_WAL_PUBLIC.EXPERIMENT_SPEC
                WHERE START_TIME IS NOT NULL
                  AND START_TIME >= '{EXPOSURE_CUTOFF}'::DATE
                  AND START_TIME <= CURRENT_TIMESTAMP()
                ORDER BY START_TIME DESC
            """
            payload = sf_db.get_df(exp_query)
            experiments = payload.to_dict('records')
            print(f"  Found {len(experiments)} experiments from Snowflake")
        except Exception as e:
            print(f"  WARNING: Could not query Snowflake for experiments: {e}")
            print("  Skipping per-experiment chart generation.")
            experiments = []

        eligible = [e for e in experiments if is_eligible(e)]
        print(f"  {len(eligible)} eligible for chart generation")

        charts_created = 0
        for exp in eligible:
            exp_id = str(exp["ID"])
            exp_name = str(exp["NAME"])
            exp_type = str(exp["TYPE"])
            full_name = f"AB_EXPOSURE_{camel_to_snake(exp_type)}_{camel_to_snake(exp_name)}".upper()
            slice_name = f"{full_name} {exp_id} V2 funnel"

            # Skip if chart already exists
            existing = db.session.query(Slice).filter_by(slice_name=slice_name).first()
            if existing:
                continue

            # Build alloc string from GROUPS: "groupName:bucketCount,..."
            groups_raw = exp.get("GROUPS", "{}")
            try:
                groups = json.loads(groups_raw) if isinstance(groups_raw, str) else (groups_raw or {})
            except (json.JSONDecodeError, TypeError):
                groups = {}
            alloc_str = ",".join(
                f"{name}:{len(buckets) if isinstance(buckets, list) else 0}"
                for name, buckets in groups.items()
            )

            params = {
                "datasource": f"{ds_exposures.id}__table",
                "viz_type": "experiment_ab",
                "groupby": ["EXPERIMENT_GROUP"],
                "metrics": list(SAVED_METRICS.keys()),
                "adhoc_filters": [{
                    "clause": "WHERE",
                    "expressionType": "SIMPLE",
                    "subject": "EXPERIMENT_SPEC_ID",
                    "operator": "IN",
                    "operatorId": "IN",
                    "comparator": [exp_id],
                    "isExtra": False,
                    "isNew": False,
                }],
                "row_limit": 1000,
                "alloc": alloc_str,
            }
            chart = Slice(
                slice_name=slice_name,
                viz_type="experiment_ab",
                datasource_type="table",
                datasource_id=ds_exposures.id,
                params=json.dumps(params),
            )
            db.session.add(chart)
            db.session.flush()
            charts_created += 1
            print(f"    Created chart '{slice_name}' (id={chart.id})")

        db.session.commit()
        print(f"  Created {charts_created} new per-experiment charts")

        # ── Update list dataset SQL with precomputed CHART_URL and EXPOSED_COUNT ──
        # Build mappings from the data we already fetched + charts we created.
        # This avoids the expensive COUNT(*) JOIN on the exposures table at query time.
        v2_charts = (
            db.session.query(Slice)
            .filter(Slice.slice_name.like("% V2 funnel"))
            .filter_by(viz_type="experiment_ab")
            .all()
        )
        chart_url_map = {}
        for c in v2_charts:
            # Extract experiment ID from chart name: "... {id} V2 funnel"
            parts = c.slice_name.replace(" V2 funnel", "").rsplit(" ", 1)
            if len(parts) == 2:
                chart_url_map[parts[1]] = f"/superset/explore/?slice_id={c.id}"

        # Build chart URL CASE expression
        if chart_url_map:
            chart_case_lines = "\n".join(
                f"    WHEN es.ID = {eid} THEN '{url}'"
                for eid, url in chart_url_map.items()
            )
            chart_url_expr = f"""CASE
{chart_case_lines}
    ELSE NULL
  END"""
        else:
            chart_url_expr = "NULL"

        # Precompute exposed counts from Snowflake (already connected)
        exposed_counts = {}
        try:
            ec_df = sf_db.get_df(f"""
                SELECT EXPERIMENT_SPEC_ID, COUNT(*) AS CNT
                FROM FIVETRAN_DATABASE.ETL_AUTO_GEN_TABLES.DIM_LOAN_APP_AB_EXPERIMENT_EXPOSURES_V2
                GROUP BY 1
            """)
            for _, row in ec_df.iterrows():
                exposed_counts[str(row.iloc[0])] = int(row.iloc[1])
            print(f"  Precomputed exposed counts for {len(exposed_counts)} experiments")
        except Exception as e:
            print(f"  WARNING: Could not precompute exposed counts: {e}")

        if exposed_counts:
            # Inline exposed counts as CASE — no expensive JOIN at query time
            count_case_lines = "\n".join(
                f"    WHEN es.ID = {eid} THEN {cnt}"
                for eid, cnt in exposed_counts.items()
            )
            exposed_count_expr = f"""CASE
{count_case_lines}
    ELSE 0
  END"""
            # Fast query: only reads experiment_spec (small table), no joins
            updated_list_sql = f"""\
SELECT
  es.ID,
  es.NAME,
  es.TYPE,
  es.HASH_TYPE,
  es.START_TIME,
  es.END_TIME,
  es.GROUPS,
  {exposed_count_expr} AS EXPOSED_COUNT,
  CASE
    WHEN es.END_TIME IS NOT NULL AND es.END_TIME < CURRENT_TIMESTAMP() THEN 'completed'
    WHEN es.START_TIME IS NOT NULL AND es.START_TIME <= CURRENT_TIMESTAMP() THEN 'running'
    ELSE 'draft'
  END AS STATUS,
  {chart_url_expr} AS CHART_URL
FROM FIVETRAN_DATABASE.POSTGRES_RDS_WAL_PUBLIC.EXPERIMENT_SPEC es
WHERE es.START_TIME IS NOT NULL
  AND es.START_TIME >= '{EXPOSURE_CUTOFF}'::DATE
  AND es.START_TIME <= CURRENT_TIMESTAMP()
  AND es.ID IN ({",".join(exposed_counts.keys())})
ORDER BY es.START_TIME DESC
"""
        else:
            # Fallback: keep the join (slower but works without precomputed data)
            updated_list_sql = f"""\
SELECT
  es.ID,
  es.NAME,
  es.TYPE,
  es.HASH_TYPE,
  es.START_TIME,
  es.END_TIME,
  es.GROUPS,
  COALESCE(ex.EXPOSED_CNT, 0) AS EXPOSED_COUNT,
  CASE
    WHEN es.END_TIME IS NOT NULL AND es.END_TIME < CURRENT_TIMESTAMP() THEN 'completed'
    WHEN es.START_TIME IS NOT NULL AND es.START_TIME <= CURRENT_TIMESTAMP() THEN 'running'
    ELSE 'draft'
  END AS STATUS,
  {chart_url_expr} AS CHART_URL
FROM FIVETRAN_DATABASE.POSTGRES_RDS_WAL_PUBLIC.EXPERIMENT_SPEC es
INNER JOIN (
  SELECT EXPERIMENT_SPEC_ID, COUNT(*) AS EXPOSED_CNT
  FROM FIVETRAN_DATABASE.ETL_AUTO_GEN_TABLES.DIM_LOAN_APP_AB_EXPERIMENT_EXPOSURES_V2
  GROUP BY 1
) ex ON es.ID = ex.EXPERIMENT_SPEC_ID
WHERE es.START_TIME IS NOT NULL
  AND es.START_TIME >= '{EXPOSURE_CUTOFF}'::DATE
  AND es.START_TIME <= CURRENT_TIMESTAMP()
ORDER BY es.START_TIME DESC
"""

        ds_list.sql = updated_list_sql

        # Add CHART_URL column to dataset if missing
        from superset.connectors.sqla.models import TableColumn
        existing_col_names = {c.column_name for c in ds_list.columns}
        if "CHART_URL" not in existing_col_names:
            ds_list.columns.append(TableColumn(
                column_name="CHART_URL",
                type="VARCHAR",
                table=ds_list,
            ))
            print("  Added CHART_URL column to list dataset")

        db.session.commit()
        print(f"  Updated list dataset SQL (precomputed counts: {len(exposed_counts)}, chart URLs: {len(chart_url_map)})")

        print("\n✅ Setup complete!")
        print(f"\n  Experiments list: http://localhost:8088/superset/dashboard/experiments/")
        print(f"\n  Datasets created:")
        print(f"    - experiment_list (id={ds_list.id})")
        print(f"    - experiment_exposures (id={ds_exposures.id})")
        print(f"    - {charts_created} per-experiment charts")
        print(f"\n  Cutoff date: {EXPOSURE_CUTOFF}")


if __name__ == "__main__":
    main()
