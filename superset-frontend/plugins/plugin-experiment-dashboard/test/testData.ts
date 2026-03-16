/**
 * Mock data fixtures for ExperimentDashboard plugin tests.
 *
 * Provides realistic experiment data for both list-view and detail-view
 * scenarios, following the pattern used by plugin-chart-table.
 */
import { DatasourceType, supersetTheme } from '@superset-ui/core';

// ── Shared scaffolding ───────────────────────────────────────────────────────

const baseDatasource = {
  id: 1,
  name: 'experiment_data',
  type: DatasourceType.Table,
  columns: [],
  metrics: [],
  columnFormats: {},
  verboseMap: {},
};

const baseFormData = {
  datasource: '1__table',
  viz_type: 'experiment_ab',
  metrics: [],
  groupby: [],
};

// ── List-view data ───────────────────────────────────────────────────────────

/** Rows that look like the experiment_list Snowflake dataset. */
export const listRows: Record<string, unknown>[] = [
  {
    ID: 101,
    NAME: 'aprIncrease0217',
    TYPE: 'underwritingPolicyParameters',
    HASH_TYPE: 'PHONE',
    START_TIME: '2026-02-17T08:00:00',
    END_TIME: null,
    GROUPS:
      '{"maybeEnableAprExperiment":[0,1,2,3,4,5],"+100bps":[6,7],"+200bps":[8,9]}',
    EXPOSED_COUNT: 26656,
    STATUS: 'running',
    CHART_URL: '/superset/explore/?slice_id=500',
  },
  {
    ID: 102,
    NAME: 'dummyDebug0226',
    TYPE: 'underwritingPolicyParameters',
    HASH_TYPE: 'PHONE',
    START_TIME: '2026-02-26T08:00:00',
    END_TIME: null,
    GROUPS: '{"dummyNoOpExperiment":[0,1,2,3,4,5,6,7,8,9]}',
    EXPOSED_COUNT: 26656,
    STATUS: 'running',
    CHART_URL: null,
  },
  {
    ID: 103,
    NAME: 'fraudCuts0315',
    TYPE: 'uccUnderwritingPolicyParameters',
    HASH_TYPE: 'PHONE',
    START_TIME: '2026-03-15T07:00:00',
    END_TIME: null,
    GROUPS: '{"enableRewardDQFraudHardcuts":[0,1,2,3,4,5,6,7,8]}',
    EXPOSED_COUNT: 6131,
    STATUS: 'running',
    CHART_URL: '/superset/explore/?slice_id=501',
  },
  {
    ID: 104,
    NAME: 'completedExperiment',
    TYPE: 'underwritingPolicyParameters',
    HASH_TYPE: 'PHONE',
    START_TIME: '2025-12-01T08:00:00',
    END_TIME: '2026-02-01T08:00:00',
    GROUPS: '{"control":[0,1,2,3,4],"treatment":[5,6,7,8,9]}',
    EXPOSED_COUNT: 50000,
    STATUS: 'completed',
    CHART_URL: '/superset/explore/?slice_id=502',
  },
];

// ── Detail-view data (raw row-level IS_* flags) ──────────────────────────────

/** Individual exposure rows for a 50/50 two-group experiment. */
function makeDetailRows(): Record<string, unknown>[] {
  const rows: Record<string, unknown>[] = [];
  // 500 control, 500 treatment — clean 50/50 split
  for (let i = 0; i < 500; i += 1) {
    const isControl = i < 250;
    const group = isControl ? 'IMPLICIT_CONTROL' : 'treatment';
    rows.push({
      EXPERIMENT_SPEC_ID: 104,
      EXPERIMENT_GROUP: group,
      LOAN_APPLICATION_ID: 1000000 + i,
      START_TIME: '2025-12-01T08:00:00',
      END_TIME: '2026-02-01T08:00:00',
      // Funnel flags — conversion rates differ between groups
      IS_LEAD: 1,
      IS_PII: Math.random() < (isControl ? 0.8 : 0.82) ? 1 : 0,
      IS_PQ: Math.random() < (isControl ? 0.5 : 0.55) ? 1 : 0,
      IS_PQA: Math.random() < (isControl ? 0.3 : 0.33) ? 1 : 0,
      IS_OFFER: Math.random() < (isControl ? 0.2 : 0.22) ? 1 : 0,
      IS_ACCEPT: Math.random() < (isControl ? 0.1 : 0.12) ? 1 : 0,
      IS_SCHED: Math.random() < (isControl ? 0.08 : 0.09) ? 1 : 0,
      IS_NOTARY_STARTED: Math.random() < (isControl ? 0.06 : 0.07) ? 1 : 0,
      IS_BOOKED: Math.random() < (isControl ? 0.04 : 0.05) ? 1 : 0,
    });
  }
  return rows;
}

/** Deterministic detail rows — small set for predictable test assertions. */
export const detailRowsSmall: Record<string, unknown>[] = [
  {
    EXPERIMENT_SPEC_ID: 104,
    EXPERIMENT_GROUP: 'IMPLICIT_CONTROL',
    LOAN_APPLICATION_ID: 1,
    START_TIME: '2025-12-01T08:00:00',
    END_TIME: '2026-02-01T08:00:00',
    IS_LEAD: 1,
    IS_PII: 1,
    IS_PQ: 1,
    IS_PQA: 0,
    IS_OFFER: 0,
    IS_ACCEPT: 0,
    IS_SCHED: 0,
    IS_NOTARY_STARTED: 0,
    IS_BOOKED: 0,
  },
  {
    EXPERIMENT_SPEC_ID: 104,
    EXPERIMENT_GROUP: 'IMPLICIT_CONTROL',
    LOAN_APPLICATION_ID: 2,
    START_TIME: '2025-12-01T08:00:00',
    END_TIME: '2026-02-01T08:00:00',
    IS_LEAD: 1,
    IS_PII: 1,
    IS_PQ: 0,
    IS_PQA: 0,
    IS_OFFER: 0,
    IS_ACCEPT: 0,
    IS_SCHED: 0,
    IS_NOTARY_STARTED: 0,
    IS_BOOKED: 0,
  },
  {
    EXPERIMENT_SPEC_ID: 104,
    EXPERIMENT_GROUP: 'IMPLICIT_CONTROL',
    LOAN_APPLICATION_ID: 3,
    START_TIME: '2025-12-01T08:00:00',
    END_TIME: '2026-02-01T08:00:00',
    IS_LEAD: 1,
    IS_PII: 0,
    IS_PQ: 0,
    IS_PQA: 0,
    IS_OFFER: 0,
    IS_ACCEPT: 0,
    IS_SCHED: 0,
    IS_NOTARY_STARTED: 0,
    IS_BOOKED: 0,
  },
  {
    EXPERIMENT_SPEC_ID: 104,
    EXPERIMENT_GROUP: 'treatment',
    LOAN_APPLICATION_ID: 4,
    START_TIME: '2025-12-01T08:00:00',
    END_TIME: '2026-02-01T08:00:00',
    IS_LEAD: 1,
    IS_PII: 1,
    IS_PQ: 1,
    IS_PQA: 1,
    IS_OFFER: 0,
    IS_ACCEPT: 0,
    IS_SCHED: 0,
    IS_NOTARY_STARTED: 0,
    IS_BOOKED: 0,
  },
  {
    EXPERIMENT_SPEC_ID: 104,
    EXPERIMENT_GROUP: 'treatment',
    LOAN_APPLICATION_ID: 5,
    START_TIME: '2025-12-01T08:00:00',
    END_TIME: '2026-02-01T08:00:00',
    IS_LEAD: 1,
    IS_PII: 1,
    IS_PQ: 1,
    IS_PQA: 0,
    IS_OFFER: 0,
    IS_ACCEPT: 0,
    IS_SCHED: 0,
    IS_NOTARY_STARTED: 0,
    IS_BOOKED: 0,
  },
  {
    EXPERIMENT_SPEC_ID: 104,
    EXPERIMENT_GROUP: 'treatment',
    LOAN_APPLICATION_ID: 6,
    START_TIME: '2025-12-01T08:00:00',
    END_TIME: '2026-02-01T08:00:00',
    IS_LEAD: 1,
    IS_PII: 0,
    IS_PQ: 0,
    IS_PQA: 0,
    IS_OFFER: 0,
    IS_ACCEPT: 0,
    IS_SCHED: 0,
    IS_NOTARY_STARTED: 0,
    IS_BOOKED: 0,
  },
];

export const detailRows = makeDetailRows();

// ── Aggregated metric rows (computeAggregatedStats format) ───────────────────

export const aggregatedRows: Record<string, unknown>[] = [
  {
    EXPERIMENT_GROUP: 'IMPLICIT_CONTROL',
    UNQ_LEAD_NUM: 1200,
    UNQ_PII_NUM: 960,
    UNQ_PQ_NUM: 600,
    UNQ_PQA_NUM: 360,
    UNQ_OFFER_NUM: 240,
    UNQ_ACCEPT_NUM: 120,
    UNQ_SCHED_NUM: 96,
    UNQ_NOTARY_NUM: 72,
    UNQ_BOOKED_NUM: 48,
  },
  {
    EXPERIMENT_GROUP: 'showButton',
    UNQ_LEAD_NUM: 1180,
    UNQ_PII_NUM: 980,
    UNQ_PQ_NUM: 650,
    UNQ_PQA_NUM: 390,
    UNQ_OFFER_NUM: 260,
    UNQ_ACCEPT_NUM: 140,
    UNQ_SCHED_NUM: 105,
    UNQ_NOTARY_NUM: 80,
    UNQ_BOOKED_NUM: 55,
  },
];

// ── Three-group experiment ───────────────────────────────────────────────────

export const threeGroupRows: Record<string, unknown>[] = [
  {
    EXPERIMENT_GROUP: 'IMPLICIT_CONTROL',
    UNQ_LEAD_NUM: 800,
    UNQ_PII_NUM: 640,
    UNQ_PQ_NUM: 400,
  },
  {
    EXPERIMENT_GROUP: 'variantA',
    UNQ_LEAD_NUM: 810,
    UNQ_PII_NUM: 670,
    UNQ_PQ_NUM: 440,
  },
  {
    EXPERIMENT_GROUP: 'variantB',
    UNQ_LEAD_NUM: 790,
    UNQ_PII_NUM: 620,
    UNQ_PQ_NUM: 380,
  },
];

// ── ChartProps-compatible objects ─────────────────────────────────────────────

export const listChartProps = {
  width: 1200,
  height: 800,
  datasource: baseDatasource,
  hooks: {},
  initialValues: {},
  queriesData: [{ data: listRows }],
  formData: { ...baseFormData },
  theme: supersetTheme,
};

export const detailChartProps = {
  width: 1200,
  height: 800,
  datasource: baseDatasource,
  hooks: {},
  initialValues: {},
  queriesData: [{ data: detailRowsSmall }],
  formData: {
    ...baseFormData,
    metrics: ['UNQ_LEAD_NUM', 'UNQ_PII_NUM', 'UNQ_PQ_NUM'],
    groupby: ['EXPERIMENT_GROUP'],
    alloc: 'treatment:5',
  },
  rawFormData: { alloc: 'treatment:5' },
  theme: supersetTheme,
};

export const aggregatedChartProps = {
  width: 1200,
  height: 800,
  datasource: baseDatasource,
  hooks: {},
  initialValues: {},
  queriesData: [{ data: aggregatedRows }],
  formData: {
    ...baseFormData,
    metrics: [
      'UNQ_LEAD_NUM',
      'UNQ_PII_NUM',
      'UNQ_PQ_NUM',
      'UNQ_PQA_NUM',
      'UNQ_OFFER_NUM',
      'UNQ_ACCEPT_NUM',
      'UNQ_SCHED_NUM',
      'UNQ_NOTARY_NUM',
      'UNQ_BOOKED_NUM',
    ],
    groupby: ['EXPERIMENT_GROUP'],
    alloc: 'showButton:8',
  },
  rawFormData: { alloc: 'showButton:8' },
  theme: supersetTheme,
};

export default {
  listRows,
  detailRows,
  detailRowsSmall,
  aggregatedRows,
  threeGroupRows,
  listChartProps,
  detailChartProps,
  aggregatedChartProps,
};
