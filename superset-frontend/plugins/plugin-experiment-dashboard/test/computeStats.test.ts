import { computeDetailStats, computeAggregatedStats } from '../src/stats';
import { detailRowsSmall, aggregatedRows, threeGroupRows } from './testData';

describe('computeDetailStats', () => {
  it('computes stats from raw row-level data', () => {
    const result = computeDetailStats(detailRowsSmall);

    expect(result.groupNames).toEqual(['IMPLICIT_CONTROL', 'treatment']);
    expect(result.controlName).toBe('IMPLICIT_CONTROL');
    expect(result.controlTotal).toBe(3);
    expect(result.total).toBe(6);
    expect(result.experimentId).toBe('104');
  });

  it('computes funnel metrics for each variant', () => {
    const result = computeDetailStats(detailRowsSmall);

    expect(result.variantMetrics).toHaveLength(1);
    const variant = result.variantMetrics[0];
    expect(variant.name).toBe('treatment');
    expect(variant.total).toBe(3);

    // IS_LEAD: all 6 rows have IS_LEAD=1
    const lead = variant.metrics.find(m => m.stage === 'IS_LEAD');
    expect(lead).toBeDefined();
    expect(lead!.controlCount).toBe(3);
    expect(lead!.variantCount).toBe(3);
    expect(lead!.controlRate).toBe(1);
    expect(lead!.variantRate).toBe(1);

    // IS_PII: control has 2/3, treatment has 2/3
    const pii = variant.metrics.find(m => m.stage === 'IS_PII');
    expect(pii).toBeDefined();
    expect(pii!.controlCount).toBe(2);
    expect(pii!.variantCount).toBe(2);
  });

  it('computes SRM check', () => {
    const result = computeDetailStats(detailRowsSmall);

    expect(result.srm).not.toBeNull();
    expect(result.srm!.expectedRatio).toBe(0.5);
    // 3 vs 3 is perfectly balanced
    expect(result.srm!.passed).toBe(true);
    expect(result.srm!.pValue).toBeGreaterThan(0.01);
  });

  it('handles single group (no SRM possible)', () => {
    const singleGroup = detailRowsSmall.filter(
      r => r.EXPERIMENT_GROUP === 'IMPLICIT_CONTROL',
    );
    const result = computeDetailStats(singleGroup);

    expect(result.groupNames).toEqual(['IMPLICIT_CONTROL']);
    expect(result.variantMetrics).toHaveLength(0);
    expect(result.srm).toBeNull();
  });
});

describe('computeAggregatedStats', () => {
  const metricLabels = [
    'UNQ_LEAD_NUM',
    'UNQ_PII_NUM',
    'UNQ_PQ_NUM',
    'UNQ_PQA_NUM',
    'UNQ_OFFER_NUM',
    'UNQ_ACCEPT_NUM',
    'UNQ_SCHED_NUM',
    'UNQ_NOTARY_NUM',
    'UNQ_BOOKED_NUM',
  ];
  const groupby = ['EXPERIMENT_GROUP'];

  it('computes stats from aggregated metric rows', () => {
    const result = computeAggregatedStats(
      aggregatedRows,
      metricLabels,
      groupby,
      'showButton:8',
    );

    expect(result.groupNames).toEqual(['IMPLICIT_CONTROL', 'showButton']);
    expect(result.controlName).toBe('IMPLICIT_CONTROL');
    expect(result.controlTotal).toBe(1200); // UNQ_LEAD_NUM for control
  });

  it('produces variant metrics for each metric label', () => {
    const result = computeAggregatedStats(
      aggregatedRows,
      metricLabels,
      groupby,
      'showButton:8',
    );

    expect(result.variantMetrics).toHaveLength(1);
    const variant = result.variantMetrics[0];
    expect(variant.name).toBe('showButton');
    expect(variant.total).toBe(1180); // UNQ_LEAD_NUM for variant

    // Check that metrics contain the expected stages
    // First metric label (UNQ_LEAD_NUM) is used as totalMetric, so
    // variant.metrics starts from the second label (UNQ_PII_NUM)
    expect(variant.metrics.length).toBe(metricLabels.length - 1);

    const pii = variant.metrics[0];
    expect(pii.controlCount).toBe(960);
    expect(pii.variantCount).toBe(980);
  });

  it('computes SRM with alloc parameter', () => {
    const result = computeAggregatedStats(
      aggregatedRows,
      metricLabels,
      groupby,
      'showButton:8',
    );

    expect(result.srm).not.toBeNull();
    // alloc "showButton:8" means 8 buckets for showButton, 2 for control
    // Expected ratio: 2/10 = 0.2 for control
    expect(result.srm!.expectedRatio).toBeCloseTo(0.2, 1);
  });

  it('handles three-group experiments', () => {
    const result = computeAggregatedStats(
      threeGroupRows,
      ['UNQ_LEAD_NUM', 'UNQ_PII_NUM', 'UNQ_PQ_NUM'],
      groupby,
    );

    expect(result.groupNames).toHaveLength(3);
    expect(result.variantMetrics).toHaveLength(2);
    expect(result.srm).not.toBeNull();
  });

  it('handles empty data', () => {
    const result = computeAggregatedStats([], metricLabels, groupby);

    expect(result.groupNames).toHaveLength(0);
    expect(result.variantMetrics).toHaveLength(0);
    expect(result.srm).toBeNull();
  });
});
