import { ChartProps } from '@superset-ui/core';
import transformProps from '../../src/plugin/transformProps';
import {
  listChartProps,
  detailChartProps,
  aggregatedChartProps,
} from '../testData';

describe('ExperimentDashboard transformProps', () => {
  it('transforms list-view chart props', () => {
    const chartProps = new ChartProps(listChartProps);
    const result = transformProps(chartProps);

    expect(result.width).toBe(1200);
    expect(result.height).toBe(800);
    expect(result.experiments).toHaveLength(4);
    expect(result.experiments[0]).toHaveProperty('ID', 101);
    expect(result.experiments[0]).toHaveProperty('STATUS', 'running');
  });

  it('transforms detail-view chart props with alloc', () => {
    const chartProps = new ChartProps(detailChartProps);
    const result = transformProps(chartProps);

    expect(result.experiments).toHaveLength(6);
    expect(result.urlParams).toHaveProperty('alloc', 'treatment:5');
  });

  it('extracts metric labels from formData', () => {
    const chartProps = new ChartProps(aggregatedChartProps);
    const result = transformProps(chartProps);

    expect(result.metricLabels).toEqual([
      'UNQ_LEAD_NUM',
      'UNQ_PII_NUM',
      'UNQ_PQ_NUM',
      'UNQ_PQA_NUM',
      'UNQ_OFFER_NUM',
      'UNQ_ACCEPT_NUM',
      'UNQ_SCHED_NUM',
      'UNQ_NOTARY_NUM',
      'UNQ_BOOKED_NUM',
    ]);
    expect(result.groupbyColumns).toEqual(['EXPERIMENT_GROUP']);
  });

  it('handles empty queriesData gracefully', () => {
    const chartProps = new ChartProps({
      ...listChartProps,
      queriesData: [],
    });
    const result = transformProps(chartProps);

    expect(result.experiments).toEqual([]);
    expect(result.metricLabels).toEqual([]);
  });

  it('omits alloc from urlParams when not set', () => {
    const chartProps = new ChartProps({
      ...detailChartProps,
      formData: { ...detailChartProps.formData, alloc: undefined },
    });
    const result = transformProps(chartProps);

    // alloc not in formData or rawFormData → not in urlParams
    expect(result.urlParams?.alloc).toBeUndefined();
  });
});
