/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { ChartProps, getMetricLabel } from '@superset-ui/core';
import { ExperimentDashboardProps } from '../types';

/**
 * Transforms Superset query results into props for the experiment dashboard.
 *
 * Data arrives as aggregated rows (one per group) with metric columns.
 * We pass the raw data and metric labels so the chart can render CI bars.
 */
export default function transformProps(
  chartProps: ChartProps,
): ExperimentDashboardProps {
  const { width, height, queriesData, formData, rawFormData } = chartProps;
  const data = queriesData?.[0]?.data || [];

  // Extract metric labels for the chart to know which columns are metrics
  const metricLabels = (formData?.metrics || []).map((m: any) =>
    getMetricLabel(m),
  );
  const groupbyColumns = formData?.groupby || [];

  // alloc is stored in chart params for SRM expected ratios
  const alloc =
    formData?.alloc || (rawFormData as Record<string, unknown>)?.alloc || '';

  return {
    width,
    height,
    experiments: data,
    metricLabels,
    groupbyColumns,
    urlParams: {
      ...(formData?.url_params || {}),
      ...(alloc ? { alloc } : {}),
    },
  };
}
