/**
 * Shared TypeScript interfaces for the experiment dashboard plugin.
 *
 * Includes the top-level component props (used by the plugin loader) and
 * internal data structures for list rows, funnel metrics, and SRM results.
 *
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

/** Props passed to the root experiment dashboard component by the plugin loader. */
export interface ExperimentDashboardProps {
  height: number;
  width: number;
  experiments: Record<string, unknown>[];
  metricLabels: string[];
  groupbyColumns: string[];
  urlParams?: Record<string, string>;
}

/** Parsed experiment row used in the list view. */
export interface ListExperiment {
  id: string;
  name: string;
  type: string;
  status: string;
  startTime: unknown;
  endTime: unknown;
  groups: Record<string, unknown>;
  exposedCount: number;
  isComparable: boolean;
  groupCount: number;
  totalBuckets: number;
  chartUrl: string | null;
}

/** A single funnel-stage metric comparing control vs variant. */
export interface FunnelMetric {
  stage: string;
  label: string;
  controlCount: number;
  variantCount: number;
  controlTotal: number;
  variantTotal: number;
  controlRate: number;
  variantRate: number;
  delta: number;
  relativeDelta: number;
  pValue: number;
  ci95: [number, number];
  direction: 'winning' | 'losing' | 'inconclusive';
  isSignificant: boolean;
}

/** Result of a Sample Ratio Mismatch (SRM) check. */
export interface SrmResult {
  passed: boolean;
  pValue: number;
  chiSq: number;
  expectedRatio: number;
  observedRatio: number;
}
