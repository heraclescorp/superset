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
import { t, ChartMetadata, ChartPlugin } from '@superset-ui/core';
import buildQuery from './buildQuery';
import controlPanel from './controlPanel';
import transformProps from './transformProps';
import thumbnail from '../images/thumbnail.png';

/**
 * Experiment Dashboard plugin.
 *
 * Uses Superset's standard query pipeline for the experiment list
 * (via a virtual dataset). Detail stats are fetched from a custom
 * API endpoint (/api/v1/experiment/<id>/stats).
 */
export default class ExperimentDashboardPlugin extends ChartPlugin {
  constructor() {
    const metadata = new ChartMetadata({
      description:
        'Full A/B experiment dashboard: funnel, frequentist & Bayesian stats, SRM check, time series.',
      name: t('Experiment Dashboard'),
      thumbnail,
    });

    super({
      buildQuery,
      controlPanel,
      loadChart: () => import('../ExperimentDashboard'),
      metadata,
      transformProps,
    });
  }
}
