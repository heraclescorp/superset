/**
 * Root component for the Aven Experiment Dashboard plugin.
 *
 * Routes between the list view (when dataset contains STATUS columns) and the
 * detail view (when dataset contains metric/exposure data). The default export
 * is consumed by the plugin loader.
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
import React, { useMemo } from 'react';
import { ExperimentDashboardProps } from './types';
import { isListData } from './utils';
import ExperimentListView from './components/ExperimentListView';
import ExperimentDetailView from './components/ExperimentDetailView';

export default function AvenExperimentDash({
  height,
  experiments: rows,
  metricLabels = [],
  groupbyColumns = [],
  urlParams = {},
}: ExperimentDashboardProps) {
  const listMode = useMemo(() => isListData(rows), [rows]);

  if (listMode) {
    return <ExperimentListView rows={rows} height={height} />;
  }

  return (
    <ExperimentDetailView
      rows={rows}
      height={height}
      metricLabels={metricLabels}
      groupbyColumns={groupbyColumns}
      urlParams={urlParams}
    />
  );
}

