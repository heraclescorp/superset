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
import { t, validateNonEmpty } from '@superset-ui/core';
import {
  ControlPanelConfig,
  sharedControls,
} from '@superset-ui/chart-controls';

const config: ControlPanelConfig = {
  controlPanelSections: [
    {
      label: t('Query'),
      expanded: true,
      controlSetRows: [
        [
          {
            name: 'groupby',
            config: {
              ...sharedControls.groupby,
              label: t('Columns'),
              description: t(
                'Column to group by (e.g. EXPERIMENT_GROUP). Each unique value becomes a group for comparison.',
              ),
              validators: [validateNonEmpty],
            },
          },
        ],
        [
          {
            name: 'metrics',
            config: {
              ...sharedControls.metrics,
              label: t('Metrics'),
              description: t(
                'Metrics to compare across groups (e.g. UNQ_LEAD_#, UNQ_PQ_#). Each metric gets CI bars.',
              ),
              validators: [validateNonEmpty],
            },
          },
        ],
        ['adhoc_filters'],
        ['row_limit'],
        [
          {
            name: 'alloc',
            config: {
              type: 'HiddenControl',
              label: t('Allocation'),
              default: '',
            },
          },
        ],
      ],
    },
  ],
};

export default config;
