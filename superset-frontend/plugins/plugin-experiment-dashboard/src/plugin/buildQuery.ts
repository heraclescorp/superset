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
import { buildQueryContext, QueryFormData } from '@superset-ui/core';

/**
 * Builds a QueryContext using standard Superset metrics + groupby.
 *
 * Per-experiment charts have their own adhoc_filters baked in by the pipeline.
 * If experiment_id is present in the URL (legacy), it's injected as a WHERE filter.
 */
export default function buildQuery(formData: QueryFormData) {
  const extraFilters: { col: string; op: string; val: number }[] = [];
  const experimentId =
    formData.url_params?.experiment_id ||
    (typeof window !== 'undefined'
      ? new URLSearchParams(window.location.search).get('experiment_id')
      : null);

  if (experimentId) {
    extraFilters.push({
      col: 'EXPERIMENT_SPEC_ID',
      op: '==',
      val: Number(experimentId),
    });
  }

  return buildQueryContext(formData, baseQueryObject => [
    {
      ...baseQueryObject,
      filters: [...(baseQueryObject.filters || []), ...extraFilters],
      is_timeseries: false,
    },
  ]);
}
