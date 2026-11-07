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
import { QueryData, SupersetClient } from '@superset-ui/core';
import getBootstrapData from 'src/utils/getBootstrapData';
import { DEFAULT_BOOTSTRAP_DATA } from 'src/constants';
import {
  beginChartLoad,
  chartDataReady,
  chartRenderStarted,
  chartRenderFinished,
  closeDashboardView,
  finishChartLoad,
  flushBrowserEvents,
  observeChartResponse,
  openDashboardView,
  BrowserEvent,
} from './browserPerformance';

jest.mock('@superset-ui/core', () => ({
  ...jest.requireActual('@superset-ui/core'),
  SupersetClient: {
    ...jest.requireActual('@superset-ui/core').SupersetClient,
    post: jest.fn(() => Promise.resolve({})),
    getCSRFToken: jest.fn(() => Promise.resolve('csrf-token')),
    getGuestToken: jest.fn(() => undefined),
  },
}));

jest.mock('src/utils/getBootstrapData', () => ({
  ...jest.requireActual('src/utils/getBootstrapData'),
  __esModule: true,
  default: jest.fn(() => ({
    ...jest.requireActual('src/utils/getBootstrapData').default(),
    common: {
      ...jest.requireActual('src/utils/getBootstrapData').default().common,
      conf: { OBSERVABILITY_ENABLED: true },
    },
  })),
}));

let clock = 0;
let performanceSpy: jest.SpyInstance;

function events(): BrowserEvent[] {
  return jest.mocked(SupersetClient.post).mock.calls.flatMap(([options]) => {
    const payload = options.jsonPayload;
    return payload && typeof payload === 'object'
      ? payload.observability_events ?? []
      : [];
  });
}

beforeEach(() => {
  jest.useFakeTimers();
  clock = 0;
  performanceSpy = jest.spyOn(performance, 'now').mockImplementation(() => clock);
  jest.mocked(SupersetClient.post).mockClear();
  Object.defineProperty(navigator, 'sendBeacon', {
    configurable: true,
    writable: true,
    value: undefined,
  });
});

afterEach(() => {
  Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'visible' });
  document.dispatchEvent(new Event('visibilitychange'));
  Object.defineProperty(navigator, 'webdriver', { configurable: true, value: false });
  window.dispatchEvent(new Event('pagehide'));
  performanceSpy.mockRestore();
  jest.useRealTimers();
});

test('separates data, render and end-to-end timings and completes the dashboard once', () => {
  const view = openDashboardView(10, [1]);
  const load = beginChartLoad(1, { chart_id: 1, dashboard_id: 10 }, new AbortController().signal);
  observeChartResponse(load, new Response(null, {
    status: 202,
    headers: { 'X-Superset-Request-Id': 'submit-request-id' },
  }));
  clock = 100;
  observeChartResponse(load, new Response(null, {
    headers: { 'X-Superset-Request-Id': 'readback-request-id' },
  }), 80);
  const data: QueryData[] = [];
  chartDataReady(load, data);
  clock = 120;
  chartRenderStarted(data);
  clock = 160;
  chartRenderFinished(data);
  chartRenderFinished(data);
  closeDashboardView(view);
  flushBrowserEvents();
  const observed = events();
  expect(observed.filter(event => event.event_name === 'chart.request').map(event => event.request_id)).toEqual([
    'submit-request-id', 'readback-request-id',
  ]);
  expect(observed.find(event => event.event_name === 'chart.request' && event.request_id === 'readback-request-id'))
    .toMatchObject({ duration_seconds: 0.02 });
  expect(observed.find(event => event.event_name === 'chart.data_ready')).toMatchObject({ duration_seconds: 0.1 });
  expect(observed.filter(event => event.event_name === 'chart.render')).toEqual([
    expect.objectContaining({ duration_seconds: 0.04, request_id: 'readback-request-id', outcome: 'success' }),
  ]);
  expect(observed.filter(event => event.event_name === 'chart.load' && event.outcome === 'success')).toEqual([
    expect.objectContaining({
      duration_seconds: 0.16,
      browser_load_id: load?.id,
      async_execution: true,
      force_refresh: false,
    }),
  ]);
  expect(observed.filter(event => event.event_name === 'dashboard.load' && event.outcome !== 'started')).toEqual([
    expect.objectContaining({ outcome: 'success', expected_charts: 1, completed_charts: 1 }),
  ]);
  expect(new Set(observed.map(event => event.browser_event_id)).size).toBe(observed.length);
  expect(new Set(observed.map(event => event.browser_view_id))).toEqual(new Set([view?.id]));
});

test('excludes hidden-tab time from active browser durations', () => {
  const view = openDashboardView(30, [5]);
  const load = beginChartLoad(5, { chart_id: 5, dashboard_id: 30 }, new AbortController().signal);
  clock = 10;
  Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'hidden' });
  document.dispatchEvent(new Event('visibilitychange'));
  clock = 60;
  Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'visible' });
  document.dispatchEvent(new Event('visibilitychange'));
  clock = 80;
  const data: QueryData[] = [];
  chartDataReady(load, data);
  chartRenderFinished(data);
  closeDashboardView(view);
  flushBrowserEvents();
  const chartLoad = events().find(event => event.event_name === 'chart.load' && event.outcome === 'success');
  expect(chartLoad?.duration_seconds).toBeCloseTo(0.08);
  expect(chartLoad?.active_duration_seconds).toBeCloseTo(0.03);
  expect(chartLoad?.hidden_duration_seconds).toBeCloseTo(0.05);
  expect(chartLoad?.backgrounded).toBe(true);
  expect(chartLoad?.fully_backgrounded).toBe(false);
  const dashboardLoad = events().find(event => event.event_name === 'dashboard.load' && event.outcome === 'success');
  expect(dashboardLoad?.active_duration_seconds).toBeCloseTo(0.03);
  expect(dashboardLoad?.hidden_duration_seconds).toBeCloseTo(0.05);
});

test('flags a load that completes entirely in the background', () => {
  Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'hidden' });
  document.dispatchEvent(new Event('visibilitychange'));
  const view = openDashboardView(31, [6]);
  const load = beginChartLoad(6, { chart_id: 6, dashboard_id: 31 }, new AbortController().signal);
  clock = 50;
  const data: QueryData[] = [];
  chartDataReady(load, data);
  chartRenderFinished(data);
  closeDashboardView(view);
  flushBrowserEvents();
  const chartLoad = events().find(event => event.event_name === 'chart.load' && event.outcome === 'success');
  expect(chartLoad?.active_duration_seconds).toBe(0);
  expect(chartLoad?.hidden_duration_seconds).toBeCloseTo(0.05);
  expect(chartLoad?.fully_backgrounded).toBe(true);
});

test('marks Playwright browser loads as automation', () => {
  Object.defineProperty(navigator, 'webdriver', { configurable: true, value: true });
  const view = openDashboardView(32, [7]);
  const load = beginChartLoad(7, { chart_id: 7, dashboard_id: 32 }, new AbortController().signal);
  clock = 25;
  const data: QueryData[] = [];
  chartDataReady(load, data);
  chartRenderFinished(data);
  closeDashboardView(view);
  flushBrowserEvents();
  const observed = events();
  expect(observed.find(event => event.event_name === 'chart.load' && event.outcome === 'success')?.automation).toBe(true);
  expect(observed.find(event => event.event_name === 'dashboard.load' && event.outcome === 'success')?.automation).toBe(true);
});

test('superseded results and abort callbacks cannot complete the replacement load', () => {
  const firstController = new AbortController();
  const first = beginChartLoad(2, { chart_id: 2 }, firstController.signal);
  const firstData: QueryData[] = [];
  chartDataReady(first, firstData);
  clock = 25;
  const second = beginChartLoad(2, { chart_id: 2 }, new AbortController().signal);
  firstController.abort();
  chartRenderFinished(firstData);
  const secondData: QueryData[] = [];
  chartDataReady(second, secondData);
  clock = 50;
  chartRenderFinished(secondData);
  flushBrowserEvents();
  expect(events().filter(event => event.event_name === 'chart.load' && event.outcome !== 'started')).toEqual([
    expect.objectContaining({ browser_load_id: first?.id, outcome: 'abandoned' }),
    expect.objectContaining({ browser_load_id: second?.id, outcome: 'success' }),
  ]);
});

test('render failures and unfinished dashboard navigation have distinct terminal outcomes', () => {
  const view = openDashboardView(20, [3, 4]);
  const load = beginChartLoad(3, { chart_id: 3, dashboard_id: 20 }, new AbortController().signal);
  const data: QueryData[] = [];
  chartDataReady(load, data);
  chartRenderFinished(data, true);
  closeDashboardView(view);
  flushBrowserEvents();
  expect(events()).toEqual(expect.arrayContaining([
    expect.objectContaining({ event_name: 'chart.render', outcome: 'error' }),
    expect.objectContaining({ event_name: 'chart.load', outcome: 'error', phase: 'render' }),
    expect.objectContaining({ event_name: 'dashboard.load', outcome: 'abandoned', completed_charts: 1, expected_charts: 2 }),
  ]));
});

test('transport failures are isolated and pending queues stay bounded', () => {
  jest.mocked(SupersetClient.post).mockImplementationOnce(() => Promise.reject(new Error('offline')));
  for (let id = 1; id <= 70; id += 1) {
    const load = beginChartLoad(id, { chart_id: id }, new AbortController().signal);
    finishChartLoad(load, 'error', 'data');
  }
  flushBrowserEvents();
  expect(jest.mocked(SupersetClient.post).mock.calls).toHaveLength(3);
  expect(jest.mocked(SupersetClient.post).mock.calls.every(([options]) => {
    const payload = options.jsonPayload;
    return payload && typeof payload === 'object' && payload.observability_events.length <= 64;
  })).toBe(true);
});

test('pagehide sends pending abandonment through the authenticated CSRF-aware log form', async () => {
  await Promise.resolve();
  const beacon = jest.fn((url: string, body: FormData): boolean => Boolean(url && body));
  Object.defineProperty(navigator, 'sendBeacon', { value: beacon, writable: true });
  jest.mocked(SupersetClient.getGuestToken).mockReturnValueOnce('guest-credential');
  const load = beginChartLoad(5, { chart_id: 5 }, new AbortController().signal);
  window.dispatchEvent(new Event('pagehide'));
  expect(beacon).toHaveBeenCalledTimes(1);
  const form: FormData = beacon.mock.calls[0][1];
  expect(form.get('csrf_token')).toBe('csrf-token');
  expect(form.get('guest_token')).toBe('guest-credential');
  const payload = form.get('observability_events');
  expect(typeof payload).toBe('string');
  if (typeof payload !== 'string') throw new Error('Missing browser events');
  expect(JSON.parse(payload)).toEqual(expect.arrayContaining([
    expect.objectContaining({ browser_load_id: load?.id, outcome: 'abandoned' }),
  ]));
  expect(payload).not.toContain('guest-credential');
});

test('disabled instrumentation never starts loads or sends requests', () => {
  const disabled = {
    ...DEFAULT_BOOTSTRAP_DATA,
    common: {
      ...DEFAULT_BOOTSTRAP_DATA.common,
      conf: { OBSERVABILITY_ENABLED: false },
    },
  };
  jest.mocked(getBootstrapData).mockReturnValueOnce(disabled);
  expect(beginChartLoad(6, { chart_id: 6 }, new AbortController().signal)).toBeUndefined();
  jest.mocked(getBootstrapData).mockReturnValueOnce(disabled);
  expect(openDashboardView(30, [6])).toBeUndefined();
  flushBrowserEvents();
  expect(SupersetClient.post).not.toHaveBeenCalled();
});
