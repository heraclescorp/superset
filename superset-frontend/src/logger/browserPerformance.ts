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
import { nanoid } from 'nanoid';
import { QueryData, SupersetClient } from '@superset-ui/core';
import { ensureAppRoot } from 'src/utils/navigationUtils';
import getBootstrapData from 'src/utils/getBootstrapData';
import { Logger } from './LogUtils';

type Outcome = 'started' | 'success' | 'error' | 'abandoned';
type Phase = 'load' | 'data' | 'render' | 'request';
type BrowserEventName =
  | 'chart.load'
  | 'chart.data_ready'
  | 'chart.render'
  | 'chart.request'
  | 'dashboard.load';

interface Assets {
  chart_id?: number;
  dashboard_id?: number;
  dataset_id?: number;
  datasource_type?: string;
}

export interface BrowserEvent extends Assets {
  event_name: BrowserEventName;
  browser_event_id: string;
  browser_view_id: string;
  browser_load_id?: string;
  phase: Phase;
  outcome: Outcome;
  duration_seconds: number;
  active_duration_seconds?: number;
  hidden_duration_seconds?: number;
  backgrounded?: boolean;
  fully_backgrounded?: boolean;
  automation?: boolean;
  request_id?: string;
  expected_charts?: number;
  completed_charts?: number;
  cache_hit?: boolean;
  async_execution?: boolean;
  force_refresh?: boolean;
}

export interface DashboardView {
  id: string;
  dashboardId: number;
  started: number;
  hiddenAtStart: number;
  expected?: Set<number>;
  completed: Set<number>;
  failed: boolean;
  finished: boolean;
}

export interface ChartLoad {
  id: string;
  viewId: string;
  key: string;
  assets: Assets;
  started: number;
  hiddenAtStart: number;
  dataReady?: number;
  renderStarted?: number;
  requestId?: string;
  cacheHit?: boolean;
  asyncExecution?: boolean;
  forceRefresh: boolean;
  finished: boolean;
  detach: () => void;
  dashboard?: DashboardView;
}

// These are browser observations, never authoritative query/warehouse counts.
// Only typed identifiers and monotonic timings cross the existing log transport.
const MAX_EVENTS = 64;
const MAX_ACTIVE_LOADS = 512;
const dashboards = new Map<number, DashboardView>();
const loads = new Map<string, ChartLoad>();
const results = new WeakMap<QueryData[], ChartLoad>();
const exploreViews = new Map<string, string>();
let pending: BrowserEvent[] = [];
let flushTimer: number | undefined;
let listening = false;
let csrfToken: string | undefined;
let hiddenSince: number | null = null;
let hiddenAccumulated = 0;
let visibilityListening = false;

function hiddenNow(): number {
  const now = performance.now();
  return hiddenAccumulated + (hiddenSince !== null ? now - hiddenSince : 0);
}

function ensureVisibilityTracking(): void {
  if (visibilityListening || typeof document === 'undefined') return;
  visibilityListening = true;
  if (document.visibilityState === 'hidden') hiddenSince = performance.now();
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden') {
      hiddenSince ??= performance.now();
    } else if (hiddenSince !== null) {
      hiddenAccumulated += performance.now() - hiddenSince;
      hiddenSince = null;
    }
  });
}

function activeSeconds(started: number, hiddenAtStart: number): number {
  const wall = Math.max(0, performance.now() - started);
  const hidden = Math.max(0, hiddenNow() - hiddenAtStart);
  return Math.max(0, wall - hidden) / 1000;
}

export function flushBrowserEvents(unloading = false): void {
  if (flushTimer !== undefined) {
    clearTimeout(flushTimer);
    flushTimer = undefined;
  }
  if (!pending.length) return;
  const events = pending;
  pending = [];
  // Beacon cannot set CSRF/guest headers. Use the same authenticated form
  // convention as the legacy logger, including the CSRF token, on pagehide.
  try {
    if (unloading && navigator.sendBeacon && csrfToken !== undefined) {
      const form = new FormData();
      form.append('observability_events', JSON.stringify(events));
      form.append('csrf_token', csrfToken);
      const guestToken = SupersetClient.getGuestToken();
      if (guestToken) form.append('guest_token', guestToken);
      if (navigator.sendBeacon(ensureAppRoot('/log/?observability=browser'), form)) return;
    }
  } catch {
    // Beacon quota/security failures are non-fatal; try the normal client.
  }
  try {
    SupersetClient.post({
      endpoint: '/log/?observability=browser',
      jsonPayload: { observability_events: events },
      parseMethod: null,
      ignoreUnauthorized: true,
      fetchRetryOptions: { retries: 0 },
    }).catch(() => {});
  } catch {
    // Telemetry must not interfere with rendering or navigation.
  }
}


function isAutomation(): boolean {
  return typeof navigator !== 'undefined' && navigator.webdriver === true;
}

function emit(event: Omit<BrowserEvent, 'browser_event_id'>): void {
  if (getBootstrapData().common.conf.OBSERVABILITY_ENABLED !== true) return;
  pending.push({
    ...event,
    browser_event_id: nanoid(),
    automation: isAutomation(),
  });
  if (pending.length >= MAX_EVENTS) {
    flushBrowserEvents();
  } else if (flushTimer === undefined) {
    flushTimer = window.setTimeout(() => flushBrowserEvents(), 1000);
  }
}

function emitDashboard(view: DashboardView, outcome: Outcome): void {
  const wallSeconds = Math.max(0, performance.now() - view.started) / 1000;
  const active = activeSeconds(view.started, view.hiddenAtStart);
  const hidden = Math.max(0, wallSeconds - active);
  emit({
    event_name: 'dashboard.load',
    browser_view_id: view.id,
    dashboard_id: view.dashboardId,
    phase: 'load',
    outcome,
    duration_seconds: wallSeconds,
    active_duration_seconds: active,
    hidden_duration_seconds: hidden,
    backgrounded: hidden > 0,
    fully_backgrounded: active === 0 && wallSeconds > 0,
    expected_charts: view.expected?.size,
    completed_charts: view.completed.size,
  });
}
function dashboardView(dashboardId: number): DashboardView {
  const existing = dashboards.get(dashboardId);
  if (existing) return existing;
  const view: DashboardView = {
    id: nanoid(),
    dashboardId,
    // Include route/bootstrap time, including soft navigation's marked origin.
    started: performance.now() - Math.max(0, Logger.getTimestamp()),
    hiddenAtStart: hiddenNow(),
    completed: new Set(),
    failed: false,
    finished: false,
  };
  dashboards.set(dashboardId, view);
  emitDashboard(view, 'started');
  return view;
}


function completeDashboard(view: DashboardView): void {
  if (
    !view.finished &&
    view.expected &&
    [...view.expected].every(id => view.completed.has(id))
  ) {
    view.finished = true;
    emitDashboard(view, view.failed ? 'error' : 'success');
  }
}


function emitChart(
  load: ChartLoad,
  eventName: BrowserEventName,
  phase: Phase,
  outcome: Outcome,
  started: number,
): void {
  const wallSeconds = Math.max(0, performance.now() - started) / 1000;
  const active =
    eventName === 'chart.load'
      ? activeSeconds(started, load.hiddenAtStart)
      : undefined;
  const hidden =
    active === undefined ? undefined : Math.max(0, wallSeconds - active);
  emit({
    ...load.assets,
    event_name: eventName,
    browser_view_id: load.viewId,
    browser_load_id: load.id,
    phase,
    outcome,
    request_id: load.requestId,
    cache_hit: load.cacheHit,
    async_execution: load.asyncExecution,
    force_refresh: load.forceRefresh,
    duration_seconds: wallSeconds,
    ...(active === undefined
      ? {}
      : {
          active_duration_seconds: active,
          hidden_duration_seconds: hidden,
          backgrounded: (hidden ?? 0) > 0,
          fully_backgrounded: active === 0 && wallSeconds > 0,
        }),
  });
}
export function finishChartLoad(
  load: ChartLoad | undefined,
  outcome: Exclude<Outcome, 'started'>,
  phase: Phase = 'load',
): void {
  if (!load || load.finished) return;
  load.finished = true;
  load.detach();
  if (loads.get(load.key) === load) loads.delete(load.key);
  emitChart(load, 'chart.load', phase, outcome, load.started);
  if (load.dashboard && load.assets.chart_id && outcome !== 'abandoned') {
    load.dashboard.completed.add(load.assets.chart_id);
    load.dashboard.failed ||= outcome === 'error';
    completeDashboard(load.dashboard);
  }
}

export function closeDashboardView(view: DashboardView | undefined): void {
  if (!view) return;
  [...loads.values()].forEach(load => {
    if (load.dashboard === view) finishChartLoad(load, 'abandoned');
  });
  if (!view.finished) {
    view.finished = true;
    emitDashboard(view, 'abandoned');
  }
  if (dashboards.get(view.dashboardId) === view) dashboards.delete(view.dashboardId);
}

function listen(): void {
  if (listening) return;
  listening = true;
  ensureVisibilityTracking();
  try {
    SupersetClient.getCSRFToken()
      .then(token => {
        csrfToken = token;
      })
      .catch(() => {});
  } catch {
    // A missing authentication client must not prevent a chart from loading.
  }
  window.addEventListener('pagehide', () => {
    [...loads.values()].forEach(load => finishChartLoad(load, 'abandoned'));
    [...dashboards.values()].forEach(view => closeDashboardView(view));
    flushBrowserEvents(true);
    exploreViews.clear();
  });
}
export function openDashboardView(
  dashboardId: number,
  chartIds: number[],
): DashboardView | undefined {
  if (getBootstrapData().common.conf.OBSERVABILITY_ENABLED !== true) return undefined;
  listen();
  const view = dashboardView(dashboardId);
  view.expected = new Set(chartIds);
  completeDashboard(view);
  return view;
}

export function beginChartLoad(
  key: string | number | undefined,
  assets: Assets,
  signal: AbortSignal,
  datasource?: string,
  forceRefresh = false,
): ChartLoad | undefined {
  if (getBootstrapData().common.conf.OBSERVABILITY_ENABLED !== true) return undefined;
  listen();
  const [datasetId, datasourceType] = datasource?.split('__') ?? [];
  const loadAssets = Number(datasetId) > 0
    ? { ...assets, dataset_id: Number(datasetId), datasource_type: datasourceType }
    : assets;
  const loadKey = `${assets.dashboard_id ?? 'explore'}:${key ?? assets.chart_id ?? 0}`;
  const previous = loads.get(loadKey);
  if (previous) finishChartLoad(previous, 'abandoned');
  if (loads.size >= MAX_ACTIVE_LOADS) {
    const oldest = loads.values().next().value;
    if (oldest) finishChartLoad(oldest, 'abandoned');
  }
  const dashboard = assets.dashboard_id
    ? dashboardView(assets.dashboard_id)
    : undefined;
  const viewId = dashboard?.id ?? exploreViews.get(loadKey) ?? nanoid();
  if (!dashboard) exploreViews.set(loadKey, viewId);
  let load: ChartLoad;
  const onAbort = (): void => finishChartLoad(load, 'abandoned');
  load = {
    id: nanoid(),
    viewId,
    key: loadKey,
    assets: loadAssets,
    started: performance.now(),
    hiddenAtStart: hiddenNow(),
    forceRefresh,
    finished: false,
    detach: () => signal.removeEventListener('abort', onAbort),
    dashboard,
  };
  loads.set(loadKey, load);
  signal.addEventListener('abort', onAbort, { once: true });
  emitChart(load, 'chart.load', 'load', 'started', load.started);
  if (signal.aborted) onAbort();
  return load;
}


export function observeChartResponse(
  load: ChartLoad | undefined,
  response: Response,
  requestStarted?: number,
): void {
  if (!load || load.finished) return;
  load.asyncExecution = load.asyncExecution === true || response.status === 202;
  const requestId = response.headers?.get('X-Superset-Request-Id');
  if (requestId && /^[A-Za-z0-9_-]{8,64}$/.test(requestId)) {
    load.requestId = requestId;
    // Async submit and cached read-back each retain their request relation.
    emitChart(
      load,
      'chart.request',
      'request',
      response.ok ? 'success' : 'error',
      requestStarted ?? load.started,
    );
  }
}

export function chartDataReady(
  load: ChartLoad | undefined,
  data: QueryData[],
): void {
  if (!load || load.finished) return;
  load.dataReady = performance.now();
  load.cacheHit = data.length > 0 ? data.every(item => item.is_cached === true) : undefined;
  results.set(data, load);
  if (data.some(item => Boolean(item.error))) {
    finishChartLoad(load, 'error', 'data');
    return;
  }
  emitChart(load, 'chart.data_ready', 'data', 'success', load.started);
}

export function chartRenderStarted(data: QueryData[] | null | undefined): void {
  const load = data && results.get(data);
  if (load && !load.finished && load.renderStarted === undefined) {
    load.renderStarted = performance.now();
  }
}

export function chartRenderFinished(
  data: QueryData[] | null | undefined,
  failed = false,
): void {
  const load = data && results.get(data);
  if (!load || load.finished) return;
  emitChart(
    load,
    'chart.render',
    'render',
    failed ? 'error' : 'success',
    load.renderStarted ?? load.dataReady ?? load.started,
  );
  finishChartLoad(load, failed ? 'error' : 'success', 'render');
}

export function abandonChart(chartId: number, dashboardId?: number): void {
  const key = `${dashboardId ?? 'explore'}:${chartId}`;
  const load = loads.get(key);
  if (!dashboardId) exploreViews.delete(key);
  if (load) finishChartLoad(load, 'abandoned');
}

