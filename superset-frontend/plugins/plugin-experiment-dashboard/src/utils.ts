/**
 * Pure helper and formatting functions used across the experiment dashboard.
 *
 * Includes number formatting, relative-time display, case-insensitive row
 * accessors, statistical output formatters, and dataset detection utilities.
 */

import { ListExperiment } from './types';

// ── Number / display helpers ──────────────────────────────────────────────────

/** Locale-aware integer/float formatting (e.g. 1,234). */
export function fmtNum(n: number): string {
  return n.toLocaleString();
}

/** Convert a timestamp (epoch-ms, ISO string, etc.) into a human-readable relative label. */
export function timeAgo(val: unknown): string {
  if (val == null || val === '') return '—';

  let d: Date;
  if (typeof val === 'number') {
    d = new Date(val);
  } else {
    const s = String(val);
    d = new Date(s);
    if (isNaN(d.getTime())) {
      const num = Number(s);
      d = !isNaN(num) ? new Date(num) : d;
    }
  }

  if (isNaN(d.getTime())) return String(val).slice(0, 10);

  const diff = Date.now() - d.getTime();
  const days = Math.floor(diff / 86400000);
  if (days < 0) return d.toLocaleDateString();
  if (days === 0) return 'Today';
  if (days === 1) return 'Yesterday';
  if (days < 30) return `${days}d ago`;
  if (days < 365) return `${Math.floor(days / 30)}mo ago`;
  return `${Math.floor(days / 365)}y ago`;
}

/** Case-insensitive row field accessor (tries UPPER then lower key). */
export function get(row: Record<string, unknown>, key: string): unknown {
  return row[key] ?? row[key.toLowerCase()] ?? '';
}

// ── Dataset detection ─────────────────────────────────────────────────────────

/** Returns true when the dataset contains list-view columns (STATUS). */
export function isListData(rows: Record<string, unknown>[]): boolean {
  if (rows.length === 0) return true;
  const first = rows[0];
  return 'STATUS' in first || 'status' in first;
}

// ── List-row parser ───────────────────────────────────────────────────────────

/** Parse a raw dataset row into a structured ListExperiment object. */
export function parseListRow(row: Record<string, unknown>): ListExperiment {
  let groups: Record<string, unknown> = {};
  try {
    const raw = get(row, 'GROUPS');
    if (typeof raw === 'string') groups = JSON.parse(raw);
    else if (raw && typeof raw === 'object') groups = raw as Record<string, unknown>;
  } catch { /* ignore */ }

  const groupCount = Object.keys(groups).length;
  // Total hash buckets used (each bucket = 10%)
  const totalBuckets = Object.values(groups).reduce<number>((sum, buckets) => {
    return sum + (Array.isArray(buckets) ? buckets.length : 0);
  }, 0);
  // Comparable if 2+ explicit groups, OR if explicit groups don't cover all 10 buckets
  // (remaining buckets become IMPLICIT_CONTROL in the exposures table)
  const isComparable = groupCount >= 2 || (groupCount === 1 && totalBuckets < 10);

  return {
    id: String(get(row, 'ID')),
    name: String(get(row, 'NAME')),
    type: String(get(row, 'TYPE')),
    status: String(get(row, 'STATUS') || 'running'),
    startTime: get(row, 'START_TIME'),
    endTime: get(row, 'END_TIME'),
    groups,
    exposedCount: Number(get(row, 'EXPOSED_COUNT')) || 0,
    isComparable,
    groupCount,
    totalBuckets,
    chartUrl: (get(row, 'CHART_URL') as string) || null,
  };
}

// ── Statistical output formatters ─────────────────────────────────────────────

/** Format a proportion as a percentage string (e.g. 0.1234 → "12.34%"). */
export function fmt(v: number): string {
  return `${(v * 100).toFixed(2)}%`;
}

/** Format a p-value for display, capping at "< 0.001" for very small values. */
export function fmtP(p: number): string {
  if (p < 0.001) return '< 0.001';
  if (p < 0.01) return p.toFixed(3);
  return p.toFixed(2);
}

/** Return a color string for a test direction: green/red/gray. */
export function dirColor(d: string): string {
  if (d === 'winning') return '#15803d';
  if (d === 'losing') return '#b91c1c';
  return '#6b7280';
}
