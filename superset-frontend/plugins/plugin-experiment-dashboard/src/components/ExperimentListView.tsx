/**
 * Experiment list view component.
 *
 * Displays a searchable table of experiments with status, group allocation,
 * exposure counts, and navigation to the detail view for comparable experiments.
 */

import React, { useMemo, useState } from 'react';
import { ListExperiment } from '../types';
import { fmtNum, timeAgo, parseListRow } from '../utils';
import Shell from './Shell';

export default function ExperimentListView({
  rows,
  height,
}: {
  rows: Record<string, unknown>[];
  height: number;
}) {
  const [search, setSearch] = useState('');
  const experiments = useMemo(() => rows.map(parseListRow), [rows]);
  const filtered = useMemo(
    () =>
      experiments.filter(
        e =>
          e.name.toLowerCase().includes(search.toLowerCase()) ||
          e.id.includes(search),
      ),
    [experiments, search],
  );

  const handleSelect = (exp: ListExperiment) => {
    if (!exp.chartUrl) return;
    window.location.href = exp.chartUrl;
  };

  const thStyle: React.CSSProperties = {
    padding: '8px 12px',
    fontSize: 11,
    fontWeight: 600,
    textTransform: 'uppercase',
    color: '#9ca3af',
    borderBottom: '2px solid #e5e7eb',
    textAlign: 'left',
  };
  const tdStyle: React.CSSProperties = {
    padding: '10px 12px',
    borderBottom: '1px solid #f3f4f6',
    fontSize: 13,
    textAlign: 'left',
  };

  return (
    <Shell height={height}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 20 }}>
        <span style={{ fontSize: 22 }}>🧪</span>
        <div>
          <h1 style={{ margin: 0, fontSize: 18, fontWeight: 700 }}>Experiments</h1>
          <p style={{ margin: 0, fontSize: 12, color: '#9ca3af' }}>
            {experiments.length} experiments · {experiments.filter(e => e.chartUrl).length} with charts · Click to view details
          </p>
        </div>
      </div>

      <input
        type="text"
        placeholder="Search experiments…"
        value={search}
        onChange={e => setSearch(e.target.value)}
        style={{
          width: '100%',
          padding: '8px 12px',
          border: '1px solid #e5e7eb',
          borderRadius: 8,
          fontSize: 13,
          marginBottom: 16,
          boxSizing: 'border-box',
        }}
      />

      <table style={{ width: '100%', borderCollapse: 'collapse' }}>
        <thead>
          <tr>
            <th style={thStyle}>Experiment</th>
            <th style={thStyle}>Status</th>
            <th style={thStyle}>Exposed</th>
            <th style={thStyle}>Groups</th>
            <th style={thStyle}>Started</th>
          </tr>
        </thead>
        <tbody>
          {filtered.map(exp => (
            <tr
              key={exp.id}
              onClick={() => handleSelect(exp)}
              style={{
                cursor: exp.chartUrl ? 'pointer' : 'default',
                opacity: exp.chartUrl ? 1 : 0.55,
              }}
              onMouseEnter={e => { if (exp.chartUrl) e.currentTarget.style.background = '#f9fafb'; }}
              onMouseLeave={e => (e.currentTarget.style.background = '')}
            >
              <td style={tdStyle}>
                <div style={{ fontWeight: 500, color: exp.chartUrl ? '#6366f1' : '#9ca3af' }}>{exp.name}</div>
                <div style={{ fontSize: 11, color: '#9ca3af' }}>
                  {exp.type.replace(/_/g, ' ')} · ID: {exp.id}
                </div>
              </td>
              <td style={tdStyle}>
                <span
                  style={{
                    padding: '2px 8px',
                    borderRadius: 9999,
                    fontSize: 11,
                    fontWeight: 600,
                    background:
                      exp.status === 'running'
                        ? '#dbeafe'
                        : exp.status === 'completed'
                        ? '#dcfce7'
                        : '#f3f4f6',
                    color:
                      exp.status === 'running'
                        ? '#1d4ed8'
                        : exp.status === 'completed'
                        ? '#15803d'
                        : '#374151',
                  }}
                >
                  {exp.status}
                </span>
              </td>
              <td style={{ ...tdStyle, fontFamily: 'monospace' }}>
                {fmtNum(exp.exposedCount)}
              </td>
              <td style={{ ...tdStyle, fontSize: 12 }}>
                {exp.isComparable ? (
                  <span style={{ color: '#374151' }}>
                    {Object.entries(exp.groups).map(([name, buckets]) => {
                      const pct = Array.isArray(buckets) ? buckets.length * 10 : '?';
                      return `${name}: ${pct}%`;
                    }).join(' / ')}
                    {exp.totalBuckets < 10 && ` / control: ${(10 - exp.totalBuckets) * 10}%`}
                  </span>
                ) : (
                  <span style={{ color: '#9ca3af' }}>
                    100% → {Object.keys(exp.groups)[0] || '—'}
                  </span>
                )}
              </td>
              <td style={{ ...tdStyle, color: '#9ca3af' }}>
                {timeAgo(exp.startTime)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      {filtered.length === 0 && (
        <div style={{ textAlign: 'center', padding: 32, color: '#9ca3af' }}>
          No experiments match your search.
        </div>
      )}
    </Shell>
  );
}
