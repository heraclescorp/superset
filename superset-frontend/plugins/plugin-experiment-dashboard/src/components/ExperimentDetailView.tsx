/**
 * Experiment detail view component.
 *
 * Shows a full analysis for a single experiment: SRM check, variant selector,
 * metrics table with confidence intervals, and funnel visualization.
 */

import React, { useMemo, useState } from 'react';
import { fmtNum, fmt, fmtP, dirColor } from '../utils';
import { computeDetailStats, computeAggregatedStats } from '../stats';
import Shell from './Shell';
import CIBar from './CIBar';

export default function ExperimentDetailView({
  rows,
  height,
  metricLabels = [],
  groupbyColumns = [],
  urlParams = {},
}: {
  rows: Record<string, unknown>[];
  height: number;
  metricLabels?: string[];
  groupbyColumns?: string[];
  urlParams?: Record<string, string>;
}) {
  // If we have metricLabels from the control panel, data is pre-aggregated by Snowflake.
  // Otherwise fall back to computing from raw IS_* flag columns.
  const useAggregatedMode = metricLabels.length > 0;

  const stats = useMemo(() => {
    if (useAggregatedMode) {
      return computeAggregatedStats(rows, metricLabels, groupbyColumns, urlParams.alloc);
    }
    return computeDetailStats(rows);
  }, [rows, metricLabels, groupbyColumns, useAggregatedMode, urlParams.alloc]);
  const [selectedVariant, setSelectedVariant] = useState(0);
  const variant = stats.variantMetrics[selectedVariant];

  const thStyle: React.CSSProperties = {
    padding: '8px 10px', fontSize: 11, fontWeight: 600, color: '#9ca3af',
    textTransform: 'uppercase', borderBottom: '2px solid #e5e7eb', textAlign: 'right',
  };
  const tdStyle: React.CSSProperties = {
    padding: '10px 10px', fontSize: 13, borderBottom: '1px solid #f3f4f6',
  };

  return (
    <Shell height={height}>
      <a
        href="/superset/dashboard/experiments-v2/?force=true"
        style={{ fontSize: 13, color: '#6366f1', textDecoration: 'none', display: 'inline-block', marginBottom: 16 }}
      >
        ← Back to experiments
      </a>

      <div style={{ marginBottom: 20 }}>
        <h1 style={{ margin: 0, fontSize: 20, fontWeight: 700 }}>
          Experiment {stats.experimentId}
        </h1>
        <p style={{ margin: '4px 0 0', fontSize: 13, color: '#9ca3af' }}>
          {fmtNum(stats.total)} total exposures · {stats.controlName}: {fmtNum(stats.controlTotal)}
          {stats.variantMetrics.map(v => ` · ${v.name}: ${fmtNum(v.total)}`)}
        </p>
      </div>

      {/* Traffic balance check (SRM = Sample Ratio Mismatch) */}
      {stats.srm && (
        <div style={{
          padding: '12px 16px', borderRadius: 8, marginBottom: 20,
          border: `1px solid ${stats.srm.passed ? '#bbf7d0' : '#fecaca'}`,
          background: stats.srm.passed ? '#f0fdf4' : '#fef2f2',
          fontSize: 13, display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        }}>
          <span>
            <strong>Traffic Balance Check:</strong>{' '}
            {stats.srm.passed ? '✅ Passed' : '⚠️ Failed'}
            {!stats.srm.passed && <span style={{ color: '#b91c1c' }}> — Traffic split may be imbalanced</span>}
          </span>
          <span style={{ color: '#6b7280', fontSize: 12 }}>
            Expected {fmt(stats.srm.expectedRatio)} · Observed {fmt(stats.srm.observedRatio)} · p = {fmtP(stats.srm.pValue)}
          </span>
        </div>
      )}

      {/* Variant selector */}
      {stats.variantMetrics.length > 1 && (
        <div style={{ display: 'flex', gap: 8, alignItems: 'center', marginBottom: 16, padding: 10, background: '#f9fafb', borderRadius: 8 }}>
          <span style={{ fontSize: 12, color: '#6b7280' }}>Compare variant:</span>
          {stats.variantMetrics.map((v, i) => (
            <button key={v.name} onClick={() => setSelectedVariant(i)} style={{
              padding: '4px 12px', borderRadius: 6, fontSize: 12, fontWeight: 500, cursor: 'pointer',
              background: selectedVariant === i ? '#6366f1' : '#fff',
              color: selectedVariant === i ? '#fff' : '#374151',
              border: `1px solid ${selectedVariant === i ? '#6366f1' : '#e5e7eb'}`,
            }}>
              {v.name} (n={fmtNum(v.total)})
            </button>
          ))}
        </div>
      )}

      {/* Conversion metrics table with CI bars */}
      {variant && variant.metrics.length > 0 && (
        <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 12, overflow: 'hidden' }}>
          <div style={{ padding: '16px 16px 8px', borderBottom: '1px solid #f3f4f6' }}>
            <h3 style={{ margin: 0, fontSize: 15, fontWeight: 600 }}>Conversion Metrics</h3>
            <p style={{ margin: '2px 0 0', fontSize: 12, color: '#9ca3af' }}>
              {stats.controlName} vs {variant.name} · 95% Confidence Intervals
            </p>
          </div>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr>
                <th style={{ ...thStyle, textAlign: 'left' }}>Stage</th>
                <th style={thStyle}>{stats.controlName}</th>
                <th style={thStyle}>{variant.name}</th>
                <th style={thStyle}>Δ Change</th>
                <th style={thStyle}>P-value</th>
                <th style={{ ...thStyle, width: '20%' }}>95% CI</th>
                <th style={thStyle}>Status</th>
              </tr>
            </thead>
            <tbody>
              {variant.metrics.map(m => {
                const rowBg = m.direction === 'winning' ? '#f0fdf4' : m.direction === 'losing' ? '#fef2f2' : 'transparent';
                return (
                  <tr key={m.stage} style={{ background: rowBg }}>
                    <td style={{ ...tdStyle, textAlign: 'left', fontWeight: 500 }}>{m.label}</td>
                    <td style={{ ...tdStyle, textAlign: 'right', fontFamily: 'monospace' }}>
                      {fmt(m.controlRate)}
                      <div style={{ fontSize: 10, color: '#9ca3af' }}>n={fmtNum(m.controlCount)}</div>
                    </td>
                    <td style={{ ...tdStyle, textAlign: 'right', fontFamily: 'monospace' }}>
                      {fmt(m.variantRate)}
                      <div style={{ fontSize: 10, color: '#9ca3af' }}>n={fmtNum(m.variantCount)}</div>
                    </td>
                    <td style={{ ...tdStyle, textAlign: 'right', fontFamily: 'monospace', color: dirColor(m.direction), fontWeight: 600 }}>
                      {m.delta > 0 ? '+' : ''}{fmt(m.delta)}
                      <div style={{ fontSize: 10, color: '#9ca3af', fontWeight: 400 }}>
                        ({m.relativeDelta > 0 ? '+' : ''}{(m.relativeDelta * 100).toFixed(1)}% rel)
                      </div>
                    </td>
                    <td style={{ ...tdStyle, textAlign: 'right', fontFamily: 'monospace', fontWeight: m.pValue < 0.05 ? 700 : 400 }}>
                      {fmtP(m.pValue)}
                    </td>
                    <td style={{ ...tdStyle, padding: '10px 12px' }}>
                      <CIBar ci={m.ci95} center={m.delta} />
                      <div style={{ fontSize: 10, color: '#9ca3af', textAlign: 'center', marginTop: 2 }}>
                        [{fmt(m.ci95[0])}, {fmt(m.ci95[1])}]
                      </div>
                    </td>
                    <td style={{ ...tdStyle, textAlign: 'center' }}>
                      <span style={{
                        padding: '2px 8px', borderRadius: 9999, fontSize: 11, fontWeight: 600,
                        background: m.direction === 'winning' ? '#dcfce7' : m.direction === 'losing' ? '#fee2e2' : '#f3f4f6',
                        color: dirColor(m.direction),
                      }}>
                        {m.direction.charAt(0).toUpperCase() + m.direction.slice(1)}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* Funnel visualization */}
      {variant && variant.metrics.length > 0 && (
        <div style={{ marginTop: 24, background: '#fff', border: '1px solid #e5e7eb', borderRadius: 12, padding: 20 }}>
          <h3 style={{ margin: '0 0 16px', fontSize: 15, fontWeight: 600 }}>Funnel</h3>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            {variant.metrics.map((m, idx) => {
              const maxCount = Math.max(...variant.metrics.map(s => Math.max(s.controlCount, s.variantCount)));
              const ctrlW = maxCount > 0 ? (m.controlCount / maxCount) * 100 : 0;
              const varW = maxCount > 0 ? (m.variantCount / maxCount) * 100 : 0;
              return (
                <div key={m.stage}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                    <span style={{ fontSize: 12 }}><b>{idx + 1}.</b> {m.label}</span>
                    <span style={{ fontSize: 12, color: '#6b7280' }}>
                      <span style={{ color: '#3b82f6' }}>{fmtNum(m.controlCount)}</span>
                      {' vs '}
                      <span style={{ color: '#8b5cf6' }}>{fmtNum(m.variantCount)}</span>
                      {m.delta !== 0 && (
                        <span style={{ marginLeft: 8, color: dirColor(m.direction), fontWeight: 600 }}>
                          {m.relativeDelta > 0 ? '+' : ''}{(m.relativeDelta * 100).toFixed(1)}%
                        </span>
                      )}
                    </span>
                  </div>
                  {[
                    { label: stats.controlName, w: ctrlW, color: '#3b82f6' },
                    { label: variant.name, w: varW, color: '#8b5cf6' },
                  ].map(bar => (
                    <div key={bar.label} style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 2 }}>
                      <span style={{ fontSize: 10, color: '#9ca3af', width: 60, textAlign: 'right' }}>{bar.label}</span>
                      <div style={{ flex: 1, height: 16, background: '#f3f4f6', borderRadius: 8, overflow: 'hidden' }}>
                        <div style={{ width: `${bar.w}%`, height: '100%', background: bar.color, borderRadius: 8, transition: 'width 0.4s' }} />
                      </div>
                    </div>
                  ))}
                </div>
              );
            })}
          </div>
          <div style={{ display: 'flex', gap: 16, marginTop: 12, fontSize: 11, color: '#9ca3af' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
              <div style={{ width: 12, height: 4, background: '#3b82f6', borderRadius: 2 }} />
              {stats.controlName}
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
              <div style={{ width: 12, height: 4, background: '#8b5cf6', borderRadius: 2 }} />
              {variant.name}
            </div>
          </div>
        </div>
      )}

      {rows.length === 0 && (
        <div style={{ textAlign: 'center', padding: 48, color: '#9ca3af' }}>
          <div style={{ fontSize: 32, marginBottom: 12 }}>🔬</div>
          <div style={{ fontWeight: 500, color: '#374151', marginBottom: 8 }}>No experiment selected</div>
          <div>
            Navigate from the <a href="/superset/dashboard/experiments-v2/" style={{ color: '#6366f1' }}>experiment list</a>,
            or add an <strong>EXPERIMENT_SPEC_ID</strong> filter in the control panel.
          </div>
          <div style={{ marginTop: 8, fontSize: 12 }}>
            Use <strong>Save As</strong> to create a saved view for a specific experiment.
          </div>
        </div>
      )}

      {rows.length > 0 && (!variant || variant.metrics.length === 0) && (
        <div style={{ textAlign: 'center', padding: 48, color: '#9ca3af' }}>
          {stats.groupNames.length < 2
            ? `Only one group found (${stats.groupNames[0] || '?'}). Need at least a control and variant to compare.`
            : 'No metric data available for this experiment.'}
        </div>
      )}
    </Shell>
  );
}
