/**
 * Visual confidence interval bar component.
 * Renders a horizontal bar showing the CI range, zero line, and point estimate.
 */

import React from 'react';

/** Render a horizontal CI bar with a zero-line, shaded range, and point-estimate marker. */
export default function CIBar({ ci, center }: { ci: [number, number]; center: number }) {
  // Scale: show range around 0 (the delta), CI is already relative to baseline
  const maxAbs = Math.max(Math.abs(ci[0]), Math.abs(ci[1]), 0.02);
  const scale = (v: number) => 50 + (v / maxAbs) * 45; // 50% = center (0), 5% padding each side
  const leftPct = scale(ci[0]);
  const rightPct = scale(ci[1]);
  const centerPct = scale(center);
  const zeroPct = scale(0);
  const barColor = ci[0] > 0 ? '#22c55e' : ci[1] < 0 ? '#ef4444' : '#94a3b8';

  return (
    <div style={{ position: 'relative', height: 24, background: '#f8fafc', borderRadius: 4, overflow: 'hidden' }}>
      {/* Zero line */}
      <div style={{ position: 'absolute', left: `${zeroPct}%`, top: 0, bottom: 0, width: 1, background: '#cbd5e1', zIndex: 2 }} />
      {/* CI range bar */}
      <div style={{
        position: 'absolute',
        left: `${Math.min(leftPct, rightPct)}%`,
        width: `${Math.abs(rightPct - leftPct)}%`,
        top: 6,
        height: 12,
        background: barColor,
        opacity: 0.25,
        borderRadius: 3,
      }} />
      {/* Point estimate */}
      <div style={{
        position: 'absolute',
        left: `${centerPct}%`,
        top: 6,
        width: 8,
        height: 12,
        marginLeft: -4,
        background: barColor,
        borderRadius: 2,
        zIndex: 3,
      }} />
    </div>
  );
}
