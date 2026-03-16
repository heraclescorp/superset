/**
 * Statistical computation functions for the experiment dashboard.
 *
 * Contains normal distribution utilities, two-proportion z-tests, and the
 * two compute pipelines (raw row-level and pre-aggregated metric modes).
 */

import { FunnelMetric, SrmResult } from './types';
import { get } from './utils';

// ── Distribution helpers ──────────────────────────────────────────────────────

/** Approximation of the standard normal CDF using Horner's method. */
export function normalCdf(x: number): number {
  const a1 = 0.254829592;
  const a2 = -0.284496736;
  const a3 = 1.421413741;
  const a4 = -1.453152027;
  const a5 = 1.061405429;
  const p = 0.3275911;
  const sign = x < 0 ? -1 : 1;
  const ax = Math.abs(x);
  const t = 1.0 / (1.0 + p * ax);
  const y =
    1.0 -
    ((((a5 * t + a4) * t + a3) * t + a2) * t + a1) *
      t *
      Math.exp((-ax * ax) / 2);
  return 0.5 * (1.0 + sign * y);
}

/** Rational approximation of the inverse standard normal CDF (quantile function). */
export function normalInvCdf(p: number): number {
  if (p <= 0) return -Infinity;
  if (p >= 1) return Infinity;
  const c = [
    -7.784894002430293e-3, -3.223964580411365e-1, -2.400758277161838,
    -2.549732539343734, 4.374664141464968, 2.938163982698783,
  ];
  const d = [
    7.784695709041462e-3, 3.224671290700398e-1, 2.445134137142996,
    3.754408661907416,
  ];
  const a = [
    -3.969683028665376e1, 2.209460984245205e2, -2.759285104469687e2,
    1.38357751867269e2, -3.066479806614716e1, 2.506628277459239,
  ];
  const b = [
    -5.447609879822406e1, 1.615858368580409e2, -1.556989798598866e2,
    6.680131188771972e1, -1.328068155288572e1,
  ];
  const pLow = 0.02425;
  if (p < pLow) {
    const q = Math.sqrt(-2 * Math.log(p));
    return (
      (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) /
      ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    );
  }
  if (p <= 1 - pLow) {
    const q = p - 0.5;
    const r = q * q;
    return (
      ((((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) *
        q) /
      (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)
    );
  }
  const q = Math.sqrt(-2 * Math.log(1 - p));
  return -(
    (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) /
    ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
  );
}

// ── Funnel stage definitions matching the exposure dataset IS_* columns ──────

/**
 * Chi-squared survival function (1 - CDF) via the regularized lower
 * incomplete gamma function.  Works for any degrees of freedom >= 1.
 *
 * Uses the series expansion of the lower gamma: P(a,x) = Σ x^n / Γ(a+n+1).
 */
export function chiSquaredPValue(chiSq: number, df: number): number {
  if (chiSq <= 0 || df <= 0) return 1;
  const a = df / 2;
  const x = chiSq / 2;
  // Log-gamma via Lanczos approximation
  function logGamma(z: number): number {
    const g = 7;
    const coef = [
      0.99999999999980993, 676.5203681218851, -1259.1392167224028,
      771.32342877765313, -176.61502916214059, 12.507343278686905,
      -0.13857109526572012, 9.9843695780195716e-6, 1.5056327351493116e-7,
    ];
    if (z < 0.5)
      return (
        Math.log(Math.PI / Math.sin(Math.PI * z)) - logGamma(1 - z)
      );
    const zz = z - 1;
    let s = coef[0];
    for (let i = 1; i < g + 2; i++) s += coef[i] / (zz + i);
    const t = zz + g + 0.5;
    return 0.5 * Math.log(2 * Math.PI) + (zz + 0.5) * Math.log(t) - t + Math.log(s);
  }
  // Regularized lower incomplete gamma via series expansion
  let sum = 0;
  let term = 1 / a;
  sum = term;
  for (let n = 1; n < 200; n++) {
    term *= x / (a + n);
    sum += term;
    if (Math.abs(term) < 1e-12 * Math.abs(sum)) break;
  }
  const lowerP = Math.exp(a * Math.log(x) - x - logGamma(a)) * sum;
  return Math.max(0, Math.min(1, 1 - lowerP));
}

export const FUNNEL_STAGES = [
  { key: 'IS_LEAD', label: 'Lead' },
  { key: 'IS_PII', label: 'PII Submitted' },
  { key: 'IS_PQ', label: 'Pre-Qualification' },
  { key: 'IS_PQA', label: 'PQ Accepted' },
  { key: 'IS_OFFER', label: 'Offer Preview' },
  { key: 'IS_ACCEPT', label: 'Offer Accepted' },
  { key: 'IS_SCHED', label: 'Notary Scheduled' },
  { key: 'IS_NOTARY_STARTED', label: 'Notary Started' },
  { key: 'IS_BOOKED', label: 'Booked' },
] as const;

// ── Two-proportion z-test ─────────────────────────────────────────────────────

/** Run a two-proportion z-test comparing variant (v/vN) against control (c/cN). */
export function proportionZTest(c: number, cN: number, v: number, vN: number) {
  const p1 = cN > 0 ? c / cN : 0;
  const p2 = vN > 0 ? v / vN : 0;
  const pooled = cN + vN > 0 ? (c + v) / (cN + vN) : 0;
  const se = Math.sqrt(
    pooled * (1 - pooled) * (1 / Math.max(cN, 1) + 1 / Math.max(vN, 1)),
  );
  const z = se > 0 ? (p2 - p1) / se : 0;
  const pValue = 2 * (1 - normalCdf(Math.abs(z)));
  const zCrit = normalInvCdf(0.975);
  const seDiff = Math.sqrt(
    (p1 * (1 - p1)) / Math.max(cN, 1) + (p2 * (1 - p2)) / Math.max(vN, 1),
  );
  const diff = p2 - p1;
  const ci: [number, number] = [diff - zCrit * seDiff, diff + zCrit * seDiff];
  const isSignificant = pValue < 0.05;
  const direction: 'winning' | 'losing' | 'inconclusive' = !isSignificant
    ? 'inconclusive'
    : diff > 0
      ? 'winning'
      : 'losing';
  return {
    p1,
    p2,
    diff,
    relativeDelta: p1 > 0 ? diff / p1 : 0,
    pValue,
    ci,
    isSignificant,
    direction,
  };
}

// ── Compute pipeline: raw row-level data ──────────────────────────────────────

/** Compute funnel metrics & SRM from individual exposure rows (IS_* flag columns). */
export function computeDetailStats(rows: Record<string, unknown>[]) {
  // Separate rows by group
  const groupRows = new Map<string, Record<string, unknown>[]>();
  rows.forEach(row => {
    const group = String(get(row, 'EXPERIMENT_GROUP'));
    if (!groupRows.has(group)) groupRows.set(group, []);
    groupRows.get(group)!.push(row);
  });

  const groupNames = Array.from(groupRows.keys()).sort();
  const controlName =
    groupNames.find(n => /control/i.test(n)) || groupNames[0] || 'control';
  const variantNames = groupNames.filter(n => n !== controlName);
  const controlRows = groupRows.get(controlName) || [];
  const controlTotal = controlRows.length;

  // Compute funnel metrics for each variant vs control
  const variantMetrics = variantNames.map(vName => {
    const vRows = groupRows.get(vName) || [];
    const vTotal = vRows.length;
    const metrics: FunnelMetric[] = FUNNEL_STAGES.map(stage => {
      const cCount = controlRows.filter(
        r => Number(get(r, stage.key)) === 1,
      ).length;
      const vCount = vRows.filter(r => Number(get(r, stage.key)) === 1).length;
      const test = proportionZTest(cCount, controlTotal, vCount, vTotal);
      return {
        stage: stage.key,
        label: stage.label,
        controlCount: cCount,
        variantCount: vCount,
        controlTotal,
        variantTotal: vTotal,
        controlRate: test.p1,
        variantRate: test.p2,
        delta: test.diff,
        relativeDelta: test.relativeDelta,
        pValue: test.pValue,
        ci95: test.ci,
        direction: test.direction,
        isSignificant: test.isSignificant,
      };
    }).filter(m => m.controlCount > 0 || m.variantCount > 0);
    return { name: vName, total: vTotal, metrics };
  });

  // SRM check
  let srm: SrmResult | null = null;
  if (groupNames.length >= 2) {
    const total = rows.length;
    const expected = total / groupNames.length;
    const chiSq = groupNames.reduce((s, name) => {
      const obs = groupRows.get(name)?.length || 0;
      return s + Math.pow(obs - expected, 2) / Math.max(expected, 1);
    }, 0);
    const pValue = chiSquaredPValue(chiSq, groupNames.length - 1);
    srm = {
      passed: pValue > 0.01,
      pValue,
      chiSq,
      expectedRatio: 1 / groupNames.length,
      observedRatio: controlTotal / total,
    };
  }

  const experimentId =
    rows.length > 0 ? String(get(rows[0], 'EXPERIMENT_SPEC_ID')) : '—';
  return {
    groupNames,
    controlName,
    controlTotal,
    variantMetrics,
    srm,
    experimentId,
    total: rows.length,
  };
}

// ── Compute pipeline: pre-aggregated metrics ──────────────────────────────────

/**
 * Compute stats from pre-aggregated rows (Snowflake grouped by EXPERIMENT_GROUP).
 * Each row has metric columns like UNQ_LEAD_#, UNQ_PQ_#. Returns same shape as computeDetailStats.
 */
export function computeAggregatedStats(
  rows: Record<string, unknown>[],
  metricLabels: string[],
  groupbyColumns: string[],
  allocParam?: string,
) {
  const groupCol =
    groupbyColumns[0] ||
    Object.keys(rows[0] || {}).find(k => !metricLabels.includes(k)) ||
    '';
  const groupMap = new Map<string, Record<string, unknown>>();
  rows.forEach(row => {
    const group = String(row[groupCol] ?? '');
    groupMap.set(group, row);
  });

  const groupNames = Array.from(groupMap.keys()).sort();
  const controlName =
    groupNames.find(n => /control/i.test(n)) || groupNames[0] || 'control';
  const controlRow = groupMap.get(controlName) || {};

  // Find total metric — use the first count metric (ends with _#) as proxy for group size
  const totalMetric =
    metricLabels.find(m => /total|count|exposed|assigned/i.test(m)) ||
    metricLabels.find(m => m.endsWith('_#')) ||
    metricLabels[0];
  const controlTotal = Number(controlRow[totalMetric] || 0);

  // SRM check — use expected allocation from URL if available
  let srm: SrmResult | null = null;
  if (groupNames.length >= 2 && totalMetric) {
    const totals = groupNames.map(n =>
      Number(groupMap.get(n)?.[totalMetric] || 0),
    );
    const grandTotal = totals.reduce((s, v) => s + v, 0);

    // Parse allocation from chart params (set by pipeline at chart creation)
    const alloc = allocParam || null;
    const expectedBuckets = new Map<string, number>();
    if (alloc) {
      let usedBuckets = 0;
      alloc.split(',').forEach(part => {
        const [name, count] = part.split(':');
        if (name && count) {
          expectedBuckets.set(name, Number(count));
          usedBuckets += Number(count);
        }
      });
      // Implicit control gets remaining buckets
      if (usedBuckets < 10) {
        const implicitName = groupNames.find(n => !expectedBuckets.has(n));
        if (implicitName) expectedBuckets.set(implicitName, 10 - usedBuckets);
      }
    }

    const expectedTotals = groupNames.map(n => {
      const buckets = expectedBuckets.get(n);
      if (buckets !== undefined) return grandTotal * (buckets / 10);
      return grandTotal / groupNames.length; // fallback to equal split
    });

    const chiSq = totals.reduce((s, obs, i) => {
      const exp = Math.max(expectedTotals[i], 1);
      return s + Math.pow(obs - exp, 2) / exp;
    }, 0);
    const pValue = chiSquaredPValue(chiSq, groupNames.length - 1);
    const controlIdx = groupNames.indexOf(controlName);
    const expectedControlRatio =
      expectedTotals[controlIdx] / Math.max(grandTotal, 1);
    srm = {
      passed: pValue > 0.01,
      pValue,
      chiSq,
      expectedRatio: expectedControlRatio,
      observedRatio: controlTotal / Math.max(grandTotal, 1),
    };
  }

  // Build variant metrics
  const variantMetrics = groupNames
    .filter(n => n !== controlName)
    .map(vName => {
      const vRow = groupMap.get(vName) || {};
      const vTotal = Number(vRow[totalMetric] || 0);
      const metrics = metricLabels
        .filter(m => m !== totalMetric)
        .map(metricKey => {
          const cVal = Number(controlRow[metricKey] || 0);
          const vVal = Number(vRow[metricKey] || 0);
          const label = metricKey
            .replace(/^UNQ_/i, '')
            .replace(/_#$/i, '')
            .replace(/_/g, ' ');

          // Auto-detect metric type from naming convention
          const isRateMetric =
            metricKey.includes('%') || metricKey.includes('RATE');
          const isAvgMetric =
            metricKey.startsWith('Avg ') || metricKey.startsWith('AVG ');
          if (isRateMetric) {
            // Ratio metrics: values are proportions (0-1 from DIV0).
            // Use normal approximation CI for difference of proportions.
            const diff = vVal - cVal;
            const cSE = Math.sqrt(
              Math.max(cVal * (1 - cVal), 0) / Math.max(controlTotal, 1),
            );
            const vSE = Math.sqrt(
              Math.max(vVal * (1 - vVal), 0) / Math.max(vTotal, 1),
            );
            const seDiff = Math.sqrt(cSE * cSE + vSE * vSE);
            const zCrit = normalInvCdf(0.975);
            const ci: [number, number] = [
              diff - zCrit * seDiff,
              diff + zCrit * seDiff,
            ];
            const z = seDiff > 0 ? Math.abs(diff) / seDiff : 0;
            const pValue = seDiff > 0 ? 2 * (1 - normalCdf(z)) : 1;
            const isSignificant = pValue < 0.05;
            return {
              stage: metricKey,
              label,
              controlCount: cVal,
              variantCount: vVal,
              controlTotal,
              variantTotal: vTotal,
              controlRate: cVal,
              variantRate: vVal,
              delta: diff,
              relativeDelta: cVal > 0 ? diff / cVal : 0,
              pValue,
              ci95: ci,
              direction: (!isSignificant
                ? 'inconclusive'
                : diff > 0
                  ? 'winning'
                  : 'losing') as 'winning' | 'losing' | 'inconclusive',
              isSignificant,
            };
          }

          if (isAvgMetric) {
            // Average metrics: show raw difference. CI requires stddev which
            // we don't have from the aggregated query, so mark inconclusive.
            const diff = vVal - cVal;
            return {
              stage: metricKey,
              label,
              controlCount: cVal,
              variantCount: vVal,
              controlTotal,
              variantTotal: vTotal,
              controlRate: cVal,
              variantRate: vVal,
              delta: diff,
              relativeDelta: cVal > 0 ? diff / cVal : 0,
              pValue: 1,
              ci95: [diff, diff] as [number, number],
              direction: 'inconclusive' as const,
              isSignificant: false,
            };
          }

          // Count metrics: proportion z-test (count / total)
          const test = proportionZTest(cVal, controlTotal, vVal, vTotal);
          return {
            stage: metricKey,
            label,
            controlCount: cVal,
            variantCount: vVal,
            controlTotal,
            variantTotal: vTotal,
            controlRate: test.p1,
            variantRate: test.p2,
            delta: test.diff,
            relativeDelta: test.relativeDelta,
            pValue: test.pValue,
            ci95: test.ci,
            direction: test.direction,
            isSignificant: test.isSignificant,
          };
        })
        .filter(m => m.controlCount > 0 || m.variantCount > 0);

      return { name: vName, total: vTotal, metrics };
    });

  // Experiment ID is shown in the chart title (baked in by pipeline)
  const experimentId = '—';

  return {
    groupNames,
    controlName,
    controlTotal,
    variantMetrics,
    srm,
    experimentId,
    total: rows.length,
  };
}
