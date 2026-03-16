/**
 * Unit tests for the statistical engine (stats.ts).
 *
 * Covers normal distribution helpers, chi-squared CDF, proportion z-tests,
 * and the SRM check logic.
 */
import {
  normalCdf,
  normalInvCdf,
  chiSquaredPValue,
  proportionZTest,
} from '../src/stats';

// ── normalCdf ─────────────────────────────────────────────────────────────────

describe('normalCdf', () => {
  it('returns ~0.5 at z=0', () => {
    expect(normalCdf(0)).toBeCloseTo(0.5, 5);
  });

  it('returns ~0.84 at z=1', () => {
    expect(normalCdf(1)).toBeCloseTo(0.8413, 1);
  });

  it('returns ~0.98 at z=2', () => {
    expect(normalCdf(2)).toBeCloseTo(0.9772, 1);
  });

  it('is symmetric: CDF(-x) ≈ 1 - CDF(x)', () => {
    expect(normalCdf(-2) + normalCdf(2)).toBeCloseTo(1, 1);
  });

  it('approaches 1 for large positive z', () => {
    expect(normalCdf(6)).toBeGreaterThan(0.999999);
  });

  it('approaches 0 for large negative z', () => {
    expect(normalCdf(-6)).toBeLessThan(0.000001);
  });
});

// ── normalInvCdf ──────────────────────────────────────────────────────────────

describe('normalInvCdf', () => {
  it('returns 0 at p=0.5', () => {
    expect(normalInvCdf(0.5)).toBeCloseTo(0, 5);
  });

  it('returns ~1.96 at p=0.975', () => {
    expect(normalInvCdf(0.975)).toBeCloseTo(1.96, 2);
  });

  it('returns ~-1.96 at p=0.025', () => {
    expect(normalInvCdf(0.025)).toBeCloseTo(-1.96, 2);
  });

  it('round-trips with normalCdf', () => {
    for (const p of [0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99]) {
      expect(normalCdf(normalInvCdf(p))).toBeCloseTo(p, 1);
    }
  });

  it('returns -Infinity at p=0', () => {
    expect(normalInvCdf(0)).toBe(-Infinity);
  });

  it('returns Infinity at p=1', () => {
    expect(normalInvCdf(1)).toBe(Infinity);
  });
});

// ── chiSquaredPValue ──────────────────────────────────────────────────────────

describe('chiSquaredPValue', () => {
  it('returns 1 for chiSq=0', () => {
    expect(chiSquaredPValue(0, 1)).toBe(1);
  });

  it('df=1: chi2=3.84 gives p≈0.05', () => {
    expect(chiSquaredPValue(3.841, 1)).toBeCloseTo(0.05, 2);
  });

  it('df=1: chi2=6.63 gives p≈0.01', () => {
    expect(chiSquaredPValue(6.635, 1)).toBeCloseTo(0.01, 2);
  });

  it('df=2: chi2=5.99 gives p≈0.05', () => {
    expect(chiSquaredPValue(5.991, 2)).toBeCloseTo(0.05, 2);
  });

  it('df=2: chi2=9.21 gives p≈0.01', () => {
    expect(chiSquaredPValue(9.21, 2)).toBeCloseTo(0.01, 2);
  });

  it('df=3: chi2=7.81 gives p≈0.05', () => {
    expect(chiSquaredPValue(7.815, 3)).toBeCloseTo(0.05, 2);
  });

  it('df=5: chi2=11.07 gives p≈0.05', () => {
    expect(chiSquaredPValue(11.07, 5)).toBeCloseTo(0.05, 2);
  });

  it('large chiSq gives p near 0', () => {
    expect(chiSquaredPValue(100, 1)).toBeLessThan(0.0001);
  });
});

// ── proportionZTest ───────────────────────────────────────────────────────────

describe('proportionZTest', () => {
  it('equal proportions → inconclusive with p≈1', () => {
    const result = proportionZTest(50, 1000, 50, 1000);
    expect(result.diff).toBeCloseTo(0);
    expect(result.pValue).toBeGreaterThan(0.5);
    expect(result.direction).toBe('inconclusive');
  });

  it('detects significant winning variant', () => {
    // Control: 5%, Variant: 8%
    const result = proportionZTest(50, 1000, 80, 1000);
    expect(result.p1).toBeCloseTo(0.05);
    expect(result.p2).toBeCloseTo(0.08);
    expect(result.diff).toBeCloseTo(0.03);
    expect(result.pValue).toBeLessThan(0.05);
    expect(result.direction).toBe('winning');
    expect(result.isSignificant).toBe(true);
  });

  it('detects significant losing variant', () => {
    // Control: 8%, Variant: 5%
    const result = proportionZTest(80, 1000, 50, 1000);
    expect(result.pValue).toBeLessThan(0.05);
    expect(result.direction).toBe('losing');
  });

  it('small sample → inconclusive', () => {
    const result = proportionZTest(1, 10, 2, 10);
    expect(result.direction).toBe('inconclusive');
  });

  it('handles zero denominators', () => {
    const result = proportionZTest(0, 0, 0, 0);
    expect(result.p1).toBe(0);
    expect(result.p2).toBe(0);
    expect(result.pValue).toBeGreaterThanOrEqual(0);
  });

  it('CI contains the true difference', () => {
    const result = proportionZTest(50, 1000, 80, 1000);
    expect(result.ci[0]).toBeLessThan(result.diff);
    expect(result.ci[1]).toBeGreaterThan(result.diff);
  });

  it('relativeDelta is correct', () => {
    const result = proportionZTest(50, 1000, 80, 1000);
    expect(result.relativeDelta).toBeCloseTo(0.03 / 0.05, 2);
  });
});
