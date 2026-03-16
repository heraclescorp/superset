import { isListData, parseListRow, get, fmtNum, fmt, fmtP, dirColor, timeAgo } from '../src/utils';
import { listRows, detailRowsSmall } from './testData';

describe('utils', () => {
  describe('isListData', () => {
    it('returns true when rows contain STATUS column', () => {
      expect(isListData(listRows)).toBe(true);
    });

    it('returns false when rows lack STATUS column', () => {
      expect(isListData(detailRowsSmall)).toBe(false);
    });

    it('returns true for empty array', () => {
      expect(isListData([])).toBe(true);
    });
  });

  describe('get', () => {
    it('returns value for uppercase key', () => {
      expect(get({ NAME: 'test' }, 'NAME')).toBe('test');
    });

    it('falls back to lowercase key', () => {
      expect(get({ name: 'test' }, 'NAME')).toBe('test');
    });

    it('returns empty string for missing key', () => {
      expect(get({}, 'NAME')).toBe('');
    });
  });

  describe('parseListRow', () => {
    it('parses a running experiment with 3 groups', () => {
      const parsed = parseListRow(listRows[0]);
      expect(parsed.id).toBe('101');
      expect(parsed.name).toBe('aprIncrease0217');
      expect(parsed.status).toBe('running');
      expect(parsed.groupCount).toBe(3);
      expect(parsed.totalBuckets).toBe(10);
      expect(parsed.isComparable).toBe(true);
      expect(parsed.exposedCount).toBe(26656);
      expect(parsed.chartUrl).toBe('/superset/explore/?slice_id=500');
    });

    it('parses a single-group experiment (all 10 buckets) as not comparable', () => {
      const parsed = parseListRow(listRows[1]);
      expect(parsed.name).toBe('dummyDebug0226');
      expect(parsed.groupCount).toBe(1);
      expect(parsed.totalBuckets).toBe(10);
      expect(parsed.isComparable).toBe(false);
    });

    it('parses single-group with <10 buckets as comparable (implicit control)', () => {
      const parsed = parseListRow(listRows[2]);
      expect(parsed.name).toBe('fraudCuts0315');
      expect(parsed.groupCount).toBe(1);
      expect(parsed.totalBuckets).toBe(9);
      expect(parsed.isComparable).toBe(true);
    });

    it('parses a completed experiment', () => {
      const parsed = parseListRow(listRows[3]);
      expect(parsed.status).toBe('completed');
      expect(parsed.groupCount).toBe(2);
      expect(parsed.isComparable).toBe(true);
    });

    it('handles invalid GROUPS JSON', () => {
      const row = { ...listRows[0], GROUPS: 'not json' };
      const parsed = parseListRow(row);
      expect(parsed.groupCount).toBe(0);
      expect(parsed.isComparable).toBe(false);
    });
  });

  describe('fmtNum', () => {
    it('formats numbers with locale separators', () => {
      expect(fmtNum(1234567)).toBe('1,234,567');
    });
  });

  describe('fmt', () => {
    it('formats proportion as percentage', () => {
      expect(fmt(0.1234)).toBe('12.34%');
    });
  });

  describe('fmtP', () => {
    it('displays "< 0.001" for tiny p-values', () => {
      expect(fmtP(0.0001)).toBe('< 0.001');
    });

    it('shows 3 decimal places for small p-values', () => {
      expect(fmtP(0.005)).toBe('0.005');
    });

    it('shows 2 decimal places for larger p-values', () => {
      expect(fmtP(0.12)).toBe('0.12');
    });
  });

  describe('dirColor', () => {
    it('returns green for winning', () => {
      expect(dirColor('winning')).toBe('#15803d');
    });

    it('returns red for losing', () => {
      expect(dirColor('losing')).toBe('#b91c1c');
    });

    it('returns gray for inconclusive', () => {
      expect(dirColor('inconclusive')).toBe('#6b7280');
    });
  });

  describe('timeAgo', () => {
    it('returns "—" for null', () => {
      expect(timeAgo(null)).toBe('—');
    });

    it('returns "Today" for now', () => {
      expect(timeAgo(new Date().toISOString())).toBe('Today');
    });

    it('handles epoch timestamps', () => {
      const yesterday = Date.now() - 86400000;
      expect(timeAgo(yesterday)).toBe('Yesterday');
    });
  });
});
