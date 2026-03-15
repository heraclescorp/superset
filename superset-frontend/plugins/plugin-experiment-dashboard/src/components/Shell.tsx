/**
 * Styled outer shell container for the experiment dashboard.
 * Provides consistent height, scrolling, typography, and background.
 */

import { styled } from '@superset-ui/core';

const Shell = styled.div<{ height: number }>`
  height: ${({ height }) => height}px;
  overflow-y: auto;
  font-family: ${({ theme }) => theme.typography.families.sansSerif};
  font-size: 14px;
  color: ${({ theme }) => theme.colors.grayscale.dark2};
  background: ${({ theme }) => theme.colors.grayscale.light5};
  padding: 16px;
  box-sizing: border-box;
`;

export default Shell;
