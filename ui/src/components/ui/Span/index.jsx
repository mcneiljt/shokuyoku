import styled from 'styled-components';
import {
  compose,
  space,
  color,
  layout,
  flexbox,
  grid,
  border,
  position,
  shadow,
  typography,
} from 'styled-system';

export const Span = styled.span`
  ${compose(space, color, layout, flexbox, grid, border, position, shadow, typography)}
`;
