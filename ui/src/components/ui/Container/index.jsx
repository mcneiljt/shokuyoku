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

export const Container = styled.div`
  ${compose(space, color, layout, flexbox, grid, border, position, shadow, typography)}
`;
