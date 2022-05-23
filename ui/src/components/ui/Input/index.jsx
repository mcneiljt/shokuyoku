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

export const Input = styled.input`
  ${compose(space, color, layout, flexbox, grid, border, position, shadow, typography)}
`;
