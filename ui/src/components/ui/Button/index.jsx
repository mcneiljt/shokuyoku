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
  variant,
  system,
} from 'styled-system';

export const Button = styled.button`
  ${compose(space, color, layout, flexbox, grid, border, position, shadow, typography)}
  ${variant({
    variants: {
      primary: {
        backgroundColor: 'buttonBackground',
      },
      default: {
        backgroundColor: 'green',
      },
    },
  })}
  ${system({
    danger: {
      property: 'backgroundColor',
      cssProperty: 'backgroundColor',
      transform: () => 'red',
    },
  })}
`;

Button.defaultProps = {
  variant: 'default',
};
