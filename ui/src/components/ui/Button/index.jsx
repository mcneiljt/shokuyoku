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
import css from '@styled-system/css';

export const Button = styled.button`
  ${compose(space, color, layout, flexbox, grid, border, position, shadow, typography)}
  ${variant({
    variants: {
      primary: {
        backgroundColor: 'buttonBackground',
      },
      default: {
        backgroundColor: 'white',
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
  ${css({
    border: '1px solid gainsboro',
    borderRadius: 'small',
    fontSize: 'small',
    height: '23px',
    textTransform: 'lowercase',
    lineHeight: 1,
    ':disabled': {
      opacity: '50%',
    },
  })}
`;

Button.defaultProps = {
  variant: 'default',
};
