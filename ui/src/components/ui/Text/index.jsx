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
  typography,
  shadow,
} from 'styled-system';

const SystemText = styled.p`
  ${compose(space, color, layout, flexbox, grid, border, position, shadow, typography)}
`;

export const Text = ({children, heading, ...passThroughProps}) => {
  const textProps = {
    as: heading ? `h${heading}` : 'p',
  };
  return (
    <SystemText {...textProps} {...passThroughProps}>
      {children}
    </SystemText>
  );
};
