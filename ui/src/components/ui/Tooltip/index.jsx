import * as RadixTooltip from '@radix-ui/react-tooltip';
import styled from 'styled-components';

const StyledContent = styled(RadixTooltip.Content)({
  borderRadius: 4,
  padding: '10px 15px',
  fontSize: 15,
  lineHeight: 1,
  backgroundColor: 'white',
  boxShadow: 'hsl(206 22% 7% / 35%) 0px 10px 38px -10px, hsl(206 22% 7% / 20%) 0px 10px 20px -15px',
});

export const Tooltip = ({children, content}) => {
  return (
    <RadixTooltip.Root delayDuration={500}>
      <RadixTooltip.Trigger asChild>{children}</RadixTooltip.Trigger>
      <StyledContent>
        {content}
        <RadixTooltip.Arrow fill="white"></RadixTooltip.Arrow>
      </StyledContent>
    </RadixTooltip.Root>
  );
};
