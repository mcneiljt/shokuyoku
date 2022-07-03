import * as RadixSelect from '@radix-ui/react-select';
import {Container} from '../Container';

// TODO: this
export const Select = ({initialValue}) => (
  <RadixSelect.Root defaultValue={initialValue}>
    <RadixSelect.Trigger>
      <Container>
        <RadixSelect.Value />
        <RadixSelect.Icon />
      </Container>
    </RadixSelect.Trigger>

    <RadixSelect.Content>
      <RadixSelect.ScrollUpButton />
      <RadixSelect.Viewport>
        <RadixSelect.Item>
          <RadixSelect.ItemText />
          <RadixSelect.ItemIndicator />
        </RadixSelect.Item>

        <RadixSelect.Group>
          <RadixSelect.Label />
          <RadixSelect.Item>
            <RadixSelect.ItemText />
            <RadixSelect.ItemIndicator />
          </RadixSelect.Item>
        </RadixSelect.Group>

        <RadixSelect.Separator />
      </RadixSelect.Viewport>
      <RadixSelect.ScrollDownButton />
    </RadixSelect.Content>
  </RadixSelect.Root>
);
