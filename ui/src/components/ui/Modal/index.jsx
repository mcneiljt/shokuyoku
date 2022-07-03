import * as Dialog from '@radix-ui/react-dialog';
import styled from 'styled-components';

const Overlay = styled(Dialog.Overlay)({
  background: 'rgba(0 0 0 / 0.5)',
  position: 'fixed',
  top: 0,
  left: 0,
  right: 0,
  bottom: 0,
  display: 'grid',
  placeItems: 'center',
  overflowY: 'auto',
});

const Content = styled(Dialog.Content)({
  maxWidth: 500,
  background: 'white',
  padding: 30,
  borderRadius: 4,
  maxHeight: 500,
  overflow: 'auto',
});

export const Modal = ({trigger, children, onClose}) => {
  return (
    <Dialog.Root onOpenChange={onClose}>
      <Dialog.Trigger asChild>{trigger}</Dialog.Trigger>
      <Dialog.Portal>
        <Overlay>
          <Content>{children}</Content>
        </Overlay>
      </Dialog.Portal>
    </Dialog.Root>
  );
};
