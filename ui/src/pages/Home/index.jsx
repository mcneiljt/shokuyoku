import {Container, Text} from 'components/ui';
import {Link} from 'react-router-dom';
import styled from 'styled-components';
import arrow_left from 'assets/arrow_left.svg';
const StyledLink = styled(Link)`
  text-decoration: none;
`;
export const Home = () => {
  // TODO: configs for default table
  const defaultTable = 'default';
  // const defaultTable = 'events';

  return (
    <Container display="flex" justifyContent="center" alignItems="center" height="100%">
      <StyledLink to={`/database/${defaultTable}`}>
        <Container
          padding="large"
          borderRadius="medium"
          display="flex"
          flexDirection="row"
          alignItems="center"
          bg="cardBackground"
          border="1px solid"
          borderColor="borderColor"
          gap="medium"
          color="fontColor"
        >
          <Text heading={3}>Manage Events</Text>
          <img alt="arrow right" style={{transform: 'rotate(180deg)'}} src={arrow_left} />
        </Container>
      </StyledLink>
    </Container>
  );
};
