import {Container, Text} from 'components/ui';
import {Link} from 'react-router-dom';
export const Home = () => {
  return (
    <Container display="flex" justifyContent="center" alignItems="center" height="100%">
      <Link to="/database/default">
        <Container
          border="1px solid black"
          padding="large"
          borderRadius="medium"
          display="flex"
          flexDirection="row"
          alignItems="center"
        >
          <Text heading={3}>Manage Events</Text>
          <Container marginLeft="medium"> {'>'} </Container>
        </Container>
      </Link>
    </Container>
  );
};
