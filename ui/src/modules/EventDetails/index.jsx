import {Text, Container} from 'components/ui';

export const EventDetails = ({databaseName, eventTypeName, eventSchema}) => {
  const partitionKeyName = eventSchema.partitionKeys[0].name;
  const partitionKeyType = eventSchema.partitionKeys[0].type;
  const location = eventSchema.sd.location;

  return (
    <Container
      display="flex"
      gap="huge"
      border="1px solid"
      borderColor="borderColor"
      paddingX="small"
      paddingTop="tiny"
      paddingBottom="small"
      borderRadius="small"
      backgroundColor="cardBackground"
    >
      <Container>
        <Text heading={3} color="headingColor">
          {'Partitioned By'.toUpperCase()}
        </Text>
        <Container style={{fontFamily: 'monospace'}} display="flex" gap="small">
          <Container>
            <Text fontSize="tiny">{'Type'.toUpperCase()}</Text>
            <Text lineHeight={1}>{partitionKeyType}</Text>
          </Container>
          <Container>
            <Text fontSize="tiny">{'Name'.toUpperCase()}</Text>
            <Text lineHeight={1}>{partitionKeyName}</Text>
          </Container>
        </Container>
      </Container>
      <Container display="flex" flexDirection="column">
        <Text color="headingColor" heading={3}>
          {'Location'.toUpperCase()}
        </Text>
        <Container display="flex" alignItems="center" height="100%">
          <Text lineHeight={1}>{location}</Text>
        </Container>
      </Container>
    </Container>
  );
};
