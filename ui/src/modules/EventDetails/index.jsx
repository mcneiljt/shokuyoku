import {Text, Container} from 'components/ui';

export const EventDetails = ({databaseName, eventTypeName, eventSchema}) => {
  console.log(eventSchema);
  return (
    <Container display="flex" flexDirection="column">
      <Text heading={3}>{'Partitioned By'.toUpperCase()}</Text>
      <Container>{eventSchema.partitionKeys[0].name}</Container>
    </Container>
  );
};
