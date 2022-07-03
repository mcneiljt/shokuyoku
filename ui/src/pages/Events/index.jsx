import {useState, useEffect} from 'react';
import {Container, Text} from 'components/ui';
import {useParams} from 'react-router-dom';
import {CreateTable, EventPicker} from 'modules';

export const Events = () => {
  // TODO: hardcode to 'events'
  const {databaseName} = useParams();

  const [eventTypes, setEventTypes] = useState();
  const [deltaEventTypes, setDeltaEventTypes] = useState({});

  // TODO: refactor to react query
  useEffect(() => {
    fetch('/deltas/event_type')
      .then((response) => response.json())
      .then((delta) => {
        const d = {};
        delta.forEach((item) => {
          d[item.name] = item;
        });
        setDeltaEventTypes(d);
      });
    fetch(`/schemas/${databaseName}`)
      .then((response) => response.json())
      .then(setEventTypes);
  }, []);

  // TODO: replace loading component
  return (
    <Container display="flex" flexDirection="column" height="100%">
      <Text heading={1}>{databaseName.toUpperCase()}</Text>
      <Container display="flex" flexDirection="row" gap="huge" alignItems="stretch">
        <Container width="50%" display="flex" flexDirection="column">
          <Text heading={2}>Pick an Event (Table/Type)</Text>
          <Container
            bg="cardBackground"
            borderRadius="small"
            display="flex"
            justifyContent="center"
            paddingY="huge"
            marginTop="xLarge"
          >
            {eventTypes ? (
              <EventPicker
                deltas={deltaEventTypes}
                databaseName={databaseName}
                eventTables={eventTypes}
              />
            ) : (
              <div>loading</div>
            )}
          </Container>
        </Container>
        <Container width="50%" display="flex" flexDirection="column">
          <Text heading={2}>Create Table</Text>
          <Container
            bg="cardBackground"
            borderRadius="small"
            display="flex"
            justifyContent="center"
            paddingY="huge"
            marginTop="xLarge"
            height="100%"
          >
            <CreateTable existingTables={eventTypes} databaseName={databaseName}></CreateTable>
          </Container>
        </Container>
      </Container>
    </Container>
  );
};
