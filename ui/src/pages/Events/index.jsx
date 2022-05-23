import {useState, useEffect} from 'react';
import {Container, Text} from 'components/ui';
import {useParams} from 'react-router-dom';
import {EventPicker} from 'modules';

export const Events = () => {
  // TODO: hardcode to 'events'
  const {databaseName} = useParams();

  const [eventTypes, setEventTypes] = useState();
  // TODO: pass this into EventPicker
  const [deltaEventTypes, setDeltaEventTypes] = useState({});

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

  return (
    <Container display="flex" flexDirection="column" bg="blue" height="100%">
      <Text heading={1}>{databaseName.toUpperCase()}</Text>
      <Container display="flex" flexDirection="row" height="100%">
        <Container width="50%" bg="orange" padding="medium" display="flex" flexDirection="column">
          <Text heading={2}>Pick an Event (Table/Type)</Text>
          <Container
            bg="cyan"
            display="flex"
            justifyContent="center"
            alignItems="center"
            height="100%"
          >
            {eventTypes ? (
              <EventPicker databaseName={databaseName} eventTables={eventTypes} />
            ) : (
              <div> wait,</div>
            )}
          </Container>
        </Container>
        <Container width="50%" bg="purple" padding="medium">
          <Text heading={2}>Create Table</Text>
          create table from schema here
        </Container>
      </Container>
    </Container>
  );
};
