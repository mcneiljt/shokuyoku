import {Container, Span, Text} from 'components/ui';
import {useState, useEffect, useCallback} from 'react';
import {useParams, useNavigate, Link} from 'react-router-dom';
import {EventDetails} from 'modules';
import {useQuery, useQueryClient} from 'react-query';
import {getDeltas, getTypes, getEventSchema} from './api';

export const Event = () => {
  const {databaseName, eventTypeName} = useParams();
  const [columnTypes, setColumnTypes] = useState();
  const [deltas, setDeltas] = useState();
  const [eventSchema, setEventSchema] = useState();

  const queryClient = useQueryClient();

  const {status: typesQueryStatus, data: typesQueryData} = useQuery('types', getTypes);
  const {status: deltasQueryStatus, data: deltasQueryData} = useQuery(
    ['deltas', eventTypeName],
    () => getDeltas(eventTypeName)
  );
  const {status: schemaQueryStatus, data: schemaQueryData} = useQuery(
    ['schema', databaseName, eventTypeName],
    () => getEventSchema(databaseName, eventTypeName)
  );

  // set state when ready
  useEffect(() => {
    if (typesQueryStatus === 'success') {
      setColumnTypes(typesQueryData);
    }
  }, [typesQueryStatus, typesQueryData]);

  useEffect(() => {
    if (deltasQueryStatus === 'success') {
      setDeltas(deltasQueryData);
    }
  }, [deltasQueryStatus, deltasQueryData]);

  useEffect(() => {
    if (schemaQueryStatus === 'success') {
      setEventSchema(schemaQueryData);
      // TODO: do more stuff
    }
  }, [schemaQueryStatus, schemaQueryData]);

  const renderError = [typesQueryStatus, deltasQueryStatus, schemaQueryStatus].includes('error');
  const renderLoading = [typesQueryStatus, deltasQueryStatus, schemaQueryStatus].includes(
    'loading'
  );
  const renderSuccess =
    [typesQueryStatus, deltasQueryStatus, schemaQueryStatus].includes('success') &&
    eventSchema !== undefined &&
    columnTypes !== undefined &&
    deltas !== undefined;

  console.log(renderSuccess);
  return (
    <Container display="flex" flexDirection="column" bg="lightblue" height="100%">
      <Link to={`/database/${databaseName}`}>
        <Text mb="small">
          {'<'} Go back to <Span fontWeight="bold">{databaseName}</Span>
        </Text>
      </Link>
      <Text opacity="50%" fontWeight="bold" fontSize="xSmall">
        {databaseName.toUpperCase()}
      </Text>
      <Text lineHeight="1" heading={1}>
        {eventTypeName.toUpperCase()}
      </Text>
      <Container bg="magenta" display="flex" flexDirection="row" height="100%" padding="medium">
        {renderError && <div>what have you done??</div>}
        {renderLoading && <div>it's loading yo.</div>}
        {renderSuccess ? (
          <EventDetails
            eventSchema={eventSchema}
            databaseName={databaseName}
            eventTypeName={eventTypeName}
          />
        ) : null}
      </Container>
    </Container>
  );
};
