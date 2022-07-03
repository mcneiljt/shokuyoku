import {useState, useCallback} from 'react';
import {Button, Container, Tooltip, Input, Span} from 'components/ui';
import {useEffect} from 'react';
import {matchSorter} from 'match-sorter';
import {useConfetti} from 'hooks';
import {useNavigate} from 'react-router-dom';
import arrow_left from 'assets/arrow_left.svg';

// TODO: move this up a level?
import {useCreateSchemaMutation} from 'modules/CreateTable/hooks';

export const EventPicker = ({deltas, databaseName, eventTables}) => {
  const [searchText, setSearchText] = useState('');
  const [filteredEventTables, setFilteredEventTables] = useState(eventTables);
  const [selectedEventTable, setSelectedEventTable] = useState();
  const {createSchemaMutation} = useCreateSchemaMutation();

  const navigate = useNavigate();
  const {showConfetti} = useConfetti();

  // show search results on text change
  useEffect(() => {
    const tableMatches = matchSorter(eventTables, searchText);
    setFilteredEventTables(tableMatches);
  }, [searchText, eventTables]);

  // deselect table when not present in search results
  useEffect(() => {
    if (!filteredEventTables.includes(selectedEventTable)) {
      setSelectedEventTable(null);
    }
  }, [filteredEventTables]);

  // deselect table on "Escape" keypress
  useEffect(() => {
    document.addEventListener('keydown', handleKeyDown, false);
    return () => {
      document.removeEventListener('keydown', handleKeyDown, false);
    };
  });

  const handleKeyDown = useCallback((event) => {
    if (event && event.key === 'Escape') {
      setSelectedEventTable(null);
    }
  }, []);

  const handleSearchChange = (event) => {
    setSearchText(event.target.value);

    // :)
    if (event.target.value.toLowerCase() === 'something') {
      showConfetti();
    }
  };

  const handleClickTable = (event) => {
    setSelectedEventTable(event);
  };

  const handleCreateTable = () => {
    const newEventSchema = {
      name: searchText,
      partitionedBy: [
        {
          name: 'date',
          type: 'date',
        },
      ],
      columns: [{name: 'date', type: 'date', comment: ''}],
      location: `s3a://analytics/${searchText}`,
    };

    createSchemaMutation.mutate({databaseName, newTableName: searchText, newEventSchema});
  };

  const showDeltaIcon = (eventTableName) => (deltas[eventTableName] ? true : false);

  // TODO: validate table name prior to table creation
  return (
    <Container display="flex" flexDirection="column" width="450px">
      <Input
        placeholder="Type something"
        type="text"
        value={searchText}
        onChange={handleSearchChange}
        border="1px solid black"
        paddingY="small"
        paddingLeft="medium"
        borderRadius="medium"
        borderColor="background"
        boxShadow="0 0 16px 4px rgba(211,65,143,.1)"
      />
      <Container
        paddingY="medium"
        border="1px solid black"
        mt="small"
        borderRadius="medium"
        height="400px"
        overflow="auto"
        bg="background"
        borderColor="background"
      >
        <Container bg="white" height="100%">
          {filteredEventTables.length > 0 ? (
            [...filteredEventTables].map((eventTableName, index) => {
              return (
                <Container
                  borderTop={index === 0 ? '1px solid black' : null}
                  borderBottom={'1px solid'}
                  borderColor="background"
                  paddingY="tiny"
                  bg="white"
                  key={eventTableName}
                  onClick={() => handleClickTable(eventTableName)}
                  backgroundColor={selectedEventTable === eventTableName ? '#baeeba' : null}
                  pl="medium"
                >
                  {showDeltaIcon(eventTableName) && (
                    <Tooltip content={deltas[eventTableName].lastError}>
                      <Span mr="medium" color="red">
                        ⚠️
                      </Span>
                    </Tooltip>
                  )}
                  {eventTableName}
                </Container>
              );
            })
          ) : (
            <Container
              display="flex"
              flexDirection="column"
              textAlign="center"
              opacity="50%"
              justifyContent="center"
              alignItems="center"
              height="100%"
            >
              <Span>No dice. That table doesn't seem to exist.</Span>
              <Span>
                Want to{' '}
                <a role="button" onClick={handleCreateTable}>
                  create the table <Span fontWeight="bold">{searchText}</Span>?
                </a>
              </Span>
            </Container>
          )}
        </Container>
      </Container>
      <Button
        onClick={() => navigate(`/database/${databaseName}/event_type/${selectedEventTable}`)}
        disabled={!selectedEventTable}
        marginTop="small"
        minWidth="150px"
        marginLeft="auto"
      >
        <Container display="flex" flexDirection="row" alignItems="center" justifyContent="center">
          <Span>Manage {selectedEventTable}</Span>
          <img alt="arrow right" style={{transform: 'rotate(180deg)'}} src={arrow_left} />
        </Container>
      </Button>
    </Container>
  );
};
