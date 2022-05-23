import {useState, useCallback} from 'react';
import {Button, Container, Tooltip, Input, Span} from 'components/ui';
import {useEffect} from 'react';
import {matchSorter} from 'match-sorter';
import {useConfetti} from 'hooks';
import {useNavigate} from 'react-router-dom';

export const EventPicker = ({databaseName, eventTables}) => {
  const [searchText, setSearchText] = useState('');
  const [filteredEventTables, setFilteredEventTables] = useState(eventTables);
  const [selectedEventTable, setSelectedEventTable] = useState();
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

  // TODO: get eventType schema, replace hardcoded value with delta match logic
  // TODO: actually pass in delta data to component lol
  const showDeltaIcon = (eventTableName) => eventTableName === 'boruger';

  // TODO: validate table name prior to table creation
  // TODO: implement create table
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
      />
      <Container
        padding="medium"
        border="1px solid black"
        mt="small"
        borderRadius="medium"
        height="400px"
        overflow="auto"
      >
        {filteredEventTables.length > 0 ? (
          [...filteredEventTables].map((eventTableName, index) => {
            return (
              <Container
                borderTop={index === 0 ? '1px solid black' : null}
                borderBottom={'1px solid black'}
                paddingY="tiny"
                key={eventTableName}
                onClick={() => handleClickTable(eventTableName)}
                backgroundColor={selectedEventTable === eventTableName ? 'yellow' : null}
                pl="medium"
              >
                {showDeltaIcon(eventTableName) && (
                  <Tooltip content="date here ;)">
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
              <a href="/">
                create the table <Span fontWeight="bold">{searchText}</Span>?
              </a>
            </Span>
          </Container>
        )}
      </Container>
      <Button
        onClick={() => navigate(`/database/${databaseName}/event_type/${selectedEventTable}`)}
        disabled={!selectedEventTable}
        marginTop="small"
        minWidth="150px"
        marginLeft="auto"
      >
        Manage {selectedEventTable} {'>'}
      </Button>
    </Container>
  );
};
