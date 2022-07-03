import {Container, Modal, Span, Text, Input, Button} from 'components/ui';
import {useState, useEffect} from 'react';
import {useParams, Link} from 'react-router-dom';
import {EventDetails, EventColumnTable} from 'modules';
import {useQuery, useQueryClient} from 'react-query';
import {getDeltas, getTypes, getEventSchema} from './api';
import _ from 'lodash';
import {useUpdateSchemaMutation, useDeleteSchemaMutation} from './hooks';
import arrow_left from 'assets/arrow_left.svg';

export const Event = () => {
  const {databaseName, eventTypeName} = useParams();
  const [columnTypes, setColumnTypes] = useState();
  const [deltas, setDeltas] = useState();
  const [eventSchema, setEventSchema] = useState();
  // eventColumns refer to the EVENT's configuration
  // NOT the table component columns
  const [originalEventColumns, setOriginalEventColumns] = useState([]);
  const [mutatedEventColumns, setMutatedEventColumns] = useState([]);
  const [selectedEventColumns, setSelectedEventColumns] = useState({});
  const [deleteConfirmationText, setDeleteConfirmationText] = useState('');

  const queryClient = useQueryClient();
  const {updateSchemaMutation} = useUpdateSchemaMutation();
  const {deleteSchemaMutation} = useDeleteSchemaMutation();

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
      const eventSchemaColumns = schemaQueryData.sd.cols;
      // add ids for tracking
      const eventSchemaColumnsWithIds = eventSchemaColumns.map((column, index) => {
        return {id: index, ...column};
      });

      setOriginalEventColumns(eventSchemaColumnsWithIds);
      setMutatedEventColumns(eventSchemaColumnsWithIds);
    }
  }, [schemaQueryStatus, schemaQueryData]);

  const handleOnChange = (event) => {
    setDeleteConfirmationText(event.target.value);
  };

  // react-table example
  const handleOnBlurRow = (rowIndex, columnId, value) => {
    console.log('rowIndex', rowIndex, 'columnId', columnId, 'value', value);
    setMutatedEventColumns((oldEventColumns) => {
      return oldEventColumns.map((row, index) => {
        if (index === rowIndex) {
          return {
            ...oldEventColumns[rowIndex],
            [columnId]: value,
          };
        }
        return row;
      });
    });
  };

  const handleSaveTable = () => {
    const newEventSchema = {
      name: eventSchema.tableName,
      partitionedBy: [
        {
          name: eventSchema.partitionKeys[0].name,
          type: eventSchema.partitionKeys[0].type,
        },
      ],
      columns: mutatedEventColumns
        .filter((column) => {
          return !Object.keys(selectedEventColumns)
            .map((key) => parseInt(key))
            .includes(column.id);
        })
        .map((column) => {
          const {name, type, comment} = column;

          return {name, type, comment};
        }),
      location: eventSchema.sd.location.replace('{table_name}', eventSchema.tableName),
    };

    updateSchemaMutation.mutate({databaseName, eventTypeName, newEventSchema});
  };

  const renderError = [typesQueryStatus, deltasQueryStatus, schemaQueryStatus].includes('error');
  const renderLoading = [typesQueryStatus, deltasQueryStatus, schemaQueryStatus].includes(
    'loading'
  );
  const renderSuccess =
    [typesQueryStatus, deltasQueryStatus, schemaQueryStatus].includes('success') &&
    eventSchema !== undefined &&
    columnTypes !== undefined &&
    deltas !== undefined;

  const getSchemaDiffs = () => {
    const diff = mutatedEventColumns.filter((obj, idx) => {
      const originalRow = originalEventColumns.find((dataObj) => dataObj.id === obj.id);

      return originalRow ? !_.isEqual(obj, originalRow) : true;
    });

    return diff.map((row) => row.id);
  };

  const addEventColumn = (maxId) => {
    setMutatedEventColumns((oldEventColumns) => {
      return [
        ...oldEventColumns,
        {
          id: maxId + 1,
          name: '',
          type: 'string',
          comment: '',
          new: true,
        },
      ];
    });
  };

  return (
    <Container display="flex" flexDirection="column" height="100%">
      <Link style={{textDecoration: 'none', width: 'fit-content'}} to={`/database/${databaseName}`}>
        <Container display="flex" flexDirection="row" mb="small" gap="tiny">
          <img src={arrow_left} alt="left arrow" />{' '}
          <Text>
            go back to <Span fontWeight="bold">{databaseName}</Span>
          </Text>
        </Container>
      </Link>
      <Text opacity="50%" fontWeight="bold" fontSize="xSmall">
        {databaseName.toUpperCase()}
      </Text>
      <Container display="flex" alignItems="center" gap="small">
        <Text lineHeight="1" heading={1}>
          {eventTypeName.toUpperCase()}
        </Text>
        <Modal
          trigger={<Span>🗑️</Span>}
          onClose={(isOpen) => {
            if (!isOpen) {
              setDeleteConfirmationText('');
            }
          }}
        >
          <Container display="flex" flexDirection="column" gap="medium">
            <Text heading={2}>Delete event table?</Text>
            <Container>
              Hey, more power to you. Just to be safe, type the table name below to confirm
              deletion.
            </Container>
            <Input
              placeholder={`Type '${eventTypeName}' to confirm deletion.`}
              onChange={handleOnChange}
              value={deleteConfirmationText}
            ></Input>
            <Button
              marginRight="auto"
              minWidth="100px"
              disabled={deleteConfirmationText !== eventTypeName}
              onClick={() => deleteSchemaMutation.mutate({databaseName, eventTypeName})}
            >
              Delete
            </Button>
          </Container>
        </Modal>
        <Button onClick={handleSaveTable} ml="large">
          Save
        </Button>
      </Container>

      <Container
        display="flex"
        flexDirection="column"
        height="100%"
        paddingY="tiny"
        gap="large"
        marginTop="small"
      >
        {renderError && <div>what have you done??</div>}
        {renderLoading && <div>it's loading.</div>}
        {renderSuccess ? (
          <EventDetails
            eventSchema={eventSchema}
            databaseName={databaseName}
            eventTypeName={eventTypeName}
          />
        ) : null}
        {renderSuccess ? (
          <EventColumnTable
            tableData={mutatedEventColumns}
            onBlurRow={handleOnBlurRow}
            diffData={getSchemaDiffs()}
            onAddEventColumn={addEventColumn}
            onSelectEventColumn={setSelectedEventColumns}
            selectedEventColumns={selectedEventColumns}
            types={columnTypes}
          />
        ) : null}
      </Container>
    </Container>
  );
};
