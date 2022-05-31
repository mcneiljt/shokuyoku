import React, {useState} from 'react';
import {Button, Container, Input, Text} from 'components/ui';
import {useCreateSchemaMutation} from './hooks';

export const CreateTable = ({existingTables, databaseName}) => {
  const [newTableName, setNewTableName] = useState('');
  const {createSchemaMutation} = useCreateSchemaMutation();
  const handleInputChange = (event) => {
    setNewTableName(event.target.value);
  };

  const tableNameExists = () => {
    return existingTables
      ? existingTables.find((table) => table.toLowerCase() === newTableName.toLowerCase())
      : false;
  };

  const handleCreateTable = () => {
    const newEventSchema = {
      name: newTableName,
      partitionedBy: [
        {
          name: 'date',
          type: 'date',
        },
      ],
      columns: [{name: 'date', type: 'date', comment: ''}],
      location: `s3a://analytics/${newTableName}`,
    };

    createSchemaMutation.mutate({databaseName, newTableName, newEventSchema});
  };

  // TODO: consolidate input styles
  return (
    <Container display="flex" flexDirection="column" gap="medium" width="450px">
      <Input
        border="none"
        borderRadius="small"
        paddingY="small"
        paddingLeft="medium"
        placeholder="Enter table name"
        boxShadow="0 0 16px 4px rgba(211,65,143,.1)"
        value={newTableName}
        onChange={handleInputChange}
      ></Input>
      {tableNameExists() ? <Text color="red">that table exists already</Text> : null}
      <Button disabled={newTableName === '' || tableNameExists()} onClick={handleCreateTable}>
        create table
      </Button>
    </Container>
  );
};
