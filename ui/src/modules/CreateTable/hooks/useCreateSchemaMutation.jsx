import React from 'react';
import {useQuery, useMutation, useQueryClient} from 'react-query';
import {createEventSchema} from '../api';
import {useNavigate} from 'react-router-dom';

export const useCreateSchemaMutation = () => {
  const queryClient = useQueryClient();
  const navigate = useNavigate();

  const createSchemaMutation = useMutation(
    ({databaseName, eventTypeName, newEventSchema}) =>
      createEventSchema(databaseName, eventTypeName, newEventSchema),
    {
      onSuccess: (data, variables) => {
        const {databaseName, newTableName} = variables;

        queryClient.invalidateQueries('schema');
        alert('table created.');
        navigate(`/database/${databaseName}/event_type/${newTableName}`);
      },
    }
  );

  return {createSchemaMutation};
};
