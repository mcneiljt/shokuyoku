import React from 'react';
import {useQuery, useMutation, useQueryClient} from 'react-query';
import {updateEventSchema} from '../api';

export const useUpdateSchemaMutation = () => {
  const queryClient = useQueryClient();

  const updateSchemaMutation = useMutation(
    ({databaseName, eventTypeName, newEventSchema}) =>
      updateEventSchema(databaseName, eventTypeName, newEventSchema),
    {
      onSuccess: () => {
        queryClient.invalidateQueries('schema');
        alert('table updated.');
        // maybe refresh?
      },
    }
  );

  return {updateSchemaMutation};
};
