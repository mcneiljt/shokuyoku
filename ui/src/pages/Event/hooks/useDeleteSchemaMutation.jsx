import React from 'react';
import {useQuery, useMutation, useQueryClient} from 'react-query';
import {deleteEventSchema} from '../api';
import {useNavigate} from 'react-router-dom';

export const useDeleteSchemaMutation = () => {
  const queryClient = useQueryClient();
  const navigate = useNavigate();

  const deleteSchemaMutation = useMutation(
    ({databaseName, eventTypeName}) => deleteEventSchema(databaseName, eventTypeName),
    {
      onSuccess: () => {
        // Invalidate and refetch
        queryClient.invalidateQueries('schema');
        alert('table was deleted.');
        navigate('/');
      },
    }
  );

  return {deleteSchemaMutation};
};
