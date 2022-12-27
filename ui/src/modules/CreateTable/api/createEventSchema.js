import axios from 'axios';

export const createEventSchema = (dbName, tableName, schemaData) => {
  axios.post(
    `/schemas/${dbName}/${encodeURIComponent(tableName)}`,
    {...schemaData},
    {
      headers: {
        'Content-Type': 'application/json',
      },
    }
  );
};
