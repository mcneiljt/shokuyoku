import axios from 'axios';

export const updateEventSchema = (dbName, tableName, schemaData) => {
  axios.put(
    `/schemas/${dbName}/${encodeURIComponent(tableName)}`,
    {...schemaData},
    {
      headers: {
        'Content-Type': 'application/json',
      },
    }
  );
};
