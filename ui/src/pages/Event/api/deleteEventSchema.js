import axios from 'axios';

export const deleteEventSchema = async (dbName, tableName) => {
  await axios.delete(`/schemas/${dbName}/${encodeURIComponent(tableName)}`, {
    headers: {
      'Content-Type': 'application/json',
    },
    data: JSON.stringify({}),
  });
  return true;
};
