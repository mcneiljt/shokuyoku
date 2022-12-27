import {get} from 'axios';

export const getEventSchema = async (dbName, eventTypeName) => {
  const {data} = await get(`/schemas/${dbName}/${eventTypeName}`);
  return data;
};
