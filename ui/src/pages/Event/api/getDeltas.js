import {get} from 'axios';

export const getDeltas = async (eventTypeName) => {
  const {data} = await get(`/deltas/event_type/${eventTypeName}`);
  return data;
};
