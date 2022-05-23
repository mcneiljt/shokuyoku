import {get} from 'axios';

export const getTypes = async () => {
  const {data} = await get('/types');
  return data;
};
