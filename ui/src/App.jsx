import './App.css';
import React from 'react';

import {
  BrowserRouter,
  Routes,
  Route,
} from 'react-router-dom';
import Grid from '@mui/material/Grid';
import Typography from '@mui/material/Typography';
import Databases from './pages/Databases';
import Database from './pages/Database';
import Table from './pages/Table';

function App() {
  return (
    <Grid
      container
      justifyContent="center"
      spacing={2}
    >
      <Grid item xs={8} style={{ borderBottom: '1px solid black' }}>
        <Typography variant="h5" gutterBottom component="div">
          <a href="/">Shokuyoku</a>
        </Typography>
      </Grid>
      <Grid item xs={8}>
        <div className="App">
          <header className="App-header" />
          <BrowserRouter>
            <Routes>
              <Route path="/" element={<Databases />} />
              <Route path="/database/*">
                <Route path=":databaseName/create_table" element={<Table newEventType />} />
                <Route path=":databaseName/event_type/*">
                  <Route path=":eventTypeName" element={<Table />} />
                </Route>
                <Route path=":databaseName" element={<Database />} />
              </Route>
            </Routes>
          </BrowserRouter>
        </div>
      </Grid>
    </Grid>
  );
}

export default App;
