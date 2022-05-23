import {useState} from 'react';

import {BrowserRouter, Routes, Route} from 'react-router-dom';
import Grid from '@mui/material/Grid';
import Typography from '@mui/material/Typography';
import Databases from './pages/Databases';
import Database from './pages/Database';
import Table from './pages/Table';

import {Container, Text, Button} from 'components/ui';
import {ThemeProvider} from 'styled-components';
import styled from 'styled-components';
import {theme} from 'config/theme';
import {getCurrentTheme} from 'utils';
import {Link as RouterLink} from 'react-router-dom';
import {Home, Events, Event} from 'pages';
import {QueryClient, QueryClientProvider} from 'react-query';
const AppWrapper = styled(Container)`
  min-height: 100%;
  width: 100%;
  height: 100%;
  display: flex;
  flex-direction: column;
`;

const AppContainer = styled(Container)`
  position: relative;
  width: 100%;
  max-width: 1620px;
  height: 100%;
`;

function App() {
  // TODO: move to config
  const reactQueryConfig = {
    queries: {
      staleTime: Infinity,
      refetchIntervalInBackground: false,
      refetchOnWindowFocus: false,
    },
  };
  const queryClient = new QueryClient({defaultOptions: reactQueryConfig});

  const [themeMode, setThemeMode] = useState(
    window.localStorage.getItem('preferredTheme') || 'light'
  );

  const currentTheme = getCurrentTheme(theme, themeMode);

  const handleChangeTheme = (newTheme) => {
    window.localStorage.setItem('preferredTheme', newTheme);
    setThemeMode(newTheme);
  };
  /**
   * <Route path="/database/*">
                <Route path=":databaseName/" element={<Events />}>
                  <Route path="event_type/:eventTypeName" element={<Event />}></Route>
                </Route>
              </Route>
   */
  // return (
  //   <ThemeProvider theme={currentTheme}>
  //     <QueryClientProvider client={queryClient}>
  //       <BrowserRouter>
  //         <AppWrapper backgroundColor="background">
  //           <Container bg="green" height="50px">
  //             <Container maxWidth="1620px" marginX="auto" display="flex">
  //               <Text heading={1} paddingX="medium" width="fit-content">
  //                 <RouterLink to="/">Shokuyoku</RouterLink>
  //               </Text>
  //               <Button
  //                 variant="primary"
  //                 danger
  //                 onClick={() => handleChangeTheme(themeMode === 'light' ? 'dark' : 'light')}
  //               >
  //                 {themeMode}
  //               </Button>
  //             </Container>
  //           </Container>
  //           <AppContainer marginX="auto" paddingX="medium" paddingY="medium">
  //             <Routes>
  //               <Route path="/">
  //                 <Route index element={<Home />}></Route>
  //                 <Route path="database/:databaseName">
  //                   <Route index element={<Events />}></Route>
  //                   <Route path="event_type/:eventTypeName" element={<Event />}></Route>
  //                 </Route>
  //               </Route>
  //               <Route
  //                 path="*"
  //                 element={
  //                   <main style={{padding: '1rem'}}>
  //                     <p>There's nothing here!</p>
  //                   </main>
  //                 }
  //               />
  //             </Routes>
  //           </AppContainer>
  //         </AppWrapper>
  //       </BrowserRouter>
  //     </QueryClientProvider>
  //   </ThemeProvider>
  // );
  return (
    <Grid container justifyContent="center" spacing={2}>
      <Grid item xs={8} style={{borderBottom: '1px solid black'}}>
        <Typography variant="h5" gutterBottom component="div">
          <a href="/">Shokuyoku</a>
        </Typography>
      </Grid>
      <Grid item xs={8}>
        <div className="App">
          <Container>hi</Container>

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
