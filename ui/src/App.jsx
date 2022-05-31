import {useState} from 'react';

import {BrowserRouter, Routes, Route} from 'react-router-dom';

import {Container, Text, Button} from 'components/ui';
import {ThemeProvider} from 'styled-components';
import styled from 'styled-components';
import css from '@styled-system/css';
import {theme} from 'config/theme';
import {getCurrentTheme} from 'utils';
import {Link as RouterLink} from 'react-router-dom';
import {Home, Events, Event} from 'pages';
import {QueryClient, QueryClientProvider} from 'react-query';

// TODO: move styles
const AppWrapper = styled(Container)`
  min-height: 100%;
  width: 100%;
  height: 100%;
  display: flex;
  flex-direction: column;
  transition: all 0.2s ease;
`;

const AppContainer = styled(Container)`
  position: relative;
  width: 100%;
  max-width: 1280px;
  height: 100%;
`;

const StyledRouterLink = styled(RouterLink)`
  ${css({
    textDecoration: 'none',
    color: 'fontColor',
  })}
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

  // TODO: enable theme toggle
  return (
    <ThemeProvider theme={currentTheme}>
      <QueryClientProvider client={queryClient}>
        <BrowserRouter>
          <AppWrapper backgroundColor="background" color="fontColor">
            <Container bg="navColor" height="50px">
              <Container maxWidth="1280px" marginX="auto" display="flex" alignItems="center">
                <Text heading={1} paddingX="medium" width="fit-content">
                  <StyledRouterLink style={{textDecoration: 'none'}} to="/">
                    Shokuyoku
                  </StyledRouterLink>
                </Text>
                {/* <Button
                  ml="auto"
                  mr="medium"
                  variant="primary"
                  onClick={() => handleChangeTheme(themeMode === 'light' ? 'dark' : 'light')}
                >
                  {themeMode}
                </Button> */}
              </Container>
            </Container>
            <AppContainer marginX="auto" paddingX="medium" paddingY="medium">
              <Routes>
                <Route path="/">
                  <Route index element={<Home />}></Route>
                  <Route path="database/:databaseName">
                    <Route index element={<Events />}></Route>
                    <Route path="event_type/:eventTypeName" element={<Event />}></Route>
                  </Route>
                </Route>
                <Route
                  path="*"
                  element={
                    <main style={{padding: '1rem'}}>
                      <p>Not Found.</p>
                    </main>
                  }
                />
              </Routes>
            </AppContainer>
          </AppWrapper>
        </BrowserRouter>
      </QueryClientProvider>
    </ThemeProvider>
  );
}

export default App;
