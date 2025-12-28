// Root application component
import type { FunctionComponent } from 'preact';
import { useRoute } from '../lib/router';
import { Layout } from './Layout';
import { Overview } from './Pages/Overview';
import { ModelDetail } from './Pages/ModelDetail';
import { SourceDetail } from './Pages/SourceDetail';
import { Lineage } from './Pages/Lineage';
import { NotFound } from './Pages/NotFound';

interface AppProps {
  dbReady: boolean;
}

export const App: FunctionComponent<AppProps> = ({ dbReady }) => {
  const route = useRoute();

  const renderPage = () => {
    switch (route.type) {
      case 'overview':
        return <Overview dbReady={dbReady} />;
      case 'lineage':
        return <Lineage dbReady={dbReady} />;
      case 'model':
        return <ModelDetail path={route.path} dbReady={dbReady} />;
      case 'source':
        return <SourceDetail name={route.name} dbReady={dbReady} />;
      case 'not-found':
      default:
        return <NotFound />;
    }
  };

  return (
    <Layout dbReady={dbReady}>{renderPage()}</Layout>
  );
};
