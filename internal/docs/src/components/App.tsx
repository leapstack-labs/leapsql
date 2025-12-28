// Root application component
import type { FunctionComponent } from 'preact';
import { useMemo } from 'preact/hooks';
import type { Catalog } from '../lib/types';
import { CatalogContext, createCatalogContext } from '../lib/context';
import { useRoute } from '../lib/router';
import { Layout } from './Layout';
import { Overview } from './Pages/Overview';
import { ModelDetail } from './Pages/ModelDetail';
import { SourceDetail } from './Pages/SourceDetail';
import { Lineage } from './Pages/Lineage';
import { NotFound } from './Pages/NotFound';

interface AppProps {
  catalog: Catalog;
}

export const App: FunctionComponent<AppProps> = ({ catalog }) => {
  const catalogContext = useMemo(() => createCatalogContext(catalog), [catalog]);
  const route = useRoute();

  const renderPage = () => {
    switch (route.type) {
      case 'overview':
        return <Overview />;
      case 'lineage':
        return <Lineage />;
      case 'model':
        return <ModelDetail path={route.path} />;
      case 'source':
        return <SourceDetail name={route.name} />;
      case 'not-found':
      default:
        return <NotFound />;
    }
  };

  return (
    <CatalogContext.Provider value={catalogContext}>
      <Layout>{renderPage()}</Layout>
    </CatalogContext.Provider>
  );
};
