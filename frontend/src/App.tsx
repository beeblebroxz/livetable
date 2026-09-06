import { useEffect, useState } from 'react';
import { LiveTable } from './components/LiveTable';
import { OrdersLab } from './pages/OrdersLab';

function App() {
  const [showEditor, setShowEditor] = useState(window.location.hash === '#editor');
  useEffect(() => {
    const navigate = () => setShowEditor(window.location.hash === '#editor');
    window.addEventListener('hashchange', navigate);
    return () => window.removeEventListener('hashchange', navigate);
  }, []);

  if (!showEditor) {
    return <OrdersLab />;
  }

  return <LiveTable tableName="demo" />;
}

export default App;
