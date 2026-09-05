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

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-50 to-gray-100">
      <header className="bg-white shadow-md">
        <div className="max-w-7xl mx-auto py-6 px-4 flex items-center justify-between">
          <div>
            <h1 className="text-4xl font-bold text-gray-900">
              LiveTable Editor
            </h1>
            <p className="text-gray-600 mt-2">
              Real-time collaborative editing powered by Rust + WebSocket
            </p>
          </div>
          <a
            href="#lab"
            className="px-4 py-2 bg-purple-600 text-white rounded-lg font-medium hover:bg-purple-700 transition flex items-center gap-2"
          >
            Orders Lab
            <span>&rarr;</span>
          </a>
        </div>
      </header>
      <main className="py-8">
        <LiveTable tableName="demo" />
      </main>
      <footer className="text-center py-6 text-sm text-gray-500">
        <p>Open this page in multiple tabs to see real-time synchronization!</p>
      </footer>
    </div>
  );
}

export default App;
