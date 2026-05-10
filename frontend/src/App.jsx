import React, { useState } from 'react';
import { useLocation } from 'react-router-dom';
import Sidebar        from './components/Sidebar';
import Topbar         from './components/Topbar';
import AppRouter, { PAGE_TITLES } from './router/AppRouter';
import Login          from './pages/Login';
import Register       from './pages/Register';
import { useAuth }    from './contexts/AuthContext';

const AppLayout = () => {
  const location = useLocation();
  const title = PAGE_TITLES[location.pathname] ?? 'StayCast';
  return (
    <div className="layout">
      <Sidebar />
      <div className="content-area">
        <Topbar title={title} />
        <main className="main">
          <AppRouter />
        </main>
      </div>
    </div>
  );
};

const App = () => {
  const { user, loading } = useAuth();
  const [authScreen, setAuthScreen] = useState('login');

  // Mientras Firebase comprueba si hay sesión activa
  if (loading) {
    return (
      <div className="auth-page">
        <div style={{ textAlign: 'center' }}>
          <div className="sc-spinner-large" />
          <p style={{ marginTop: 16, color: '#64748b' }}>Cargando...</p>
        </div>
      </div>
    );
  }

  if (!user) {
    return authScreen === 'login'
      ? <Login    onGoRegister={() => setAuthScreen('register')} />
      : <Register onGoLogin={()    => setAuthScreen('login')}    />;
  }
  return <AppLayout />;
};

export default App;
