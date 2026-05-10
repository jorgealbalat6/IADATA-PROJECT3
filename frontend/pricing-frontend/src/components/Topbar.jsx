import React from 'react';
import { useAuth } from '../contexts/AuthContext';

const Topbar = ({ title }) => {
  const { user } = useAuth();
  return (
    <header className="topbar">
      <span className="topbar-title">{title}</span>
      <div className="topbar-right">
        <div className="topbar-user">
          <span className="topbar-user-name">{user?.name ?? 'Usuario'}</span>
          <div className="avatar" title={user?.name}>
            {user?.name?.[0]?.toUpperCase() ?? 'U'}
          </div>
        </div>
      </div>
    </header>
  );
};

export default Topbar;

