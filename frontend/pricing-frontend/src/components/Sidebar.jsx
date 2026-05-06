import React from 'react';
import { NavLink } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';

const NAV = [
  { to: '/',           icon: '⊞',  label: 'Dashboard',  end: true },
  { to: '/predictor',  icon: '🔮', label: 'Predictor'             },
  { to: '/properties', icon: '🏠', label: 'Inmuebles'             },
  { to: '/insights',   icon: '📊', label: 'Insights'              },
];

const Sidebar = () => {
  const { logout, user } = useAuth();
  return (
    <aside className="sidebar">
      <div className="sidebar-logo">
        <div className="sidebar-brand">
          <span className="sidebar-brand-icon">🏠</span>
          <span className="sidebar-brand-name">StayCast</span>
        </div>
      </div>

      <nav className="sidebar-nav">
        {NAV.map(({ to, icon, label, end }) => (
          <NavLink
            key={to} to={to} end={end}
            className={({ isActive }) => 'sidebar-link' + (isActive ? ' active' : '')}
          >
            <span className="sidebar-link-icon">{icon}</span>
            <span className="sidebar-label">{label}</span>
          </NavLink>
        ))}
      </nav>

      <div className="sidebar-bottom">
        <div className="sidebar-user">
          <div className="sidebar-user-avatar">{user?.name?.[0]?.toUpperCase() ?? 'U'}</div>
          <span className="sidebar-label sidebar-user-name">{user?.name ?? 'Usuario'}</span>
        </div>
        <button className="sidebar-logout" onClick={logout}>
          <span>↩</span>
          <span className="sidebar-label">Salir</span>
        </button>
      </div>
    </aside>
  );
};

export default Sidebar;

