import React from 'react';
import { NavLink } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';

const IconGrid = () => (
  <svg width="15" height="15" viewBox="0 0 15 15" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round">
    <rect x="1" y="1" width="5.5" height="5.5" rx="1"/>
    <rect x="8.5" y="1" width="5.5" height="5.5" rx="1"/>
    <rect x="1" y="8.5" width="5.5" height="5.5" rx="1"/>
    <rect x="8.5" y="8.5" width="5.5" height="5.5" rx="1"/>
  </svg>
);

const IconTrend = () => (
  <svg width="15" height="15" viewBox="0 0 15 15" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
    <polyline points="1,11 5,6 9,8.5 14,2"/>
    <polyline points="11,2 14,2 14,5"/>
  </svg>
);

const IconBuilding = () => (
  <svg width="15" height="15" viewBox="0 0 15 15" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
    <path d="M1.5 13.5V7L7.5 1.5 13.5 7v6.5H1.5z"/>
    <rect x="5.5" y="9" width="4" height="4.5"/>
  </svg>
);

const IconBarChart = () => (
  <svg width="15" height="15" viewBox="0 0 15 15" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round">
    <line x1="0.5" y1="13.5" x2="14.5" y2="13.5"/>
    <rect x="1.5" y="8" width="3" height="5.5" rx="0.5"/>
    <rect x="6" y="4" width="3" height="9.5" rx="0.5"/>
    <rect x="10.5" y="6" width="3" height="7.5" rx="0.5"/>
  </svg>
);

const NAV = [
  { to: '/',           icon: <IconGrid />,     label: 'Dashboard',  end: true },
  { to: '/predictor',  icon: <IconTrend />,    label: 'Predictor'            },
  { to: '/properties', icon: <IconBuilding />, label: 'Inmuebles'            },
  { to: '/insights',   icon: <IconBarChart />, label: 'Insights'             },
];

const Sidebar = () => {
  const { logout, user } = useAuth();
  return (
    <aside className="sidebar">
      <div className="sidebar-logo">
        <div className="sidebar-brand">
          <span className="sidebar-brand-icon">
            <svg width="18" height="18" viewBox="0 0 18 18" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
              <path d="M2 16V9L9 2l7 7v7H2z"/>
              <rect x="6.5" y="10.5" width="5" height="5.5"/>
            </svg>
          </span>
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
