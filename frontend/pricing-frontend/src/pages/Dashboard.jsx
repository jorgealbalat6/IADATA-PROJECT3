import React from 'react';
import { Link } from 'react-router-dom';
import { useProperties } from '../contexts/PropertyContext';

const Dashboard = () => {
  const { properties } = useProperties();

  return (
    <div>
      <p className="dashboard-welcome">Bienvenido, <span>María!</span></p>

      {properties.length === 0 ? (
        <div className="empty-state">
          <p><strong>No tienes alojamientos</strong> aún.</p>
          <Link to="/properties/add">
            <button className="btn btn-primary">Añadir Alojamiento</button>
          </Link>
        </div>
      ) : (
        <div className="card">
          <p style={{ color: '#555', margin: '0 0 16px' }}>
            Tienes <strong>{properties.length}</strong> alojamientos registrados.
          </p>
          <Link to="/properties">
            <button className="btn btn-primary">Ver Mis Alojamientos</button>
          </Link>
        </div>
      )}
    </div>
  );
};

export default Dashboard;
