import React from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { useProperties } from '../contexts/PropertyContext';
import PropertyForm from '../components/PropertyForm';

const AddProperty = () => {
  const { addProperty } = useProperties();
  const navigate = useNavigate();

  const handleSubmit = (data) => {
    addProperty(data);
    navigate('/properties');
  };

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Añadir Alojamiento</h1>
        <Link to="/properties"><button className="btn btn-outline">← Volver</button></Link>
      </div>
      <PropertyForm onSubmit={handleSubmit} />
    </div>
  );
};

export default AddProperty;
