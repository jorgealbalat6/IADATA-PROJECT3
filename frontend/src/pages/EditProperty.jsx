import React from 'react';
import { useNavigate, useParams, Link } from 'react-router-dom';
import { useProperties } from '../contexts/PropertyContext';
import PropertyForm from '../components/PropertyForm';

const EditProperty = () => {
  const { id } = useParams();
  const { getProperty, updateProperty } = useProperties();
  const navigate = useNavigate();
  const numericId = Number(id);
  const property = getProperty(numericId);

  const handleSubmit = (data) => {
    updateProperty(numericId, data);
    navigate('/properties');
  };

  if (!property) return <p>Propiedad no encontrada. <Link to="/properties">Volver</Link></p>;

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Editar Alojamiento</h1>
        <Link to="/properties"><button className="btn btn-outline">← Volver</button></Link>
      </div>
      <PropertyForm initialData={property} onSubmit={handleSubmit} isEdit={true} />
    </div>
  );
};

export default EditProperty;
