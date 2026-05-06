import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';

const CITIES = ['Málaga', 'Sevilla', 'Granada', 'Córdoba', 'Almería', 'Cádiz'];
const ZONES  = ['Centro', 'Centro Histórico', 'Playa', 'Alameda', 'El Palo', 'Pedregalejo', 'Teatinos'];
const TYPES  = ['Apartamento', 'Ático', 'Casa', 'Habitación', 'Otra'];

const PropertyForm = ({ initialData = {}, onSubmit, isEdit = false }) => {
  const navigate = useNavigate();
  const [formData, setFormData] = useState({
    name:        initialData.name        || '',
    city:        initialData.city        || '',
    zone:        initialData.zone        || '',
    type:        initialData.type        || '',
    rooms:       initialData.rooms       || 1,
    capacity:    initialData.capacity    || 1,
    priceMedian: initialData.priceMedian || '',
    address:     initialData.address     || '',
    latitude:    initialData.latitude    || '',
    longitude:   initialData.longitude   || '',
  });

  const handleChange = (e) => {
    const { name, value } = e.target;
    setFormData(d => ({ ...d, [name]: value }));
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    onSubmit(formData);
  };

  return (
    <div className="pform-card">
      <div className="pform-header">
        <span className="pform-header-icon">🏠</span>
        <h2 className="pform-title">{isEdit ? 'Editar Alojamiento' : 'Nuevo Alojamiento'}</h2>
      </div>

      <form onSubmit={handleSubmit} className="pform-body">
        {/* Nombre */}
        <div className="pform-input-wrapper">
          <input
            className="pform-input"
            type="text"
            name="name"
            placeholder="Nombre del Alojamiento"
            value={formData.name}
            onChange={handleChange}
            required
          />
        </div>

        {/* Ciudad */}
        <div className="pform-input-wrapper">
          <select className="pform-input pform-select" name="city" value={formData.city} onChange={handleChange} required>
            <option value="">Ciudad</option>
            {CITIES.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
          <span className="pform-select-arrow">▾</span>
        </div>

        {/* Barrio / Zona */}
        <div className="pform-input-wrapper">
          <select className="pform-input pform-select" name="zone" value={formData.zone} onChange={handleChange} required>
            <option value="">Barrio / Zona</option>
            {ZONES.map(z => <option key={z} value={z}>{z}</option>)}
          </select>
          <span className="pform-select-arrow">▾</span>
        </div>

        {/* Tipo */}
        <div className="pform-input-wrapper">
          <select className="pform-input pform-select" name="type" value={formData.type} onChange={handleChange} required>
            <option value="">Tipo de Alojamiento</option>
            {TYPES.map(t => <option key={t} value={t}>{t}</option>)}
          </select>
          <span className="pform-select-arrow">▾</span>
        </div>

        {/* Habitaciones + Capacidad en línea */}
        <div className="pform-row-inline">
          <div className="pform-inline-label">Número de Habitaciones</div>
          <div className="pform-inline-counter">
            <button type="button" className="pform-counter-btn" onClick={() => setFormData(d => ({ ...d, rooms: Math.max(1, Number(d.rooms) - 1) }))}>−</button>
            <span className="pform-counter-val">{formData.rooms}</span>
            <button type="button" className="pform-counter-btn" onClick={() => setFormData(d => ({ ...d, rooms: Number(d.rooms) + 1 }))}>+</button>
          </div>
        </div>
        <div className="pform-row-inline">
          <div className="pform-inline-label">Capacidad Máxima</div>
          <div className="pform-inline-counter">
            <button type="button" className="pform-counter-btn" onClick={() => setFormData(d => ({ ...d, capacity: Math.max(1, Number(d.capacity) - 1) }))}>−</button>
            <span className="pform-counter-val">{formData.capacity}</span>
            <button type="button" className="pform-counter-btn" onClick={() => setFormData(d => ({ ...d, capacity: Number(d.capacity) + 1 }))}>+</button>
          </div>
        </div>

        {/* Precio medio */}
        <div className="pform-input-wrapper">
          <input
            className="pform-input"
            type="number"
            name="priceMedian"
            placeholder="Precio Medio (€)"
            value={formData.priceMedian}
            onChange={handleChange}
            required
          />
        </div>

        {/* Acciones */}
        <div className="pform-actions">
          <button type="button" className="pform-btn-cancel" onClick={() => navigate('/properties')}>Cancelar</button>
          <button type="submit" className="pform-btn-save">{isEdit ? 'Guardar Cambios' : 'Guardar'}</button>
        </div>
      </form>
    </div>
  );
};

export default PropertyForm;

