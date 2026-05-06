import React, { useState } from 'react';
import useProperties from '../../hooks/useProperties';
import {
  createDefaultProperty,
  NEIGHBOURHOODS,
  ROOM_TYPES,
} from '../../models/property';

// ── Validation ───────────────────────────────────────────

const validate = (form) => {
  const errors = {};

  if (!form.name.trim())
    errors.name = 'El nombre del alojamiento es obligatorio.';
  else if (form.name.trim().length < 3)
    errors.name = 'El nombre debe tener al menos 3 caracteres.';

  if (form.accommodates < 1 || form.accommodates > 20)
    errors.accommodates = 'El nº de huéspedes debe estar entre 1 y 20.';

  if (form.bedrooms < 0 || form.bedrooms > 20)
    errors.bedrooms = 'El nº de dormitorios debe estar entre 0 y 20.';

  if (form.beds < 1 || form.beds > 20)
    errors.beds = 'El nº de camas debe estar entre 1 y 20.';

  if (form.beds < form.bedrooms)
    errors.beds = 'El nº de camas no puede ser menor que el de dormitorios.';

  if (form.number_of_reviews < 0)
    errors.number_of_reviews = 'El nº de reseñas no puede ser negativo.';

  if (form.review_scores_rating < 1 || form.review_scores_rating > 5)
    errors.review_scores_rating = 'La puntuación debe estar entre 1 y 5.';

  return errors;
};

const FieldError = ({ msg }) =>
  msg ? <span className="field-error">⚠ {msg}</span> : null;

// ── Sub-components ───────────────────────────────────────

const PropertyRow = ({ property, onDelete }) => (
  <tr className="prop-row">
    <td className="prop-cell prop-cell-name">{property.name || '—'}</td>
    <td className="prop-cell">{property.neighbourhood}</td>
    <td className="prop-cell">{property.room_type}</td>
    <td className="prop-cell prop-cell-center">{property.accommodates}</td>
    <td className="prop-cell prop-cell-center">{property.bedrooms}</td>
    <td className="prop-cell prop-cell-center">{property.beds}</td>
    <td className="prop-cell prop-cell-center">{property.number_of_reviews}</td>
    <td className="prop-cell prop-cell-center">{property.review_scores_rating}</td>
    <td className="prop-cell prop-cell-center">
      <button
        className="btn-icon btn-icon-danger"
        onClick={() => onDelete(property.id)}
        title="Eliminar inmueble"
      >
        🗑
      </button>
    </td>
  </tr>
);

const RegisterForm = ({ onSave, loading }) => {
  const [form,   setForm]   = useState(createDefaultProperty);
  const [errors, setErrors] = useState({});

  const handleChange = (e) => {
    const { name, value, type } = e.target;
    setForm(f => ({ ...f, [name]: type === 'number' ? Number(value) : value }));
    // Clear field error on change
    if (errors[name]) setErrors(prev => ({ ...prev, [name]: undefined }));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    const found = validate(form);
    if (Object.keys(found).length > 0) {
      setErrors(found);
      return;
    }
    setErrors({});
    const saved = await onSave(form);
    if (saved) setForm(createDefaultProperty());
  };

  return (
    <div className="card prop-form-card">
      <h3 className="section-title" style={{ marginBottom: 16 }}>➕ Registrar Inmueble</h3>
      <form onSubmit={handleSubmit}>

        <div className="sc-form-group">
          <label>Nombre / Título del alojamiento</label>
          <input
            type="text"
            name="name"
            value={form.name}
            onChange={handleChange}
            placeholder="Ej: Apartamento Eixample Centro"
            className={errors.name ? 'input-error' : ''}
          />
          <FieldError msg={errors.name} />
        </div>

        <div className="sc-form-row-2">
          <div className="sc-form-group">
            <label>Barrio</label>
            <select name="neighbourhood" value={form.neighbourhood} onChange={handleChange}>
              {NEIGHBOURHOODS.map(n => <option key={n} value={n}>{n}</option>)}
            </select>
          </div>
          <div className="sc-form-group">
            <label>Tipo de habitación</label>
            <select name="room_type" value={form.room_type} onChange={handleChange}>
              {ROOM_TYPES.map(r => <option key={r} value={r}>{r}</option>)}
            </select>
          </div>
        </div>

        <div className="sc-form-row-3">
          <div className="sc-form-group">
            <label>Huéspedes</label>
            <input type="number" name="accommodates" min="1" max="20" value={form.accommodates} onChange={handleChange} className={errors.accommodates ? 'input-error' : ''} />
            <FieldError msg={errors.accommodates} />
          </div>
          <div className="sc-form-group">
            <label>Dormitorios</label>
            <input type="number" name="bedrooms" min="0" max="20" value={form.bedrooms} onChange={handleChange} className={errors.bedrooms ? 'input-error' : ''} />
            <FieldError msg={errors.bedrooms} />
          </div>
          <div className="sc-form-group">
            <label>Camas</label>
            <input type="number" name="beds" min="1" max="20" value={form.beds} onChange={handleChange} className={errors.beds ? 'input-error' : ''} />
            <FieldError msg={errors.beds} />
          </div>
        </div>

        <div className="sc-form-row-2">
          <div className="sc-form-group">
            <label>Nº de reseñas</label>
            <input type="number" name="number_of_reviews" min="0" value={form.number_of_reviews} onChange={handleChange} className={errors.number_of_reviews ? 'input-error' : ''} />
            <FieldError msg={errors.number_of_reviews} />
          </div>
          <div className="sc-form-group">
            <label>Puntuación media (1–5)</label>
            <input type="number" name="review_scores_rating" min="1" max="5" step="0.1" value={form.review_scores_rating} onChange={handleChange} className={errors.review_scores_rating ? 'input-error' : ''} />
            <FieldError msg={errors.review_scores_rating} />
          </div>
        </div>

        <button type="submit" className="btn btn-primary" disabled={loading} style={{ marginTop: 8 }}>
          {loading ? '⏳ Guardando...' : '💾 Guardar Inmueble'}
        </button>
      </form>
    </div>
  );
};

// ── Main page ─────────────────────────────────────────────

const Properties = () => {
  const { properties, loading, error, addProperty, removeProperty } = useProperties();

  return (
    <div>
      <div className="page-header">
        <div>
          <h1 className="page-title">Inmuebles</h1>
          <p className="page-subtitle">Registra y gestiona tus alojamientos · StayCast</p>
        </div>
        <span className="badge badge-primary" style={{ fontSize: 14, padding: '6px 14px' }}>
          {properties.length} inmueble{properties.length !== 1 ? 's' : ''}
        </span>
      </div>

      {/* Register form */}
      <RegisterForm onSave={addProperty} loading={loading} />

      {/* Error banner */}
      {error && <div className="sc-error" style={{ marginTop: 16 }}>⚠️ {error}</div>}

      {/* Property table */}
      <div className="card" style={{ marginTop: 24, overflowX: 'auto' }}>
        <h3 className="section-title" style={{ marginBottom: 16 }}>📋 Inmuebles Registrados</h3>

        {loading && properties.length === 0 ? (
          <div className="empty-state">
            <div className="sc-spinner-large" />
            <p className="empty-state-sub" style={{ marginTop: 16 }}>Cargando inmuebles...</p>
          </div>
        ) : properties.length === 0 ? (
          <div className="empty-state">
            <div className="empty-state-icon">🏠</div>
            <p><strong>No hay inmuebles registrados</strong></p>
            <p className="empty-state-sub">Usa el formulario de arriba para añadir el primero</p>
          </div>
        ) : (
          <table className="prop-table">
            <thead>
              <tr>
                <th className="prop-th">Nombre</th>
                <th className="prop-th">Barrio</th>
                <th className="prop-th">Tipo</th>
                <th className="prop-th prop-th-center">Huésp.</th>
                <th className="prop-th prop-th-center">Dorm.</th>
                <th className="prop-th prop-th-center">Camas</th>
                <th className="prop-th prop-th-center">Reseñas</th>
                <th className="prop-th prop-th-center">Rating</th>
                <th className="prop-th prop-th-center">—</th>
              </tr>
            </thead>
            <tbody>
              {properties.map(p => (
                <PropertyRow key={p.id} property={p} onDelete={removeProperty} />
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
};

export default Properties;
