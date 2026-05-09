import React, { useState } from 'react';
import useProperties from '../../hooks/useProperties';
import {
  createDefaultProperty,
  NEIGHBOURHOODS,
  ROOM_TYPES,
} from '../../models/property';

// ── Helpers ──────────────────────────────────────────────

const toNum = (v, fallback = 0) => {
  const n = Number(v);
  return isNaN(n) ? fallback : n;
};

const parseFormNumbers = (form) => ({
  ...form,
  accommodates:         toNum(form.accommodates, 1),
  bedrooms:             toNum(form.bedrooms, 0),
  beds:                 toNum(form.beds, 1),
  number_of_reviews:    toNum(form.number_of_reviews, 0),
  review_scores_rating: toNum(form.review_scores_rating, 4.5),
  listing_price:        toNum(form.listing_price, 80),
  minimum_nights:       toNum(form.minimum_nights, 1),
});

// ── Validation ────────────────────────────────────────────

const validate = (form) => {
  const errors = {};
  if (!form.name?.trim())
    errors.name = 'El nombre del alojamiento es obligatorio.';
  else if (form.name.trim().length < 3)
    errors.name = 'El nombre debe tener al menos 3 caracteres.';
  if (toNum(form.accommodates) < 1 || toNum(form.accommodates) > 20)
    errors.accommodates = 'Entre 1 y 20.';
  if (toNum(form.bedrooms) < 0 || toNum(form.bedrooms) > 20)
    errors.bedrooms = 'Entre 0 y 20.';
  if (toNum(form.beds) < 1 || toNum(form.beds) > 20)
    errors.beds = 'Entre 1 y 20.';
  if (toNum(form.number_of_reviews) < 0)
    errors.number_of_reviews = 'No puede ser negativo.';
  if (toNum(form.review_scores_rating) < 1 || toNum(form.review_scores_rating) > 5)
    errors.review_scores_rating = 'Entre 1 y 5.';
  if (toNum(form.listing_price) < 1)
    errors.listing_price = 'Debe ser mayor que 0.';
  if (toNum(form.minimum_nights) < 1)
    errors.minimum_nights = 'Mínimo 1 noche.';
  return errors;
};

const FieldError = ({ msg }) =>
  msg ? <span className="field-error">{msg}</span> : null;

// ── Property Row ─────────────────────────────────────────

const PropertyRow = ({ property, onDelete }) => (
  <tr className="prop-row">
    <td className="prop-cell prop-cell-name">{property.name || '—'}</td>
    <td className="prop-cell">{property.neighbourhood}</td>
    <td className="prop-cell">{property.room_type}</td>
    <td className="prop-cell prop-cell-center">{property.accommodates}</td>
    <td className="prop-cell prop-cell-center">{property.bedrooms}</td>
    <td className="prop-cell prop-cell-center">{property.beds}</td>
    <td className="prop-cell prop-cell-center">{property.number_of_reviews}</td>
    <td className="prop-cell prop-cell-center">{property.review_scores_rating ?? '—'}</td>
    <td className="prop-cell prop-cell-center">{property.listing_price}€</td>
    <td className="prop-cell prop-cell-center">{property.minimum_nights}</td>
    <td className="prop-cell prop-cell-center">{property.instant_bookable ? 'Sí' : 'No'}</td>
    <td className="prop-cell prop-cell-center">
      <button
        className="btn-icon btn-icon-danger"
        onClick={() => onDelete(property.id)}
        title="Eliminar inmueble"
      >
        ×
      </button>
    </td>
  </tr>
);

// ── Shared form fields ────────────────────────────────────

const PropertyFields = ({ form, errors, handleChange, disabled = false }) => (
  <>
    {/* Row 1: Nombre | Barrio */}
    <div className="sc-form-row-2">
      <div className="sc-form-group">
        <label>Nombre / Título del alojamiento</label>
        <input
          type="text"
          name="name"
          value={form.name}
          onChange={handleChange}
          disabled={disabled}
          placeholder="Ej: Apartamento Eixample Centro"
          className={errors.name ? 'input-error' : ''}
        />
        <FieldError msg={errors.name} />
      </div>
      <div className="sc-form-group">
        <label>Barrio</label>
        <select name="neighbourhood" value={form.neighbourhood} onChange={handleChange} disabled={disabled}>
          {NEIGHBOURHOODS.map(n => <option key={n} value={n}>{n}</option>)}
        </select>
      </div>
    </div>

    {/* Row 2: Tipo | Huéspedes | Dormitorios */}
    <div className="sc-form-row-3">
      <div className="sc-form-group">
        <label>Tipo de habitación</label>
        <select name="room_type" value={form.room_type} onChange={handleChange} disabled={disabled}>
          {ROOM_TYPES.map(r => <option key={r} value={r}>{r}</option>)}
        </select>
      </div>
      <div className="sc-form-group">
        <label>Huéspedes</label>
        <input
          type="text" inputMode="numeric" pattern="[0-9]*"
          name="accommodates" value={form.accommodates} onChange={handleChange} disabled={disabled}
          className={errors.accommodates ? 'input-error' : ''}
        />
        <FieldError msg={errors.accommodates} />
      </div>
      <div className="sc-form-group">
        <label>Dormitorios</label>
        <input
          type="text" inputMode="numeric" pattern="[0-9]*"
          name="bedrooms" value={form.bedrooms} onChange={handleChange} disabled={disabled}
          className={errors.bedrooms ? 'input-error' : ''}
        />
        <FieldError msg={errors.bedrooms} />
      </div>
    </div>

    {/* Row 3: Camas | Reseñas | Valoración */}
    <div className="sc-form-row-3">
      <div className="sc-form-group">
        <label>Camas</label>
        <input
          type="text" inputMode="numeric" pattern="[0-9]*"
          name="beds" value={form.beds} onChange={handleChange} disabled={disabled}
          className={errors.beds ? 'input-error' : ''}
        />
        <FieldError msg={errors.beds} />
      </div>
      <div className="sc-form-group">
        <label>Nº de reseñas</label>
        <input
          type="text" inputMode="numeric" pattern="[0-9]*"
          name="number_of_reviews" value={form.number_of_reviews} onChange={handleChange} disabled={disabled}
          className={errors.number_of_reviews ? 'input-error' : ''}
        />
        <FieldError msg={errors.number_of_reviews} />
      </div>
      <div className="sc-form-group">
        <label>Valoración media (1–5)</label>
        <input
          type="text" inputMode="decimal" pattern="[0-9.]*"
          name="review_scores_rating" value={form.review_scores_rating} onChange={handleChange} disabled={disabled}
          className={errors.review_scores_rating ? 'input-error' : ''}
        />
        <FieldError msg={errors.review_scores_rating} />
      </div>
    </div>

    {/* Row 4: Precio | Mínimo noches | Reserva instantánea */}
    <div className="sc-form-row-3">
      <div className="sc-form-group">
        <label>Precio/noche (€)</label>
        <input
          type="text" inputMode="numeric" pattern="[0-9]*"
          name="listing_price" value={form.listing_price} onChange={handleChange} disabled={disabled}
          className={errors.listing_price ? 'input-error' : ''}
        />
        <FieldError msg={errors.listing_price} />
      </div>
      <div className="sc-form-group">
        <label>Estancia mínima (noches)</label>
        <input
          type="text" inputMode="numeric" pattern="[0-9]*"
          name="minimum_nights" value={form.minimum_nights} onChange={handleChange} disabled={disabled}
          className={errors.minimum_nights ? 'input-error' : ''}
        />
        <FieldError msg={errors.minimum_nights} />
      </div>
      <div className="sc-form-group">
        <label>Reserva instantánea</label>
        <select
          name="instant_bookable"
          value={form.instant_bookable ? 'true' : 'false'}
          onChange={handleChange}
          disabled={disabled}
        >
          <option value="false">No</option>
          <option value="true">Sí</option>
        </select>
      </div>
    </div>
  </>
);

// ── Register Form ────────────────────────────────────────

const RegisterForm = ({ onSave, loading }) => {
  const [form, setForm] = useState(createDefaultProperty);
  const [errors, setErrors] = useState({});

  const handleChange = (e) => {
    const { name, value, type, checked } = e.target;
    let val;
    if (name === 'instant_bookable') val = value === 'true';
    else if (type === 'checkbox') val = checked;
    else val = value;
    setForm(f => ({ ...f, [name]: val }));
    if (errors[name]) setErrors(prev => ({ ...prev, [name]: undefined }));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    const parsed = parseFormNumbers(form);
    const found = validate(parsed);
    if (Object.keys(found).length > 0) { setErrors(found); return; }
    setErrors({});
    const saved = await onSave(parsed);
    if (saved) setForm(createDefaultProperty());
  };

  return (
    <div className="card prop-form-card">
      <h3 className="section-title" style={{ marginBottom: 16 }}>Registrar inmueble</h3>
      <form onSubmit={handleSubmit}>
        <PropertyFields form={form} errors={errors} handleChange={handleChange} />
        <button type="submit" className="btn btn-primary" disabled={loading} style={{ marginTop: 8 }}>
          {loading ? 'Guardando...' : 'Guardar inmueble'}
        </button>
      </form>
    </div>
  );
};

// ── Edit Form ────────────────────────────────────────────

const EditForm = ({ properties, onEdit, loading }) => {
  const [selectedId, setSelectedId] = useState('');
  const [form, setForm] = useState(null);
  const [errors, setErrors] = useState({});
  const [saved, setSaved] = useState(false);

  const handleSelect = (e) => {
    const id = e.target.value;
    setSelectedId(id);
    setSaved(false);
    if (!id) { setForm(null); return; }
    const apt = properties.find(p => p.id === id);
    if (apt) setForm({ ...apt });
  };

  const handleChange = (e) => {
    const { name, value, type, checked } = e.target;
    let val;
    if (name === 'instant_bookable') val = value === 'true';
    else if (type === 'checkbox') val = checked;
    else val = value;
    setForm(f => ({ ...f, [name]: val }));
    setSaved(false);
    if (errors[name]) setErrors(prev => ({ ...prev, [name]: undefined }));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    const parsed = parseFormNumbers(form);
    const found = validate(parsed);
    if (Object.keys(found).length > 0) { setErrors(found); return; }
    setErrors({});
    const ok = await onEdit(selectedId, parsed);
    if (ok) setSaved(true);
  };

  return (
    <div className="card prop-form-card" style={{ marginTop: 24 }}>
      <h3 className="section-title" style={{ marginBottom: 16 }}>Editar inmueble</h3>

      <div className="sc-form-group">
        <label>Selecciona un alojamiento</label>
        {properties.length === 0 ? (
          <p>No tienes inmuebles registrados.</p>
        ) : (
          <select value={selectedId} onChange={handleSelect}>
            <option value="">— Selecciona —</option>
            {properties.map(p => (
              <option key={p.id} value={p.id}>{p.name}</option>
            ))}
          </select>
        )}
      </div>

      {form && (
        <form onSubmit={handleSubmit} style={{ marginTop: 12 }}>
          <PropertyFields form={form} errors={errors} handleChange={handleChange} />
          <button type="submit" className="btn btn-primary" disabled={loading} style={{ marginTop: 8 }}>
            {loading ? 'Guardando...' : 'Guardar cambios'}
          </button>
          {saved && <p style={{ color: 'var(--success)', marginTop: 8 }}>Cambios guardados correctamente</p>}
        </form>
      )}
    </div>
  );
};

// ── Main page ─────────────────────────────────────────────

const Properties = () => {
  const { properties, loading, error, addProperty, removeProperty, editProperty } = useProperties();

  return (
    <div>
      <div className="page-header">
        <div>
          <h1 className="page-title">Inmuebles</h1>
          <p className="page-subtitle">Registra y gestiona tus alojamientos</p>
        </div>
        <span className="badge badge-primary" style={{ fontSize: 14, padding: '6px 14px' }}>
          {properties.length} inmueble{properties.length !== 1 ? 's' : ''}
        </span>
      </div>

      <RegisterForm onSave={addProperty} loading={loading} />

      <EditForm properties={properties} onEdit={editProperty} loading={loading} />

      {error && <div className="sc-error" style={{ marginTop: 16 }}>{error}</div>}

      <div className="card" style={{ marginTop: 24, overflowX: 'auto' }}>
        <h3 className="section-title" style={{ marginBottom: 16 }}>Inmuebles registrados</h3>

        {loading && properties.length === 0 ? (
          <div className="empty-state">
            <div className="sc-spinner-large" />
            <p className="empty-state-sub" style={{ marginTop: 16 }}>Cargando inmuebles...</p>
          </div>
        ) : properties.length === 0 ? (
          <div className="empty-state">
            <p><strong>No hay inmuebles registrados</strong></p>
            <p className="empty-state-sub">Usa el formulario de arriba para añadir el primero</p>
          </div>
        ) : (
          <table className="prop-table">
            <thead>
              <tr>
                <th className="prop-th">Nombre</th>
                <th className="prop-th">Barrio</th>
                <th className="prop-th">Tipo de habitación</th>
                <th className="prop-th prop-th-center">Huéspedes</th>
                <th className="prop-th prop-th-center">Dormitorios</th>
                <th className="prop-th prop-th-center">Camas</th>
                <th className="prop-th prop-th-center">Nº reseñas</th>
                <th className="prop-th prop-th-center">Valoración</th>
                <th className="prop-th prop-th-center">Precio/noche (€)</th>
                <th className="prop-th prop-th-center">Est. mínima</th>
                <th className="prop-th prop-th-center">Reserva instant.</th>
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
