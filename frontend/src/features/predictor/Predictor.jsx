import React, { useState } from 'react';
import usePrediction from '../../hooks/usePrediction';
import {
  createDefaultRequest, NEIGHBOURHOODS, ROOM_TYPES,
  getDemandLevel, getInterpretation, DEMAND_COLORS, getModelConfidence,
} from '../../models/prediction';

const saveToHistory = (request, result) => {
  try {
    const history = JSON.parse(localStorage.getItem('sc_history') || '[]');
    history.push({
      probability:   result.probability,
      date:          request.date,
      neighbourhood: request.neighbourhood,
      room_type:     request.room_type,
      timestamp:     new Date().toISOString(),
    });
    localStorage.setItem('sc_history', JSON.stringify(history.slice(-50)));
  } catch { /* ignore */ }
};

/** Toggle checkbox with label. */
const Toggle = ({ label, checked, onChange }) => (
  <label className="sc-toggle-item">
    <span className="sc-toggle-label">{label}</span>
    <div className="sc-toggle">
      <input type="checkbox" checked={checked} onChange={onChange} />
      <span className="sc-toggle-slider" />
    </div>
  </label>
);

const Predictor = () => {
  const [form, setForm] = useState(createDefaultRequest);
  const { result, loading, error, predict } = usePrediction();

  const handleChange = (e) => {
    const { name, value, type } = e.target;
    const val = type === 'number' ? Number(value) : value;
    setForm(f => ({ ...f, [name]: val }));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    const data = await predict(form);
    if (data) saveToHistory(form, data);
  };

  const demand  = result ? getDemandLevel(result.probability) : null;
  const message = result ? getInterpretation(form, result.probability) : null;
  const pct     = result ? Math.round(result.probability * 100) : 0;
  const color   = demand ? DEMAND_COLORS[demand.color] : '#1e3a5f';

  return (
    <div>
      <div className="page-header">
        <div>
          <h1 className="page-title">Predictor de Ocupación</h1>
          <p className="page-subtitle">Introduce los datos del alojamiento y el día para predecir</p>
        </div>
      </div>

      <div className="predictor-layout">

        {/* ── FORM ── */}
        <div className="card predictor-form-card">
          <form onSubmit={handleSubmit}>

            <div className="form-section-title">🏠 Datos del Alojamiento</div>

            <div className="sc-form-row-2">
              <div className="sc-form-group">
                <label>Barrio / Neighbourhood</label>
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
                <input type="number" name="accommodates" min="1" max="20" value={form.accommodates} onChange={handleChange} />
              </div>
              <div className="sc-form-group">
                <label>Precio/noche (€)</label>
                <input type="number" name="listing_price" min="1" max="2000" step="1" value={form.listing_price} onChange={handleChange} />
              </div>
              <div className="sc-form-group">
                <label>Mínimo noches</label>
                <input type="number" name="minimum_nights" min="1" max="365" value={form.minimum_nights} onChange={handleChange} />
              </div>
            </div>

            <div className="sc-form-row-2">
              <div className="sc-form-group">
                <label>Nº de reseñas</label>
                <input type="number" name="number_of_reviews" min="0" value={form.number_of_reviews} onChange={handleChange} />
              </div>
              <div className="sc-form-group">
                <label>Puntuación media (1–5)</label>
                <input type="number" name="review_scores_rating" min="1" max="5" step="0.1" value={form.review_scores_rating} onChange={handleChange} />
              </div>
            </div>

            <div className="form-section-title" style={{ marginTop: 24 }}>🗓️ Datos del Día</div>

            <div className="sc-form-row-2">
              <div className="sc-form-group">
                <label>Fecha</label>
                <input type="date" name="date" value={form.date} onChange={handleChange} />
              </div>
            </div>

            <div className="sc-toggle-row">
              <Toggle
                label="Reserva instantánea"
                checked={form.instant_bookable === true}
                onChange={e => setForm(f => ({ ...f, instant_bookable: e.target.checked }))}
              />
            </div>

            <button type="submit" className="predict-btn" disabled={loading}>
              {loading
                ? <span className="predict-btn-loading"><span className="sc-spinner" />Calculando...</span>
                : '🔮 Predecir Ocupación'}
            </button>

            {error && <div className="sc-error">⚠️ {error}</div>}
          </form>
        </div>

        {/* ── RESULT COLUMN ── */}
        <div className="predictor-result-col">
          {!result && !loading && (
            <div className="card result-placeholder">
              <div className="result-placeholder-icon">🔮</div>
              <p className="result-placeholder-title">Esperando predicción</p>
              <p className="result-placeholder-sub">
                Rellena el formulario y pulsa <strong>Predecir Ocupación</strong>
              </p>
            </div>
          )}

          {loading && (
            <div className="card result-placeholder">
              <div className="sc-spinner-large" />
              <p className="result-placeholder-sub" style={{ marginTop: 18 }}>Consultando el modelo XGBoost...</p>
            </div>
          )}

          {result && !loading && (
            <div className="card result-card">
              <div className="result-card-header">
                <span className="result-card-label">Probabilidad de ocupación</span>
                <span className={`badge badge-${demand.color}`}>{demand.label}</span>
              </div>

              <div className="result-percentage" style={{ color }}>
                {pct}<span className="result-pct-sign">%</span>
              </div>

              <div className="result-progress-track">
                <div className="result-progress-fill" style={{ width: `${pct}%`, background: color }} />
              </div>

              <div className="result-interpretation">
                <span className="interp-bulb">💡</span>
                <p>{message}</p>
              </div>

              <div className="result-details-grid">
                {[
                  ['Fecha',              form.date],
                  ['Barrio',             form.neighbourhood],
                  ['Tipo',               form.room_type],
                  ['Precio/noche',       `${form.listing_price}€`],
                  ['Mínimo noches',      form.minimum_nights],
                  ['Huéspedes',          form.accommodates],
                  ['Reserva inmediata',  form.instant_bookable ? '✅ Sí' : '❌ No'],
                  ['Confianza modelo',   getModelConfidence(result.probability)],
                ].map(([label, value]) => (
                  <div key={label} className="result-detail-item">
                    <span className="detail-label">{label}</span>
                    <span className="detail-value">{value}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default Predictor;
