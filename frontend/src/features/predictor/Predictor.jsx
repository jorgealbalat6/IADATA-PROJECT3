import React, { useState, useEffect } from 'react';
import useProperties from '../../hooks/useProperties';
import usePrediction from '../../hooks/usePrediction';
import {
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

const Predictor = () => {
  const { properties, loading: propsLoading } = useProperties();
  const { result, loading, error, predict } = usePrediction();

  const [selectedId, setSelectedId] = useState('');
  const [date, setDate]             = useState(new Date().toISOString().split('T')[0]);
  const [listingPrice, setListingPrice]     = useState(80);
  const [minimumNights, setMinimumNights]   = useState(2);
  const [wasSaved, setWasSaved]             = useState(false);

  const selected = properties.find(p => p.id === selectedId);

  // Cuando seleccionas un apartamento, carga su precio y mínimo noches
  useEffect(() => {
    if (!selected) return;
    setListingPrice(selected.listing_price || 80);
    setMinimumNights(selected.minimum_nights || 2);
  }, [selected]);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!selected) return;

    // Detectar si es simulación (precio o mín noches distintos del apartamento)
    const isSimulation =
      listingPrice !== (selected.listing_price || 80) ||
      minimumNights !== (selected.minimum_nights || 2);

    const form = {
      apartment_id:         selectedId,
      is_simulation:        isSimulation,
      date,
      neighbourhood:        selected.neighbourhood,
      room_type:            selected.room_type,
      accommodates:         selected.accommodates,
      listing_price:        listingPrice,
      minimum_nights:       minimumNights,
      number_of_reviews:    selected.number_of_reviews,
      review_scores_rating: selected.review_scores_rating,
      instant_bookable:     selected.instant_bookable || false,
    };

    setWasSaved(!isSimulation);
    const data = await predict(form);
    if (data) saveToHistory(form, data);
  };

  const demand  = result ? getDemandLevel(result.probability) : null;
  const message = result && selected ? getInterpretation({
    instant_bookable:     selected.instant_bookable || false,
    listing_price:        listingPrice,
    review_scores_rating: selected.review_scores_rating,
    accommodates:         selected.accommodates,
    number_of_reviews:    selected.number_of_reviews,
    minimum_nights:       minimumNights,
  }, result.probability) : null;
  const pct   = result ? Math.round(result.probability * 100) : 0;
  const color = demand ? DEMAND_COLORS[demand.color] : '#1e3a5f';

  const today   = new Date().toISOString().split('T')[0];
  const maxDate = new Date(Date.now() + 13 * 86400000).toISOString().split('T')[0];

  return (
    <div>
      <div className="page-header">
        <div>
          <h1 className="page-title">Predictor de Ocupación</h1>
          <p className="page-subtitle">Selecciona un alojamiento y simula diferentes escenarios</p>
        </div>
      </div>

      <div className="predictor-layout">
        <div className="card predictor-form-card">
          <form onSubmit={handleSubmit}>

            <div className="form-section-title">🏠 Selecciona tu Alojamiento</div>

            <div className="sc-form-group">
              <label>Alojamiento</label>
              {propsLoading ? (
                <p>Cargando inmuebles...</p>
              ) : properties.length === 0 ? (
                <p>No tienes inmuebles registrados. Añade uno primero.</p>
              ) : (
                <select value={selectedId} onChange={(e) => setSelectedId(e.target.value)} required>
                  <option value="">— Selecciona un alojamiento —</option>
                  {properties.map(p => (
                    <option key={p.id} value={p.id}>{p.name}</option>
                  ))}
                </select>
              )}
            </div>

            {selected && (
              <>
                <div className="sc-form-row-2" style={{ marginTop: 12 }}>
                  <div className="sc-form-group">
                    <label>Barrio</label>
                    <input type="text" value={selected.neighbourhood} disabled />
                  </div>
                  <div className="sc-form-group">
                    <label>Tipo</label>
                    <input type="text" value={selected.room_type} disabled />
                  </div>
                </div>
                <div className="sc-form-row-2">
                  <div className="sc-form-group">
                    <label>Huéspedes</label>
                    <input type="number" value={selected.accommodates} disabled />
                  </div>
                  <div className="sc-form-group">
                    <label>Reseñas</label>
                    <input type="number" value={selected.number_of_reviews} disabled />
                  </div>
                </div>
                <div className="sc-form-group">
                  <label>Reserva instantánea</label>
                  <input type="text" value={selected.instant_bookable ? '✅ Sí' : '❌ No'} disabled />
                </div>
              </>
            )}

            <div className="form-section-title" style={{ marginTop: 24 }}>🗓️ Datos de la Predicción</div>

            <div className="sc-form-row-3">
              <div className="sc-form-group">
                <label>Fecha</label>
                <input type="date" value={date} min={today} max={maxDate}
                  onChange={(e) => setDate(e.target.value)} />
              </div>
              <div className="sc-form-group">
                <label>Precio/noche (€)</label>
                <input type="number" min="1" max="2000" step="1" value={listingPrice}
                  onChange={(e) => setListingPrice(Number(e.target.value))} />
              </div>
              <div className="sc-form-group">
                <label>Mínimo noches</label>
                <input type="number" min="1" max="365" value={minimumNights}
                  onChange={(e) => setMinimumNights(Number(e.target.value))} />
              </div>
            </div>

            <button type="submit" className="predict-btn" disabled={loading || !selectedId}>
              {loading
                ? <span className="predict-btn-loading"><span className="sc-spinner" />Calculando...</span>
                : '🔮 Predecir Ocupación'}
            </button>

            {error && <div className="sc-error">⚠️ {error}</div>}
          </form>
        </div>

        <div className="predictor-result-col">
          {!result && !loading && (
            <div className="card result-placeholder">
              <div className="result-placeholder-icon">🔮</div>
              <p className="result-placeholder-title">Esperando predicción</p>
              <p className="result-placeholder-sub">
                Selecciona un alojamiento y pulsa <strong>Predecir Ocupación</strong>
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
                  ['Alojamiento',        selected?.name],
                  ['Fecha',              date],
                  ['Barrio',             selected?.neighbourhood],
                  ['Tipo',               selected?.room_type],
                  ['Precio/noche',       `${listingPrice}€`],
                  ['Mínimo noches',      minimumNights],
                  ['Huéspedes',          selected?.accommodates],
                  ['Reserva inmediata',  selected?.instant_bookable ? '✅ Sí' : '❌ No'],
                  ['Confianza modelo',   getModelConfidence(result.probability)],
                ].map(([label, value]) => (
                  <div key={label} className="result-detail-item">
                    <span className="detail-label">{label}</span>
                    <span className="detail-value">{value}</span>
                  </div>
                ))}
              </div>

              {/* Aviso informativo */}
              <div style={{
                marginTop: 20,
                padding: '14px 16px',
                background: wasSaved ? '#f0fdf4' : '#f0f4ff',
                borderRadius: 8,
                borderLeft: `4px solid ${wasSaved ? '#16a34a' : '#1e3a5f'}`,
                fontSize: 13,
                lineHeight: 1.5,
                color: '#334155',
              }}>
                {wasSaved ? (
                  <>
                    <strong>✅ Predicción guardada.</strong> Se ha registrado en el historial de <strong>{selected?.name}</strong> para el {date}.
                  </>
                ) : (
                  <>
                    <strong>ℹ️ Simulación.</strong> Has modificado el precio o mínimo de noches, por lo que esta predicción no se guarda.
                    Para que se registre, usa los valores reales de tu alojamiento o edítalos en <strong>Inmuebles → ✏️ Editar Inmueble</strong>.
                  </>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default Predictor;