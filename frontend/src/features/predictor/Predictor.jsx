import React, { useState, useEffect } from 'react';
import useProperties from '../../hooks/useProperties';
import usePrediction from '../../hooks/usePrediction';
import { getDemandLevel } from '../../models/prediction';

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
  const { result, loading, error, predict }   = usePrediction();

  const [selectedId, setSelectedId]       = useState('');
  const [date, setDate]                   = useState(new Date().toISOString().split('T')[0]);
  const [listingPrice, setListingPrice]   = useState('80');
  const [minimumNights, setMinimumNights] = useState('2');

  const selected = properties.find(p => p.id === selectedId);

  useEffect(() => {
    if (!selected) return;
    setListingPrice(String(selected.listing_price || 80));
    setMinimumNights(String(selected.minimum_nights || 2));
  }, [selected]);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!selected) return;

    const price  = Number(listingPrice);
    const nights = Number(minimumNights);
    const isSimulation =
      price  !== (selected.listing_price  || 80) ||
      nights !== (selected.minimum_nights || 2);

    const form = {
      apartment_id:         selectedId,
      is_simulation:        isSimulation,
      date,
      neighbourhood:        selected.neighbourhood,
      room_type:            selected.room_type,
      accommodates:         selected.accommodates,
      listing_price:        price,
      minimum_nights:       nights,
      number_of_reviews:    selected.number_of_reviews,
      review_scores_rating: selected.review_scores_rating,
      instant_bookable:     selected.instant_bookable || false,
    };

    const data = await predict(form);
    if (data) saveToHistory(form, data);
  };

  const demand = result ? getDemandLevel(result.probability) : null;
  const pct    = result ? Math.round(result.probability * 100) : 0;

  const today   = new Date().toISOString().split('T')[0];
  const maxDate = new Date(Date.now() + 13 * 86400000).toISOString().split('T')[0];

  return (
    <div>
      <div className="page-header">
        <div>
          <h1 className="page-title">Predictor</h1>
          <p className="page-subtitle">Estima la probabilidad de ocupación para cualquier fecha</p>
        </div>
      </div>

      <div className="predictor-layout">

        {/* ── Formulario ── */}
        <div className="card predictor-form-card">
          <form onSubmit={handleSubmit}>

            {/* Fila 1: Selector | Barrio */}
            <div className="sc-form-row-2">
              <div className="sc-form-group">
                <label>Alojamiento</label>
                {propsLoading ? (
                  <p style={{ fontSize: 13, color: 'var(--text-muted)' }}>Cargando...</p>
                ) : properties.length === 0 ? (
                  <p style={{ fontSize: 13, color: 'var(--text-muted)' }}>Sin inmuebles registrados.</p>
                ) : (
                  <select value={selectedId} onChange={(e) => setSelectedId(e.target.value)} required>
                    <option value="">Selecciona un alojamiento</option>
                    {properties.map(p => (
                      <option key={p.id} value={p.id}>{p.name}</option>
                    ))}
                  </select>
                )}
              </div>
              <div className="sc-form-group">
                <label>Barrio</label>
                <input type="text" value={selected?.neighbourhood ?? ''} disabled />
              </div>
            </div>

            {/* Fila 2: Tipo | Huéspedes | Dormitorios */}
            <div className="sc-form-row-3">
              <div className="sc-form-group">
                <label>Tipo de habitación</label>
                <input type="text" value={selected?.room_type ?? ''} disabled />
              </div>
              <div className="sc-form-group">
                <label>Huéspedes</label>
                <input type="text" value={selected?.accommodates ?? ''} disabled />
              </div>
              <div className="sc-form-group">
                <label>Dormitorios</label>
                <input type="text" value={selected?.bedrooms ?? ''} disabled />
              </div>
            </div>

            {/* Fila 3: Camas | Reseñas | Valoración */}
            <div className="sc-form-row-3">
              <div className="sc-form-group">
                <label>Camas</label>
                <input type="text" value={selected?.beds ?? ''} disabled />
              </div>
              <div className="sc-form-group">
                <label>Reseñas</label>
                <input type="text" value={selected?.number_of_reviews ?? ''} disabled />
              </div>
              <div className="sc-form-group">
                <label>Valoración media</label>
                <input type="text" value={selected?.review_scores_rating ?? ''} disabled />
              </div>
            </div>

            {/* Fila 4: Precio | Estancia | Reserva */}
            <div className="sc-form-row-3">
              <div className="sc-form-group">
                <label>Precio por noche (€)</label>
                <input
                  type="text" inputMode="numeric" pattern="[0-9]*"
                  value={listingPrice}
                  onChange={(e) => setListingPrice(e.target.value)}
                  disabled={!selected}
                />
              </div>
              <div className="sc-form-group">
                <label>Estancia mínima (noches)</label>
                <input
                  type="text" inputMode="numeric" pattern="[0-9]*"
                  value={minimumNights}
                  onChange={(e) => setMinimumNights(e.target.value)}
                  disabled={!selected}
                />
              </div>
              <div className="sc-form-group">
                <label>Reserva instantánea</label>
                <select value={selected?.instant_bookable ? 'true' : 'false'} disabled>
                  <option value="false">No</option>
                  <option value="true">Sí</option>
                </select>
              </div>
            </div>

            {/* Fila 5: Fecha */}
            <div className="sc-form-row-3">
              <div className="sc-form-group">
                <label>Fecha de predicción</label>
                <input
                  type="date" value={date} min={today} max={maxDate}
                  onChange={(e) => setDate(e.target.value)}
                />
              </div>
            </div>

            <button type="submit" className="predict-btn" disabled={loading || !selectedId}>
              {loading
                ? <span className="predict-btn-loading"><span className="sc-spinner" />Calculando...</span>
                : 'Predecir ocupación'}
            </button>

            {error && <div className="sc-error">{error}</div>}
          </form>
        </div>

        {/* ── Resultado ── */}
        {!result && !loading && (
          <div className="card result-placeholder">
            <p className="result-placeholder-title">Sin predicción</p>
            <p className="result-placeholder-sub">
              Selecciona un alojamiento y pulsa Predecir para obtener el resultado.
            </p>
          </div>
        )}

        {loading && (
          <div className="card result-placeholder">
            <div className="sc-spinner-large" />
            <p className="result-placeholder-sub" style={{ marginTop: 18 }}>Procesando...</p>
          </div>
        )}

        {result && !loading && (
          <div className="result-card">

            <div className="result-card-header">
              <span className="result-card-label">Probabilidad de ocupación</span>
              <span className="demand-badge">{demand.label}</span>
            </div>

            <div className="result-percentage">
              {pct}<span className="result-pct-sign">%</span>
            </div>

            <div className="result-progress-track">
              <div className="result-progress-fill" style={{ width: `${pct}%` }} />
            </div>

            <div className="result-divider" />
            <div className="result-details-grid">
              {[
                ['Alojamiento',      selected?.name],
                ['Fecha',            date],
                ['Barrio',           selected?.neighbourhood],
                ['Tipo',             selected?.room_type],
                ['Precio por noche', `${listingPrice} €`],
                ['Est. mínima',      `${minimumNights} noches`],
                ['Huéspedes',        selected?.accommodates],
                ['Reserva instant.', selected?.instant_bookable ? 'Sí' : 'No'],
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
  );
};

export default Predictor;
