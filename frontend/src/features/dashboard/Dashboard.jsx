import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import useProperties from '../../hooks/useProperties';
import { getDemandLevel, DEMAND_COLORS } from '../../models/prediction';

const API_BASE = import.meta.env.VITE_API_URL ?? '';

const DAY_NAMES   = ['Dom', 'Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb'];
const MONTH_NAMES = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'];

const fetchUpcoming = async (apartmentId) => {
  const token = localStorage.getItem('auth_token');
  const res = await fetch(`${API_BASE}/apartments/${apartmentId}/predictions/upcoming`, {
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
};

/* ═══════════ Calendar Day Cell ═══════════ */

const DayCell = ({ date, prediction }) => {
  const d        = new Date(date + 'T00:00:00');
  const dayNum   = d.getDate();
  const dayName  = DAY_NAMES[d.getDay()];
  const monthName = MONTH_NAMES[d.getMonth()];
  const isWeekend = d.getDay() === 0 || d.getDay() === 6;

  if (!prediction) {
    return (
      <div className="cal-day cal-day-empty" style={{ opacity: 0.5 }}>
        <div className="cal-day-header">
          <span className="cal-day-name">{dayName}</span>
          <span className="cal-day-num">{dayNum} {monthName}</span>
        </div>
        <div className="cal-day-body">
          <span style={{ fontSize: 13, color: 'var(--slate-400)' }}>Sin datos</span>
        </div>
      </div>
    );
  }

  const prob   = prediction.probability;
  const pct    = Math.round(prob * 100);
  const demand = getDemandLevel(prob);
  const color  = DEMAND_COLORS[demand.color];

  return (
    <div className="cal-day" style={{
      borderLeft: `3px solid ${color}`,
      background: isWeekend ? 'var(--slate-50)' : 'var(--card)',
    }}>
      <div className="cal-day-header">
        <span className="cal-day-name" style={{ fontWeight: isWeekend ? 600 : 500 }}>{dayName}</span>
        <span className="cal-day-num">{dayNum} {monthName}</span>
      </div>
      <div className="cal-day-body">
        <span className="cal-day-pct" style={{ color }}>{pct}%</span>
        <span className={`badge badge-${demand.color}`} style={{ fontSize: 10, padding: '2px 8px' }}>
          {demand.label}
        </span>
      </div>
      <div className="cal-day-bar">
        <div className="cal-day-bar-fill" style={{ width: `${pct}%`, background: color }} />
      </div>
    </div>
  );
};

/* ═══════════ KPI Summary ═══════════ */

const KPISummary = ({ predictions }) => {
  if (!predictions || predictions.length === 0) return null;

  const probs    = predictions.map(p => p.probability);
  const avg      = probs.reduce((a, b) => a + b, 0) / probs.length;
  const max      = Math.max(...probs);
  const min      = Math.min(...probs);
  const highDays = probs.filter(p => p >= 0.7).length;
  const avgDemand = getDemandLevel(avg);

  return (
    <div className="kpi-grid" style={{ marginBottom: 20 }}>
      <div className="kpi-card">
        <div className="kpi-title">Ocupación media</div>
        <div className="kpi-value" style={{ color: DEMAND_COLORS[avgDemand.color] }}>
          {Math.round(avg * 100)}%
        </div>
      </div>
      <div className="kpi-card">
        <div className="kpi-title">Día más alto</div>
        <div className="kpi-value">{Math.round(max * 100)}%</div>
      </div>
      <div className="kpi-card">
        <div className="kpi-title">Días alta demanda</div>
        <div className="kpi-value">
          {highDays} <span style={{ fontSize: 14, fontWeight: 400, color: 'var(--slate-400)' }}>de 14</span>
        </div>
      </div>
    </div>
  );
};

/* ═══════════ Main Dashboard ═══════════ */

const Dashboard = () => {
  const navigate = useNavigate();
  const { properties, loading: propsLoading } = useProperties();
  const [selectedId, setSelectedId] = useState('');
  const [predictions, setPredictions] = useState([]);
  const [loading, setLoading]   = useState(false);
  const [error, setError]       = useState(null);

  const selected = properties.find(p => p.id === selectedId);

  useEffect(() => {
    if (properties.length > 0 && !selectedId) {
      setSelectedId(properties[0].id);
    }
  }, [properties]);

  useEffect(() => {
    if (!selectedId) { setPredictions([]); return; }
    let cancelled = false;
    const load = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await fetchUpcoming(selectedId);
        if (!cancelled) setPredictions(data);
      } catch (e) {
        if (!cancelled) setError(e.message);
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    load();
    return () => { cancelled = true; };
  }, [selectedId]);

  const today = new Date();
  const days  = Array.from({ length: 14 }, (_, i) => {
    const d = new Date(today);
    d.setDate(d.getDate() + i);
    return d.toISOString().split('T')[0];
  });

  const predMap = {};
  predictions.forEach(p => { predMap[p.date] = p; });

  const hasPredictions = predictions.length > 0;

  return (
    <div>
      <div className="page-header">
        <div>
          <h1 className="page-title">Dashboard</h1>
          <p className="page-subtitle">Predicciones de ocupación · Próximos 14 días</p>
        </div>
        <button className="btn btn-primary" onClick={() => navigate('/predictor')}>Nueva predicción</button>
      </div>

      {/* Selector de apartamento */}
      <div className="card" style={{ padding: '16px 24px', marginBottom: 20 }}>
        <div className="sc-form-group" style={{ marginBottom: 0 }}>
          <label>Alojamiento</label>
          {propsLoading ? (
            <p>Cargando inmuebles...</p>
          ) : properties.length === 0 ? (
            <div style={{ textAlign: 'center', padding: '20px 0' }}>
              <p>No tienes inmuebles registrados.</p>
              <button className="btn btn-primary" style={{ marginTop: 8 }} onClick={() => navigate('/properties')}>
                Añadir inmueble
              </button>
            </div>
          ) : (
            <select value={selectedId} onChange={e => setSelectedId(e.target.value)}>
              {properties.map(p => (
                <option key={p.id} value={p.id}>
                  {p.name} — {p.neighbourhood} ({p.room_type})
                </option>
              ))}
            </select>
          )}
        </div>
      </div>

      {loading && (
        <div className="card" style={{ padding: 40, textAlign: 'center' }}>
          <div className="sc-spinner-large" />
          <p style={{ marginTop: 16, color: 'var(--slate-500)' }}>Cargando predicciones...</p>
        </div>
      )}

      {error && <div className="sc-error">{error}</div>}

      {!loading && selectedId && (
        <>
          {hasPredictions && <KPISummary predictions={predictions} />}

          <div className="card" style={{ padding: '24px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
              <h2 style={{ fontSize: 15, fontWeight: 600, color: 'var(--navy)', margin: 0 }}>
                Calendario de ocupación
              </h2>
              {selected && (
                <span style={{ fontSize: 13, color: 'var(--slate-500)' }}>
                  {selected.name} · {selected.neighbourhood}
                </span>
              )}
            </div>

            {!hasPredictions ? (
              <div className="empty-state">
                <p><strong>Sin predicciones aún</strong></p>
                <p className="empty-state-sub">
                  Las predicciones se generan automáticamente cada día a las 6:00 AM, o puedes crear una manualmente.
                </p>
                <button className="btn btn-primary" style={{ marginTop: 16 }} onClick={() => navigate('/predictor')}>
                  Ir al predictor
                </button>
              </div>
            ) : (
              <div className="cal-grid">
                {days.map(date => (
                  <DayCell key={date} date={date} prediction={predMap[date]} />
                ))}
              </div>
            )}
          </div>

          {hasPredictions && (
            <div style={{ display: 'flex', gap: 16, justifyContent: 'center', marginTop: 14, fontSize: 12, color: 'var(--slate-500)' }}>
              {[
                { label: 'Baja',  color: DEMAND_COLORS.danger  },
                { label: 'Media', color: DEMAND_COLORS.warning  },
                { label: 'Alta',  color: DEMAND_COLORS.success  },
              ].map(item => (
                <div key={item.label} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                  <div style={{ width: 10, height: 10, borderRadius: 2, background: item.color }} />
                  <span>{item.label}</span>
                </div>
              ))}
            </div>
          )}
        </>
      )}
    </div>
  );
};

export default Dashboard;
