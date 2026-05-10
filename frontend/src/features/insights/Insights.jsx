import React, { useState, useEffect } from 'react';
import useProperties from '../../hooks/useProperties';

const API_BASE = import.meta.env.VITE_API_URL ?? '';

const MONTH_NAMES = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'];
const DOW_NAMES   = ['Dom','Lun','Mar','Mié','Jue','Vie','Sáb'];

const forceLogout = () => {
  localStorage.removeItem('auth_token');
  localStorage.removeItem('auth_user');
  window.location.replace('/login');
};

const fetchInsights = async (neighbourhood, room_type, price, accommodates) => {
  const token = localStorage.getItem('auth_token');
  const params = new URLSearchParams({ neighbourhood, room_type });
  if (price) params.append('price', price);
  if (accommodates) params.append('accommodates', accommodates);
  const res = await fetch(`${API_BASE}/insights?${params}`, {
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
  });
  if (res.status === 401) { forceLogout(); throw new Error('HTTP 401'); }
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
};

/* ═══════════════════ Chart Components ═══════════════════ */

const BarChart = ({ data, title, subtitle, valueFormatter }) => {
  if (!data || data.length === 0) return null;
  const max = Math.max(...data.map(d => d.v));
  const fmt = valueFormatter || (v => `${Math.round(v * 100)}%`);
  return (
    <div className="chart-card card">
      <div className="chart-header">
        <h3 className="chart-title">{title}</h3>
        {subtitle && <p className="chart-subtitle">{subtitle}</p>}
      </div>
      <div className="v-bar-chart">
        {data.map((d, i) => (
          <div key={i} className="v-bar-item">
            <span className="v-bar-top-label">{fmt(d.v)}</span>
            <div className="v-bar-track">
              <div className="v-bar-fill"
                style={{
                  height: `${max > 0 ? (d.v / max) * 100 : 0}%`,
                  background: 'var(--slate-300)',
                }} />
            </div>
            <span className="v-bar-bottom-label">{d.label}</span>
          </div>
        ))}
      </div>
    </div>
  );
};


const StatCard = ({ label, value, sub }) => (
  <div className="card" style={{ padding: '20px 24px', textAlign: 'center' }}>
    <p style={{ fontSize: 13, color: 'var(--slate-500)', marginBottom: 4 }}>{label}</p>
    <p style={{ fontSize: 28, fontWeight: 700, color: 'var(--navy)', margin: 0 }}>{value}</p>
    {sub && <p style={{ fontSize: 12, color: 'var(--slate-400)', marginTop: 4 }}>{sub}</p>}
  </div>
);


/* ═══════════════════ Price-Occupancy Relationship ═══════════════════ */

const PriceOccupancyChart = ({ data, userPrice }) => {
  if (!data || data.length === 0) return null;
  const maxOcc = Math.max(...data.map(d => d.occupancy));

  return (
    <div className="chart-card card">
      <div className="chart-header">
        <h3 className="chart-title">Relación Precio–Ocupación</h3>
        <p className="chart-subtitle">¿Los más baratos se alquilan más en tu barrio?</p>
      </div>
      <div className="v-bar-chart">
        {data.map((d, i) => {
          const isUserBucket = userPrice && findBucket(userPrice) === d.bucket;
          return (
            <div key={i} className="v-bar-item">
              <span className="v-bar-top-label">{Math.round(d.occupancy * 100)}%</span>
              <div className="v-bar-track">
                <div className="v-bar-fill" style={{
                  height: `${maxOcc > 0 ? (d.occupancy / maxOcc) * 100 : 0}%`,
                  background: isUserBucket ? 'var(--navy)' : 'var(--slate-200)',
                }} />
              </div>
              <span className="v-bar-bottom-label" style={isUserBucket ? { fontWeight: 600, color: 'var(--navy)' } : {}}>
                {d.bucket}€
              </span>
              <span style={{ fontSize: 10, color: '#94a3b8' }}>({d.count})</span>
            </div>
          );
        })}
      </div>
    </div>
  );
};

const findBucket = (price) => {
  if (price < 30) return '0-30';
  if (price < 60) return '30-60';
  if (price < 90) return '60-90';
  if (price < 120) return '90-120';
  if (price < 160) return '120-160';
  if (price < 200) return '160-200';
  if (price < 300) return '200-300';
  return '300+';
};

/* ═══════════════════ Section Header ═══════════════════ */

const SectionHeader = ({ title }) => (
  <h2 style={{
    fontSize: 15, fontWeight: 600, color: 'var(--navy)', marginTop: 32, marginBottom: 16,
    paddingBottom: 8, borderBottom: '1px solid var(--border)',
  }}>{title}</h2>
);

/* ═══════════════════ Main Component ═══════════════════ */

const Insights = () => {
  const { properties, loading: propsLoading } = useProperties();
  const [selectedId, setSelectedId] = useState('');
  const [data, setData]       = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError]     = useState(null);

  const selected = properties.find(p => p.id === selectedId);

  useEffect(() => {
    if (!selected) { setData(null); return; }
    let cancelled = false;
    const load = async () => {
      setLoading(true);
      setError(null);
      try {
        const res = await fetchInsights(
          selected.neighbourhood,
          selected.room_type,
          selected.listing_price,
          selected.accommodates,
        );
        if (!cancelled) setData(res);
      } catch (e) {
        if (!cancelled) setError(e.message || 'Error al cargar insights');
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    load();
    return () => { cancelled = true; };
  }, [selected]);

  // ── Transform data ──
  const monthly = data?.monthly
    ? data.monthly.sort((a, b) => a.month - b.month).map(m => ({
        label: MONTH_NAMES[m.month - 1], v: m.occupancy, avg_price: m.avg_price, num_listings: m.num_listings,
      }))
    : [];

  const weekly = data?.weekly
    ? data.weekly.sort((a, b) => a.dow - b.dow).map(w => ({ label: DOW_NAMES[w.dow - 1], v: w.occupancy }))
    : [];

  const stats = data?.stats || {};
  const bcn = data?.bcn || {};

  return (
    <div>
      <div className="page-header">
        <div>
          <h1 className="page-title">Insights</h1>
          <p className="page-subtitle">Datos reales de ocupación en Barcelona</p>
        </div>
      </div>

      {/* Selector */}
      <div className="card" style={{ padding: '20px 24px', marginBottom: 24 }}>
        <div className="sc-form-group">
          <label>Selecciona un alojamiento para ver insights de su zona</label>
          {propsLoading ? (
            <p>Cargando inmuebles...</p>
          ) : properties.length === 0 ? (
            <p>No tienes inmuebles registrados. Añade uno primero en Inmuebles.</p>
          ) : (
            <select value={selectedId} onChange={e => setSelectedId(e.target.value)}>
              <option value="">— Selecciona un alojamiento —</option>
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
          <p style={{ marginTop: 16, color: 'var(--slate-500)' }}>Consultando BigQuery...</p>
        </div>
      )}

      {error && <div className="sc-error">{error}</div>}

      {data && !loading && (
        <>
          {/* ═══════════ Contexto del mercado ═══════════ */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 16, marginBottom: 24 }}>
            <StatCard
              label="Tu precio"
              value={`${selected.listing_price ?? '—'}€`}
              sub={`Media barrio: ${stats.avg_price ?? '—'}€`}
            />
            <StatCard
              label="Ocupación media barrio"
              value={`${Math.round((stats.avg_occupancy || 0) * 100)}%`}
              sub={`BCN: ${Math.round((bcn.avg_occupancy || 0) * 100)}%`}
            />
            <StatCard
              label="Alojamientos similares"
              value={stats.num_listings || 0}
              sub={`${selected.room_type} en ${selected.neighbourhood}`}
            />
          </div>

          {/* ═══════════ Patrones de ocupación ═══════════ */}
          <SectionHeader title="Patrones de ocupación" />

          <div className="insights-grid">
            <BarChart
              data={monthly}
              title="Ocupación por mes"
              subtitle={`${selected.room_type} en ${selected.neighbourhood}`}
            />
            <BarChart
              data={weekly}
              title="Ocupación por día de semana"
              subtitle={`${selected.room_type} en ${selected.neighbourhood}`}
            />
          </div>

          {/* ═══════════ Relación Precio-Ocupación ═══════════ */}
          <SectionHeader title="Relación Precio–Ocupación" />

          <PriceOccupancyChart
            data={data.price_occupancy}
            userPrice={selected.listing_price}
          />

        </>
      )}
    </div>
  );
};

export default Insights;
