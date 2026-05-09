import React, { useState, useEffect } from 'react';
import useProperties from '../../hooks/useProperties';
import { DEMAND_COLORS, getDemandLevel } from '../../models/prediction';

const API_BASE = import.meta.env.VITE_API_URL ?? '';

const MONTH_NAMES = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'];
const DOW_NAMES   = ['Dom','Lun','Mar','Mié','Jue','Vie','Sáb'];

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
                  background: d.color || DEMAND_COLORS[getDemandLevel(d.v).color] || '#3b82f6',
                }} />
            </div>
            <span className="v-bar-bottom-label">{d.label}</span>
          </div>
        ))}
      </div>
    </div>
  );
};

const HBarChart = ({ data, title, subtitle, highlight }) => {
  if (!data || data.length === 0) return null;
  const max = Math.max(...data.map(d => d.v));
  return (
    <div className="chart-card card">
      <div className="chart-header">
        <h3 className="chart-title">{title}</h3>
        {subtitle && <p className="chart-subtitle">{subtitle}</p>}
      </div>
      <div className="h-bar-chart">
        {data.map((d, i) => {
          const isHighlight = d.label === highlight;
          return (
            <div key={i} className="h-bar-item">
              <span className="h-bar-label" style={isHighlight ? { fontWeight: 700 } : {}}>
                {isHighlight ? `▸ ${d.label}` : d.label}
              </span>
              <div className="h-bar-track">
                <div className="h-bar-fill" style={{
                  width: `${max > 0 ? (d.v / max) * 100 : 0}%`,
                  background: isHighlight ? DEMAND_COLORS.primary : '#93c5fd',
                }} />
              </div>
              <span className="h-bar-value">{Math.round(d.v * 100)}%</span>
            </div>
          );
        })}
      </div>
    </div>
  );
};

const StatCard = ({ label, value, sub }) => (
  <div className="card" style={{ padding: '20px 24px', textAlign: 'center' }}>
    <p style={{ fontSize: 13, color: '#64748b', marginBottom: 4 }}>{label}</p>
    <p style={{ fontSize: 28, fontWeight: 700, color: '#1e3a5f', margin: 0 }}>{value}</p>
    {sub && <p style={{ fontSize: 12, color: '#94a3b8', marginTop: 4 }}>{sub}</p>}
  </div>
);

/* ═══════════════════ Price Position Gauge ═══════════════════ */

const PriceGauge = ({ percentile, userPrice, stats }) => {
  if (percentile == null) return null;
  const pct = Math.max(0, Math.min(100, percentile));
  const label = pct < 25 ? 'Muy barato' : pct < 45 ? 'Barato' : pct < 55 ? 'En la media' : pct < 75 ? 'Caro' : 'Muy caro';
  const color = pct < 25 ? '#16a34a' : pct < 45 ? '#65a30d' : pct < 55 ? '#ca8a04' : pct < 75 ? '#ea580c' : '#dc2626';

  return (
    <div className="card" style={{ padding: '24px' }}>
      <h3 className="chart-title" style={{ marginBottom: 16 }}>Tu posición de precio en el barrio</h3>
      <div style={{ position: 'relative', height: 40, background: 'linear-gradient(to right, #16a34a, #65a30d, #ca8a04, #ea580c, #dc2626)', borderRadius: 8, marginBottom: 12 }}>
        <div style={{
          position: 'absolute', left: `${pct}%`, top: -8, transform: 'translateX(-50%)',
          width: 0, height: 0, borderLeft: '8px solid transparent', borderRight: '8px solid transparent', borderTop: '12px solid #1e3a5f',
        }} />
        <div style={{
          position: 'absolute', left: `${pct}%`, bottom: -24, transform: 'translateX(-50%)',
          fontSize: 13, fontWeight: 700, color: '#1e3a5f', whiteSpace: 'nowrap',
        }}>
          {userPrice}€ — {label}
        </div>
        {/* Labels */}
        <span style={{ position: 'absolute', left: 4, top: '50%', transform: 'translateY(-50%)', fontSize: 11, color: '#fff', fontWeight: 600 }}>Barato</span>
        <span style={{ position: 'absolute', right: 4, top: '50%', transform: 'translateY(-50%)', fontSize: 11, color: '#fff', fontWeight: 600 }}>Caro</span>
      </div>
      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, color: '#64748b', marginTop: 32 }}>
        <span>P10: {stats.p10_price}€</span>
        <span>P25: {stats.p25_price}€</span>
        <span>Mediana: {stats.median_price}€</span>
        <span>P75: {stats.p75_price}€</span>
        <span>P90: {stats.p90_price}€</span>
      </div>
    </div>
  );
};

/* ═══════════════════ Comparison Table ═══════════════════ */

const CompareTable = ({ selected, stats, bcn }) => {
  if (!stats || !bcn) return null;
  const rows = [
    { label: 'Precio medio/noche', yours: `${selected.listing_price ?? '—'}€`, zone: `${stats.avg_price}€`, bcn: `${bcn.avg_price}€` },
    { label: 'Ocupación media', yours: '—', zone: `${Math.round(stats.avg_occupancy * 100)}%`, bcn: `${Math.round(bcn.avg_occupancy * 100)}%` },
    { label: 'Nº alojamientos', yours: '—', zone: stats.num_listings, bcn: bcn.num_listings },
  ];

  return (
    <div className="card" style={{ padding: '24px' }}>
      <h3 className="chart-title" style={{ marginBottom: 16 }}>Tu apartamento vs la competencia</h3>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 14 }}>
        <thead>
          <tr style={{ borderBottom: '2px solid #e2e8f0' }}>
            <th style={{ textAlign: 'left', padding: '8px 12px', color: '#64748b' }}></th>
            <th style={{ textAlign: 'center', padding: '8px 12px', color: '#1e3a5f', fontWeight: 700 }}>Tu alojamiento</th>
            <th style={{ textAlign: 'center', padding: '8px 12px', color: '#64748b' }}>Tu barrio</th>
            <th style={{ textAlign: 'center', padding: '8px 12px', color: '#64748b' }}>Barcelona</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} style={{ borderBottom: '1px solid #f1f5f9' }}>
              <td style={{ padding: '10px 12px', fontWeight: 500 }}>{r.label}</td>
              <td style={{ padding: '10px 12px', textAlign: 'center', fontWeight: 700, color: '#1e3a5f' }}>{r.yours}</td>
              <td style={{ padding: '10px 12px', textAlign: 'center' }}>{r.zone}</td>
              <td style={{ padding: '10px 12px', textAlign: 'center' }}>{r.bcn}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

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
                  background: isUserBucket ? '#1e3a5f' : '#93c5fd',
                }} />
              </div>
              <span className="v-bar-bottom-label" style={isUserBucket ? { fontWeight: 700, color: '#1e3a5f' } : {}}>
                {d.bucket}€
              </span>
              <span style={{ fontSize: 10, color: '#94a3b8' }}>({d.count})</span>
            </div>
          );
        })}
      </div>
      {userPrice && (
        <p style={{ fontSize: 12, color: '#64748b', textAlign: 'center', marginTop: 8 }}>
          Tu precio ({userPrice}€) resaltado en azul oscuro · Entre paréntesis: nº de listings
        </p>
      )}
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
    fontSize: 18, fontWeight: 700, color: '#1e3a5f', marginTop: 32, marginBottom: 16,
    paddingBottom: 8, borderBottom: '2px solid #e2e8f0',
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

  const neighbourhoods = data?.neighbourhood
    ? data.neighbourhood.map(n => ({ label: n.name, v: n.occupancy, count: n.count }))
    : [];

  const competitionMonthly = data?.monthly
    ? data.monthly.sort((a, b) => a.month - b.month).map(m => ({
        label: MONTH_NAMES[m.month - 1], v: m.num_listings,
      }))
    : [];

  const priceMonthly = data?.monthly
    ? data.monthly.sort((a, b) => a.month - b.month).map(m => ({
        label: MONTH_NAMES[m.month - 1], v: m.avg_price,
      }))
    : [];

  const stats = data?.stats || {};
  const bcn = data?.bcn || {};

  return (
    <div>
      <div className="page-header">
        <div>
          <h1 className="page-title">Insights del Mercado</h1>
          <p className="page-subtitle">Datos reales de ocupación en Barcelona · Inside Airbnb</p>
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
          <p style={{ marginTop: 16, color: '#64748b' }}>Consultando BigQuery...</p>
        </div>
      )}

      {error && <div className="sc-error">⚠️ {error}</div>}

      {data && !loading && (
        <>
          {/* ═══════════ SECCIÓN 1: Tu apartamento vs competencia ═══════════ */}
          <SectionHeader title="Tu apartamento vs la competencia" />

          {/* KPIs */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 16, marginBottom: 24 }}>
            <StatCard
              label="Tu precio"
              value={`${selected.listing_price ?? '—'}€`}
              sub={`Media barrio: ${stats.avg_price}€`}
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
            <StatCard
              label="Precio mediano"
              value={`${stats.median_price || '—'}€`}
              sub={`BCN: ${bcn.median_price || '—'}€`}
            />
          </div>

          {/* Comparison table */}
          <CompareTable selected={selected} stats={stats} bcn={bcn} />

          {/* Price gauge */}
          <div style={{ marginTop: 16 }}>
            <PriceGauge
              percentile={data.user_price_percentile}
              userPrice={selected.listing_price}
              stats={stats}
            />
          </div>

          {/* ═══════════ SECCIÓN 2: Ocupación ═══════════ */}
          <SectionHeader title="Patrones de ocupación" />

          <div className="insights-grid">
            <BarChart
              data={monthly}
              title="Ocupación por Mes"
              subtitle={`${selected.room_type} en ${selected.neighbourhood}`}
            />
            <BarChart
              data={weekly}
              title="Ocupación por Día de Semana"
              subtitle={`${selected.room_type} en ${selected.neighbourhood}`}
            />
          </div>

          {/* Neighbourhood ranking */}
          <div style={{ marginTop: 16 }}>
            <HBarChart
              data={neighbourhoods}
              title="Ranking de Barrios"
              subtitle={`Ocupación media para ${selected.room_type} — Top 15`}
              highlight={selected.neighbourhood}
            />
          </div>

          {/* ═══════════ SECCIÓN 3: Tendencias ═══════════ */}
          <SectionHeader title="Tendencias" />

          <div className="insights-grid">
            <BarChart
              data={competitionMonthly}
              title="Competencia por Mes"
              subtitle="Nº de listings activos en tu barrio"
              valueFormatter={v => String(v)}
            />
            <BarChart
              data={priceMonthly}
              title="Precio Medio por Mes"
              subtitle={`${selected.neighbourhood} · Tu precio: ${selected.listing_price ?? '—'}€`}
              valueFormatter={v => `${Math.round(v)}€`}
            />
          </div>

          {/* ═══════════ SECCIÓN 4: Relación Precio-Ocupación ═══════════ */}
          <SectionHeader title="Relación Precio–Ocupación" />

          <PriceOccupancyChart
            data={data.price_occupancy}
            userPrice={selected.listing_price}
          />

          <p style={{ marginTop: 24, fontSize: 12, color: '#94a3b8', textAlign: 'center' }}>
            Fuente: Inside Airbnb Barcelona · Datos del último snapshot disponible
          </p>
        </>
      )}
    </div>
  );
};

export default Insights;
