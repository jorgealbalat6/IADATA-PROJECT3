import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { getDemandLevel, DEMAND_COLORS, getModelConfidence } from '../../models/prediction';

const KPICard = ({ title, value, subtitle, colorVar, icon }) => (
  <div className="kpi-card">
    <div className="kpi-icon" style={{ background: `${colorVar}18`, color: colorVar }}>
      {icon}
    </div>
    <div className="kpi-body">
      <div className="kpi-value" style={{ color: colorVar }}>{value}</div>
      <div className="kpi-title">{title}</div>
      {subtitle && <div className="kpi-subtitle">{subtitle}</div>}
    </div>
  </div>
);

const Dashboard = () => {
  const [history, setHistory] = useState(() => {
    try { return JSON.parse(localStorage.getItem('sc_history') || '[]'); }
    catch { return []; }
  });

  const avgProb = history.length > 0
    ? history.reduce((s, h) => s + h.probability, 0) / history.length
    : null;

  const demand = avgProb !== null ? getDemandLevel(avgProb) : null;

  const todayStr   = new Date().toISOString().split('T')[0];
  const todayCount = history.filter(h => h.date === todayStr).length;

  const clearHistory = () => {
    localStorage.removeItem('sc_history');
    setHistory([]);
  };

  return (
    <div>
      <div className="page-header">
        <div>
          <h1 className="page-title">Dashboard</h1>
          <p className="page-subtitle">Resumen de predicciones de ocupación · StayCast</p>
        </div>
        <Link to="/predictor">
          <button className="btn btn-primary">🔮 Nueva Predicción</button>
        </Link>
      </div>

      {/* KPI Grid */}
      <div className="kpi-grid">
        <KPICard
          title="Probabilidad Media"
          value={avgProb !== null ? `${Math.round(avgProb * 100)}%` : '—'}
          subtitle="De todas tus predicciones"
          colorVar="#1e3a5f"
          icon="📊"
        />
        <KPICard
          title="Nivel de Demanda"
          value={demand ? demand.label : '—'}
          subtitle={history.length > 0 ? `${history.length} predicción${history.length !== 1 ? 'es' : ''}` : 'Sin datos aún'}
          colorVar={demand ? DEMAND_COLORS[demand.color] : '#94a3b8'}
          icon="📈"
        />
        <KPICard
          title="Confianza del Modelo"
          value={avgProb !== null ? getModelConfidence(avgProb) : '—'}
          subtitle="XGBoost accuracy estimada"
          colorVar="#0891b2"
          icon="🤖"
        />
        <KPICard
          title="Predicciones Hoy"
          value={todayCount}
          subtitle="En esta sesión"
          colorVar="#16a34a"
          icon="🗓️"
        />
      </div>

      {/* Recent history */}
      <div className="section-header">
        <h2 className="section-title">Predicciones Recientes</h2>
        {history.length > 0 && (
          <button className="btn-link" onClick={clearHistory}>Limpiar historial</button>
        )}
      </div>

      {history.length === 0 ? (
        <div className="empty-state">
          <div className="empty-state-icon">🔮</div>
          <p><strong>No hay predicciones aún</strong></p>
          <p className="empty-state-sub">Usa el predictor para calcular la probabilidad de ocupación</p>
          <Link to="/predictor">
            <button className="btn btn-primary" style={{ marginTop: 16 }}>Ir al Predictor</button>
          </Link>
        </div>
      ) : (
        <div className="card">
          <div className="history-list">
            {[...history].reverse().slice(0, 10).map((item) => {
              const d   = getDemandLevel(item.probability);
              const pct = Math.round(item.probability * 100);
              return (
                <div key={item.timestamp} className="history-item">
                  <div className="history-left">
                    <span className="history-date">{item.date}</span>
                    <span className="history-meta">{item.neighbourhood} · {item.room_type}</span>
                  </div>
                  <div className="history-right">
                    <div className="mini-bar-wrap">
                      <div className="mini-bar-fill" style={{ width: `${pct}%`, backgroundColor: DEMAND_COLORS[d.color] }} />
                    </div>
                    <span className={`badge badge-${d.color}`}>{pct}%</span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
};

export default Dashboard;
