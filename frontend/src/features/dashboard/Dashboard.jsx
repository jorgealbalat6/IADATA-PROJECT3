import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { getDemandLevel } from '../../models/prediction';

const KPICard = ({ title, value, subtitle }) => (
  <div className="kpi-card">
    <div className="kpi-title">{title}</div>
    <div className="kpi-value">{value}</div>
    {subtitle && <div className="kpi-subtitle">{subtitle}</div>}
  </div>
);

const Dashboard = () => {
  const navigate = useNavigate();
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
          <p className="page-subtitle">Resumen de predicciones de ocupación</p>
        </div>
        <button className="btn btn-primary" onClick={() => navigate('/predictor')}>Nueva predicción</button>
      </div>

      {/* KPI Grid */}
      <div className="kpi-grid">
        <KPICard
          title="Probabilidad media"
          value={avgProb !== null ? `${Math.round(avgProb * 100)}%` : '—'}
          subtitle="De todas tus predicciones"
        />
        <KPICard
          title="Nivel de demanda"
          value={demand ? demand.label : '—'}
          subtitle={history.length > 0 ? `${history.length} predicción${history.length !== 1 ? 'es' : ''}` : 'Sin datos aún'}
        />
        <KPICard
          title="Predicciones hoy"
          value={todayCount}
          subtitle="En esta sesión"
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
          <p><strong>No hay predicciones aún</strong></p>
          <p className="empty-state-sub">Usa el predictor para calcular la probabilidad de ocupación</p>
          <button className="btn btn-primary" style={{ marginTop: 16 }} onClick={() => navigate('/predictor')}>Ir al predictor</button>
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
                      <div className="mini-bar-fill" style={{ width: `${pct}%` }} />
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
