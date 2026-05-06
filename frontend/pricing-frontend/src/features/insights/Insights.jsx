import React from 'react';
import { getDemandLevel, DEMAND_COLORS } from '../../models/prediction';

const MONTHLY = [
  { label: 'Ene', v: 0.42 }, { label: 'Feb', v: 0.48 }, { label: 'Mar', v: 0.55 },
  { label: 'Abr', v: 0.63 }, { label: 'May', v: 0.71 }, { label: 'Jun', v: 0.82 },
  { label: 'Jul', v: 0.88 }, { label: 'Ago', v: 0.91 }, { label: 'Sep', v: 0.79 },
  { label: 'Oct', v: 0.65 }, { label: 'Nov', v: 0.50 }, { label: 'Dic', v: 0.58 },
];

const WEEKDAY = [
  { label: 'Lun', v: 0.44 }, { label: 'Mar', v: 0.42 }, { label: 'Mié', v: 0.46 },
  { label: 'Jue', v: 0.52 }, { label: 'Vie', v: 0.73 }, { label: 'Sáb', v: 0.86 }, { label: 'Dom', v: 0.68 },
];

const TEMPERATURE = [
  { label: '<5°',    v: 0.32 }, { label: '5-10°', v: 0.41 }, { label: '10-15°', v: 0.53 },
  { label: '15-20°', v: 0.67 }, { label: '20-25°', v: 0.79 }, { label: '25-30°', v: 0.83 }, { label: '>30°', v: 0.61 },
];

const FEATURES = [
  { label: 'is_weekend',    v: 0.22 },
  { label: 'temp_mean',     v: 0.18 },
  { label: 'is_holiday',    v: 0.15 },
  { label: 'neighbourhood', v: 0.13 },
  { label: 'review_score',  v: 0.11 },
  { label: 'accommodates',  v: 0.09 },
  { label: 'num_reviews',   v: 0.07 },
  { label: 'room_type',     v: 0.05 },
];

/** Top N features highlighted with primary color, rest with light blue. */
const TOP_FEATURES_COUNT = 3;

const BarChart = ({ data, title, subtitle }) => {
  const max = Math.max(...data.map(d => d.v));
  return (
    <div className="chart-card card">
      <div className="chart-header">
        <h3 className="chart-title">{title}</h3>
        {subtitle && <p className="chart-subtitle">{subtitle}</p>}
      </div>
      <div className="v-bar-chart">
        {data.map((d, i) => (
          <div key={i} className="v-bar-item">
            <span className="v-bar-top-label">{Math.round(d.v * 100)}%</span>
            <div className="v-bar-track">
              <div
                className="v-bar-fill"
                style={{ height: `${(d.v / max) * 100}%`, background: DEMAND_COLORS[getDemandLevel(d.v).color] }}
              />
            </div>
            <span className="v-bar-bottom-label">{d.label}</span>
          </div>
        ))}
      </div>
    </div>
  );
};

const HBarChart = ({ data, title, subtitle }) => {
  const max = Math.max(...data.map(d => d.v));
  return (
    <div className="chart-card card">
      <div className="chart-header">
        <h3 className="chart-title">{title}</h3>
        {subtitle && <p className="chart-subtitle">{subtitle}</p>}
      </div>
      <div className="h-bar-chart">
        {data.map((d, i) => (
          <div key={i} className="h-bar-item">
            <span className="h-bar-label">{d.label}</span>
            <div className="h-bar-track">
              <div
                className="h-bar-fill"
                style={{
                  width: `${(d.v / max) * 100}%`,
                  background: i < TOP_FEATURES_COUNT ? DEMAND_COLORS.primary : '#93c5fd',
                }}
              />
            </div>
            <span className="h-bar-value">{Math.round(d.v * 100)}%</span>
          </div>
        ))}
      </div>
    </div>
  );
};

const Insights = () => (
  <div>
    <div className="page-header">
      <div>
        <h1 className="page-title">Insights del Modelo</h1>
        <p className="page-subtitle">Patrones de ocupación y variables más influyentes · XGBoost</p>
      </div>
    </div>

    <div className="insights-grid">
      <BarChart
        data={MONTHLY}
        title="Ocupación por Mes"
        subtitle="Probabilidad media mensual a lo largo del año"
      />
      <BarChart
        data={WEEKDAY}
        title="Ocupación por Día de Semana"
        subtitle="El fin de semana concentra la mayor demanda"
      />
      <BarChart
        data={TEMPERATURE}
        title="Ocupación por Temperatura"
        subtitle="Temperatura óptima entre 20–30 °C"
      />
      <HBarChart
        data={FEATURES}
        title="Importancia de Variables"
        subtitle="Contribución relativa de cada variable al modelo"
      />
    </div>
  </div>
);

export default Insights;
