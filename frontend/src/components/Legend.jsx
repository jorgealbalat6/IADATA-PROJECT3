import React from 'react';

const ITEMS = [
  { key: 'correct',  label: 'Precio Bien Ajustado' },
  { key: 'raise',    label: 'Conviene Subir' },
  { key: 'lower',    label: 'Conviene Bajar' },
  { key: 'occupied', label: 'Ocupado' },
];

/**
 * Color legend for the calendar views.
 * Props: style (optional inline style overrides)
 */
const Legend = ({ style }) => (
  <div className="legend" style={style}>
    {ITEMS.map(({ key, label }) => (
      <div key={key} className="legend-item">
        <div className={`legend-dot ${key}`} />
        {label}
      </div>
    ))}
  </div>
);

export default Legend;
