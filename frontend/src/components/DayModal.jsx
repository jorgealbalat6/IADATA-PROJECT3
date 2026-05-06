import React from 'react';
import {
  MONTH_NAMES,
  WEEK_DAY_NAMES,
  DEMAND_LABELS,
  DEMAND_LEVEL,
  DEMAND_DESC,
} from '../utils/calendarUtils';

/**
 * Modal shown when clicking a calendar day.
 *
 * Props:
 *  - selected: { day, date, price, rec, state } | null
 *  - month: number (0-11)
 *  - onClose: () => void
 *  - onApply: () => void
 */
const DayModal = ({ selected, month, onClose, onApply }) => {
  if (!selected) return null;

  const weekDay = WEEK_DAY_NAMES[selected.date.getDay()];
  const weekDayCapitalized = weekDay.charAt(0).toUpperCase() + weekDay.slice(1);

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={e => e.stopPropagation()}>

        <div className="modal-header">
          <span className="modal-date">
            {weekDayCapitalized} {selected.day} de {MONTH_NAMES[month]}
          </span>
          <button className="modal-close" onClick={onClose}>×</button>
        </div>

        <div className="modal-body">
          <div className="price-comparison">
            <span className="price-current">{selected.price} €</span>
            <span className="price-arrow">›</span>
            <span className="price-rec">{selected.rec} €</span>
          </div>
          <div className="price-range">
            Entre {selected.rec - 5} € – {selected.rec + 5} €
          </div>

          <div className="modal-stat">
            <span>●</span>
            <span>
              <strong>{Math.floor(30 + (selected.day * 3) % 50)}%</strong>{' '}
              Ocupación de la Zona
            </span>
          </div>

          <div>
            <span className={`demand-badge ${DEMAND_LEVEL[selected.state]}`}>
              {DEMAND_LABELS[selected.state]}
            </span>
          </div>

          <div className="demand-desc">{DEMAND_DESC[selected.state]}</div>

          <button className="modal-apply-btn" onClick={onApply ?? onClose}>
            Aplicar Recomendaciones
          </button>
        </div>

      </div>
    </div>
  );
};

export default DayModal;
