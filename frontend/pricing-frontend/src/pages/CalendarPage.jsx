import React, { useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { useProperties } from '../contexts/PropertyContext';
import { getDayData, DAY_NAMES, MONTH_NAMES } from '../utils/calendarUtils';
import useCalendar from '../hooks/useCalendar';
import MonthNav   from '../components/MonthNav';
import Legend     from '../components/Legend';
import DayModal   from '../components/DayModal';

const CalendarPage = () => {
  const { id } = useParams();
  const { getProperty } = useProperties();
  const property = getProperty(Number(id));

  const { month, year, rows, prevMonth, nextMonth } = useCalendar();
  const [selected, setSelected] = useState(null);

  if (!property) return <p>Propiedad no encontrada. <Link to="/properties">Volver</Link></p>;

  const base = Number(property.priceMedian) || 90;

  const handleCellClick = (day) => {
    const data = getDayData(Number(id), day, base);
    if (data.state === 'occupied') return;
    setSelected({ day, date: new Date(year, month, day), ...data });
  };

  return (
    <div>
      <div className="page-header">
        <div>
          <span style={{ fontWeight: 700, fontSize: 18, color: '#1a1a2e' }}>
            Calendario de {MONTH_NAMES[month]} {year}
          </span>
          <div style={{ fontSize: 12, color: '#aaa', marginTop: 4 }}>
            {property.name} · {property.zone}
          </div>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <MonthNav month={month} year={year} onPrev={prevMonth} onNext={nextMonth} />
          <Link to="/properties/add">
            <button className="btn btn-primary" style={{ marginLeft: 10 }}>+ Añadir Alojamiento</button>
          </Link>
        </div>
      </div>

      <div className="card">
        <table className="single-cal-table">
          <thead>
            <tr>{DAY_NAMES.map(d => <th key={d} className="single-cal-th">{d}</th>)}</tr>
          </thead>
          <tbody>
            {rows.map((row, ri) => (
              <tr key={ri}>
                {row.map((day, ci) => {
                  if (!day) return <td key={ci} className="single-cal-cell empty-cell" />;
                  const { state, price, rec } = getDayData(Number(id), day, base);
                  return (
                    <td key={ci} className={`single-cal-cell ${state}`} onClick={() => handleCellClick(day)}>
                      <span className="single-day-num">{day}</span>
                      {price !== null && <span className="single-price">{price} €</span>}
                      {price !== null && <span className="single-rec-price">{rec} €</span>}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>

        <Legend style={{ marginTop: 16 }} />
      </div>

      <DayModal
        selected={selected}
        month={month}
        onClose={() => setSelected(null)}
      />
    </div>
  );
};

export default CalendarPage;

