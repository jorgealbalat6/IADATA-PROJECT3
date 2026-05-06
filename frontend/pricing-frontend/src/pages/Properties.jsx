import React from 'react';
import { Link } from 'react-router-dom';
import { useProperties } from '../contexts/PropertyContext';
import { getDayData, DAY_NAMES } from '../utils/calendarUtils';
import useCalendar from '../hooks/useCalendar';
import MonthNav   from '../components/MonthNav';
import Legend     from '../components/Legend';

const Properties = () => {
  const { properties } = useProperties();
  const { month, year, rows, prevMonth, nextMonth } = useCalendar();

  if (properties.length === 0) {
    return (
      <div>
        <div className="page-header">
          <h1 className="page-title">Mis Alojamientos</h1>
          <Link to="/properties/add">
            <button className="btn btn-primary">+ Añadir Alojamiento</button>
          </Link>
        </div>
        <div className="empty-state">
          <p><strong>No tienes alojamientos</strong> aún.</p>
          <Link to="/properties/add">
            <button className="btn btn-primary">Añadir Alojamiento</button>
          </Link>
        </div>
      </div>
    );
  }

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Mis Alojamientos</h1>
        <Link to="/properties/add">
          <button className="btn btn-primary">+ Añadir Alojamiento</button>
        </Link>
      </div>

      <div className="card">
        <div className="prop-selector">
          <div className="prop-selector-left">
            <div className="prop-selector-info">
              <h3>Todos los alojamientos</h3>
              <p>Málaga</p>
            </div>
          </div>
          <MonthNav month={month} year={year} onPrev={prevMonth} onNext={nextMonth} />
        </div>

        <div className="cal-wrapper">
          <table className="cal-table">
            <thead>
              <tr>
                <th className="cal-th" style={{ textAlign: 'left' }} />
                {DAY_NAMES.map(d => <th key={d} className="cal-th">{d}</th>)}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, ri) =>
                properties.map((prop) => {
                  const base = Number(prop.priceMedian) || 90;
                  return (
                    <tr key={`${prop.id}-${ri}`}>
                      {ri === 0 && (
                        <td className="cal-prop-name" rowSpan={rows.length}>
                          <Link to={`/properties/${prop.id}/calendar`} style={{ textDecoration: 'none', color: 'inherit' }}>
                            {prop.name}
                            <small>{prop.zone}</small>
                          </Link>
                        </td>
                      )}
                      {row.map((day, ci) => {
                        if (!day) return <td key={ci} className="cal-cell empty-cell" />;
                        const { state, price, rec } = getDayData(prop.id, day, base);
                        return (
                          <td key={ci} className={`cal-cell ${state}`}>
                            {price !== null && (
                              <>
                                <span className="cal-price">{price} €</span>
                                <span className="cal-rec">{rec} €</span>
                              </>
                            )}
                          </td>
                        );
                      })}
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>

        <Legend style={{ marginTop: 14 }} />
      </div>

      <div className="alerts-section">
        <p className="alerts-title">Alertas Activas</p>
        <div className="alert-item">
          <div className="alert-icon">M</div>
          <div className="alert-info">
            <div className="alert-title">Malagua Fair</div>
            <div className="alert-desc">Del 1 al 14 de agosto se celebra la Feria de Málaga.</div>
          </div>
          <button className="btn-link">Ver Afectados ›</button>
        </div>
      </div>
    </div>
  );
};

export default Properties;

