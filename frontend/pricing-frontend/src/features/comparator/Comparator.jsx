import React, { useState } from 'react';
import usePrediction from '../../hooks/usePrediction';
import {
  DEFAULT_REQUEST, NEIGHBOURHOODS, ROOM_TYPES, getDemandLevel,
} from '../../models/prediction';

const COLOR_MAP = { success: '#16a34a', warning: '#d97706', danger: '#dc2626' };

const Comparator = () => {
  const [scenarios,     setScenarios]     = useState([]);
  const [form,          setForm]          = useState(DEFAULT_REQUEST);
  const [scenarioName,  setScenarioName]  = useState('');
  const [showForm,      setShowForm]      = useState(false);
  const { predict, loading } = usePrediction();

  const handleChange = (e) => {
    const { name, value, type } = e.target;
    const val = type === 'number' ? Number(value) : value;
    if (name === 'date') {
      const dow = new Date(value + 'T12:00:00').getDay();
      setForm(f => ({ ...f, date: value, is_weekend: [0, 6].includes(dow) ? 1 : 0 }));
    } else {
      setForm(f => ({ ...f, [name]: val }));
    }
  };

  const addScenario = async (e) => {
    e.preventDefault();
    const result = await predict(form);
    if (!result) return;
    setScenarios(s => [...s, {
      id:      Date.now().toString(),
      name:    scenarioName.trim() || `Escenario ${s.length + 1}`,
      request: { ...form },
      result,
    }]);
    setScenarioName('');
    setShowForm(false);
  };

  const remove = (id) => setScenarios(s => s.filter(sc => sc.id !== id));

  const sorted = [...scenarios].sort((a, b) => b.result.probability - a.result.probability);

  return (
    <div>
      <div className="page-header">
        <div>
          <h1 className="page-title">Comparador de Escenarios</h1>
          <p className="page-subtitle">Compara múltiples configuraciones en paralelo</p>
        </div>
        <button className="btn btn-primary" onClick={() => setShowForm(s => !s)}>
          {showForm ? '✕ Cancelar' : '+ Añadir Escenario'}
        </button>
      </div>

      {/* ── ADD FORM ── */}
      {showForm && (
        <div className="card comp-add-card">
          <h3 className="comp-form-title">Nuevo Escenario</h3>
          <form onSubmit={addScenario}>
            <div className="sc-form-group" style={{ marginBottom: 14 }}>
              <label>Nombre del escenario</label>
              <input
                type="text"
                placeholder="Ej: Fin de semana en Eixample"
                value={scenarioName}
                onChange={e => setScenarioName(e.target.value)}
              />
            </div>

            <div className="sc-form-row-2">
              <div className="sc-form-group">
                <label>Barrio</label>
                <select name="neighbourhood" value={form.neighbourhood} onChange={handleChange}>
                  {NEIGHBOURHOODS.map(n => <option key={n} value={n}>{n}</option>)}
                </select>
              </div>
              <div className="sc-form-group">
                <label>Tipo de habitación</label>
                <select name="room_type" value={form.room_type} onChange={handleChange}>
                  {ROOM_TYPES.map(r => <option key={r} value={r}>{r}</option>)}
                </select>
              </div>
            </div>

            <div className="sc-form-row-3">
              <div className="sc-form-group">
                <label>Huéspedes</label>
                <input type="number" name="accommodates" min="1" max="20" value={form.accommodates} onChange={handleChange} />
              </div>
              <div className="sc-form-group">
                <label>Fecha</label>
                <input type="date" name="date" value={form.date} onChange={handleChange} />
              </div>
              <div className="sc-form-group">
                <label>Temperatura (°C)</label>
                <input type="number" name="temp_mean" value={form.temp_mean} onChange={handleChange} />
              </div>
            </div>

            <div className="sc-toggle-row" style={{ marginBottom: 16 }}>
              <label className="sc-toggle-item">
                <span className="sc-toggle-label">Fin de semana</span>
                <div className="sc-toggle">
                  <input type="checkbox" checked={form.is_weekend === 1} onChange={e => setForm(f => ({ ...f, is_weekend: e.target.checked ? 1 : 0 }))} />
                  <span className="sc-toggle-slider" />
                </div>
              </label>
              <label className="sc-toggle-item">
                <span className="sc-toggle-label">Festivo</span>
                <div className="sc-toggle">
                  <input type="checkbox" checked={form.is_holiday === 1} onChange={e => setForm(f => ({ ...f, is_holiday: e.target.checked ? 1 : 0 }))} />
                  <span className="sc-toggle-slider" />
                </div>
              </label>
            </div>

            <button type="submit" className="btn btn-primary" disabled={loading}>
              {loading ? <><span className="sc-spinner" /> Calculando...</> : '+ Añadir al comparador'}
            </button>
          </form>
        </div>
      )}

      {/* ── TABLE ── */}
      {scenarios.length === 0 ? (
        <div className="empty-state">
          <div className="empty-state-icon">⚖️</div>
          <p><strong>Sin escenarios aún</strong></p>
          <p className="empty-state-sub">Añade al menos dos escenarios para compararlos</p>
        </div>
      ) : (
        <div className="card">
          <div className="comp-table-wrap">
            <table className="comp-table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Escenario</th>
                  <th>Barrio</th>
                  <th>Tipo</th>
                  <th>Fecha</th>
                  <th>°C</th>
                  <th>Finde</th>
                  <th>Festivo</th>
                  <th>Probabilidad</th>
                  <th>Demanda</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {sorted.map((sc, i) => {
                  const d   = getDemandLevel(sc.result.probability);
                  const pct = Math.round(sc.result.probability * 100);
                  const clr = COLOR_MAP[d.color];
                  return (
                    <tr key={sc.id} className={i === 0 ? 'comp-row-best' : ''}>
                      <td className="comp-rank">{i === 0 ? '🏆' : i + 1}</td>
                      <td><strong>{sc.name}</strong></td>
                      <td>{sc.request.neighbourhood}</td>
                      <td className="comp-cell-muted">{sc.request.room_type}</td>
                      <td className="comp-cell-muted">{sc.request.date}</td>
                      <td>{sc.request.temp_mean}°</td>
                      <td>{sc.request.is_weekend ? '✅' : '—'}</td>
                      <td>{sc.request.is_holiday ? '✅' : '—'}</td>
                      <td>
                        <div className="comp-prob-cell">
                          <div className="comp-bar-track">
                            <div className="comp-bar-fill" style={{ width: `${pct}%`, background: clr }} />
                          </div>
                          <span className="comp-pct" style={{ color: clr }}>{pct}%</span>
                        </div>
                      </td>
                      <td><span className={`badge badge-${d.color}`}>{d.label}</span></td>
                      <td>
                        <button className="btn-icon-remove" onClick={() => remove(sc.id)} title="Eliminar">✕</button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
};

export default Comparator;
