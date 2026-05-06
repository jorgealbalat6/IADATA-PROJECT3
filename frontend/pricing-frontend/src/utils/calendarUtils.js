// ============================================================
// calendarUtils.js
// Shared constants and pure functions for calendar/pricing logic.
// Single source of truth – imported by Properties, CalendarPage, etc.
// ============================================================

export const DAY_NAMES  = ['LUN', 'MAR', 'MIÉ', 'JUE', 'VIE', 'SÁB', 'DOM'];
export const MONTH_NAMES = [
  'Enero','Febrero','Marzo','Abril','Mayo','Junio',
  'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre',
];
export const WEEK_DAY_NAMES = [
  'domingo','lunes','martes','miércoles','jueves','viernes','sábado',
];

export const DEMAND_LABELS = {
  correct:  'Demanda Ajustada',
  raise:    'Demanda Alta',
  lower:    'Demanda Baja',
  occupied: 'Ocupado',
};

export const DEMAND_LEVEL = {
  correct:  'medium',
  raise:    'high',
  lower:    'low',
  occupied: 'medium',
};

export const DEMAND_DESC = {
  correct:  'El precio está bien ajustado según la demanda de la zona y propiedades similares.',
  raise:    'Hay alta demanda para esta fecha. Te recomendamos subir el precio para maximizar tus ingresos.',
  lower:    'En base a propiedades similares, día de semana y ocupación en la zona te recomendamos bajar el precio para atraer más reservas.',
  occupied: 'Esta fecha ya está ocupada.',
};

/**
 * Deterministic state from a numeric seed.
 * Returns one of: 'correct' | 'raise' | 'lower' | 'occupied'
 */
export const getState = (seed) => {
  const states = ['correct', 'raise', 'lower', 'occupied'];
  return states[seed % 4];
};

/**
 * Current price for a day. Returns null when occupied.
 */
export const getPrice = (base, day, state) => {
  if (state === 'occupied') return null;
  return base + (((day * 7 + base) % 20) - 10);
};

/**
 * Recommended price for a day.
 */
export const getRecPrice = (base, day) =>
  base + (((day * 3 + base) % 15) - 7);

/**
 * Returns all data for a single calendar day.
 * @param {number} propertyId
 * @param {number} day        Day of the month (1-31)
 * @param {number} base       Property median price
 */
export const getDayData = (propertyId, day, base) => {
  const seed  = propertyId * 100 + day;
  const state = getState(seed);
  const price = getPrice(base, day, state);
  const rec   = getRecPrice(base, day);
  return { state, price, rec };
};

/**
 * Builds the flat array of cells for a month.
 * Leading nulls align the first day to the correct weekday column (Mon = 0).
 */
export const buildCells = (year, month) => {
  const firstDayIndex = (new Date(year, month, 1).getDay() + 6) % 7;
  const totalDays     = new Date(year, month + 1, 0).getDate();
  const cells         = Array(firstDayIndex).fill(null);
  for (let d = 1; d <= totalDays; d++) cells.push(d);
  return cells;
};

/**
 * Splits a flat cells array into rows of 7.
 */
export const buildRows = (cells) => {
  const rows = [];
  for (let i = 0; i < cells.length; i += 7) rows.push(cells.slice(i, i + 7));
  return rows;
};
