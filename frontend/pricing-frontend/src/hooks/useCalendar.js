// ============================================================
// useCalendar.js
// Encapsulates month/year navigation state and derived calendar data.
// Used by both Properties.jsx and CalendarPage.jsx.
// ============================================================

import { useState, useMemo } from 'react';
import { buildCells, buildRows } from '../utils/calendarUtils';

/**
 * @param {Date} [initialDate]  Optional starting date (defaults to today)
 * @returns {{
 *   month: number,
 *   year: number,
 *   cells: (number|null)[],
 *   rows: (number|null)[][],
 *   prevMonth: () => void,
 *   nextMonth: () => void,
 * }}
 */
const useCalendar = (initialDate = new Date()) => {
  const [month, setMonth] = useState(initialDate.getMonth());
  const [year,  setYear]  = useState(initialDate.getFullYear());

  const cells = useMemo(() => buildCells(year, month), [year, month]);
  const rows  = useMemo(() => buildRows(cells),        [cells]);

  const prevMonth = () => {
    if (month === 0) { setYear(y => y - 1); setMonth(11); }
    else setMonth(m => m - 1);
  };

  const nextMonth = () => {
    if (month === 11) { setYear(y => y + 1); setMonth(0); }
    else setMonth(m => m + 1);
  };

  return { month, year, cells, rows, prevMonth, nextMonth };
};

export default useCalendar;
