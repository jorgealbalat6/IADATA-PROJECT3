import React from 'react';

// Names of the days of the week in Spanish, starting with Monday.
const dayNames = ['Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb', 'Dom'];

/**
 * Calendar component renders a month view with seven columns starting
 * on Monday.  It accepts a `month` (0-11), `year`, an array of
 * `days` objects and an optional click handler.  Each entry in
 * `days` should have a `date` string and a `state`: one of
 * 'correct', 'raise', 'lower' or 'occupied'.
 */
const Calendar = ({ month, year, days = [], onCellClick }) => {
  // Construct a Date for the first day of the month
  const firstDay = new Date(year, month, 1);
  // Compute Monday-based index (0 = Monday, 6 = Sunday)
  const firstDayIndex = (firstDay.getDay() + 6) % 7;
  const totalDays = new Date(year, month + 1, 0).getDate();

  // Create an array of cells containing either null or an object with
  // the day number and any associated data.  Leading nulls are used
  // to align the first day on the correct weekday column.
  const cells = [];
  for (let i = 0; i < firstDayIndex; i++) {
    cells.push(null);
  }
  for (let d = 1; d <= totalDays; d++) {
    const cellDate = new Date(year, month, d);
    const dayData = days.find((item) => {
      const d1 = new Date(item.date);
      return d1.toDateString() === cellDate.toDateString();
    });
    cells.push({ number: d, data: dayData });
  }

  // Split the cells into rows of 7
  const rows = [];
  for (let i = 0; i < cells.length; i += 7) {
    rows.push(cells.slice(i, i + 7));
  }

  // Determine CSS class based on state
  const getClass = (cell) => {
    if (!cell || !cell.data) return 'calendar-cell';
    switch (cell.data.state) {
      case 'raise':
        return 'calendar-cell raise';
      case 'lower':
        return 'calendar-cell lower';
      case 'occupied':
        return 'calendar-cell occupied';
      default:
        return 'calendar-cell correct';
    }
  };

  return (
    <div className="calendar">
      <div className="calendar-grid">
        {/* Header row with day names */}
        <div className="calendar-row header">
          {dayNames.map((d) => (
            <div key={d} className="calendar-header-cell">
              {d}
            </div>
          ))}
        </div>
        {/* Render each week */}
        {rows.map((row, idx) => (
          <div key={idx} className="calendar-row">
            {row.map((cell, cid) =>
              cell ? (
                <div
                  key={cid}
                  className={getClass(cell)}
                  onClick={() => onCellClick && onCellClick(cell)}
                >
                  <span className="day-number">{cell.number}</span>
                </div>
              ) : (
                <div key={cid} className="calendar-cell empty"></div>
              )
            )}
          </div>
        ))}
      </div>
    </div>
  );
};

export default Calendar;
