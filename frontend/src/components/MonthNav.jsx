import React from 'react';
import { MONTH_NAMES } from '../utils/calendarUtils';

/**
 * Reusable month navigation bar.
 * Props: month (0-11), year, onPrev, onNext, className
 */
const MonthNav = ({ month, year, onPrev, onNext, className = '' }) => (
  <div className={`prop-selector-nav ${className}`}>
    <button className="month-nav-btn" onClick={onPrev}>‹</button>
    <span className="month-label">{MONTH_NAMES[month]} {year}</span>
    <button className="month-nav-btn" onClick={onNext}>›</button>
  </div>
);

export default MonthNav;
