import React from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import Dashboard  from '../features/dashboard/Dashboard';
import Predictor  from '../features/predictor/Predictor';
import Insights   from '../features/insights/Insights';
import Properties from '../features/properties/Properties';

/** Page titles keyed by route path — co-located with route definitions. */
export const PAGE_TITLES = {
  '/':           'Dashboard',
  '/predictor':  'Predictor',
  '/properties': 'Inmuebles',
  '/insights':   'Insights',
};

const AppRouter = () => (
  <Routes>
    <Route path="/"           element={<Dashboard />}   />
    <Route path="/predictor"  element={<Predictor />}   />
    <Route path="/properties" element={<Properties />}  />
    <Route path="/insights"   element={<Insights />}    />
    <Route path="*"           element={<Navigate to="/" replace />} />
  </Routes>
);

export default AppRouter;

