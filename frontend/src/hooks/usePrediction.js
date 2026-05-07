// ============================================================
// usePrediction.js  — Hook for prediction state management
// ============================================================

import { useState, useCallback } from 'react';
import { predictOccupancy } from '../services/predictor.service';

const usePrediction = () => {
  const [result,  setResult]  = useState(null);
  const [loading, setLoading] = useState(false);
  const [error,   setError]   = useState(null);

  const predict = useCallback(async (request) => {
    setLoading(true);
    setError(null);
    try {
      const data = await predictOccupancy(request);
      setResult(data);
      return data;
    } catch (e) {
      setError(e.message || 'Error al conectar con el modelo.');
      return null;
    } finally {
      setLoading(false);
    }
  }, []);

  return { result, loading, error, predict };
};

export default usePrediction;
