// ============================================================
// useProperties.js  — Hook for property list state management
// ============================================================

import { useState, useEffect, useCallback } from 'react';
import {
  fetchProperties,
  createProperty,
  deleteProperty,
  updateProperty,
} from '../services/properties.service';
const useProperties = () => {
  const [properties, setProperties] = useState([]);
  const [loading,    setLoading]    = useState(false);
  const [error,      setError]      = useState(null);

  // Load on mount
  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await fetchProperties();
        if (!cancelled) setProperties(data);
      } catch (e) {
        if (!cancelled) setError(e.message || 'Error al cargar inmuebles.');
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    load();
    return () => { cancelled = true; };
  }, []);

  /** Register a new property and append it to the list. */
  const addProperty = useCallback(async (formData) => {
    setLoading(true);
    setError(null);
    try {
      const created = await createProperty(formData);
      setProperties(prev => [...prev, created]);
      return created;
    } catch (e) {
      setError(e.message || 'Error al registrar el inmueble.');
      return null;
    } finally {
      setLoading(false);
    }
  }, []);

  /** Remove a property by id. */
  const removeProperty = useCallback(async (id) => {
    try {
      await deleteProperty(id);
      setProperties(prev => prev.filter(p => p.id !== id));
    } catch (e) {
      setError(e.message || 'Error al eliminar el inmueble.');
    }
  }, []);
  /** Update a property by id. */
  const editProperty = useCallback(async (id, data) => {
    setLoading(true);
    setError(null);
    try {
      await updateProperty(id, data);
      setProperties(prev => prev.map(p => p.id === id ? { ...p, ...data } : p));
      return true;
    } catch (e) {
      setError(e.message || 'Error al actualizar el inmueble.');
      return false;
    } finally {
      setLoading(false);
    }
  }, []);

  return { properties, loading, error, addProperty, removeProperty, editProperty };
  
};

export default useProperties;
