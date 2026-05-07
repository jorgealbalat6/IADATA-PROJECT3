import React, { createContext, useContext, useState } from 'react';

const PropertyContext = createContext();
export const useProperties = () => useContext(PropertyContext);

const SEED = [
  { id: 1, name: 'Ródaga Bolérico', address: 'Calle Mayor 1', city: 'Málaga', zone: 'Centro', type: 'Apartamento', rooms: 2, capacity: 4, priceMedian: 90 },
  { id: 2, name: 'Ático Centro Histórico', address: 'Plaza España 5', city: 'Málaga', zone: 'Histórico', type: 'Ático', rooms: 3, capacity: 6, priceMedian: 100 },
  { id: 3, name: 'Casa Puerta Caleana', address: 'Av. del Mar 12', city: 'Málaga', zone: 'Playa', type: 'Casa', rooms: 4, capacity: 8, priceMedian: 85 },
];

export const PropertyProvider = ({ children }) => {
  const [properties, setProperties] = useState(() => {
    const stored = localStorage.getItem('properties');
    const parsed = stored ? JSON.parse(stored) : null;
    return parsed && parsed.length > 0 ? parsed : SEED;
  });

  const persist = (list) => {
    setProperties(list);
    localStorage.setItem('properties', JSON.stringify(list));
  };

  const addProperty = (data) => {
    const newProp = { id: properties.length ? properties[properties.length - 1].id + 1 : 1, ...data };
    persist([...properties, newProp]);
  };

  const updateProperty = (id, data) => {
    persist(properties.map((p) => (p.id === id ? { ...p, ...data } : p)));
  };

  const getProperty = (id) => properties.find((p) => p.id === id);

  return (
    <PropertyContext.Provider value={{ properties, addProperty, updateProperty, getProperty }}>
      {children}
    </PropertyContext.Provider>
  );
};
