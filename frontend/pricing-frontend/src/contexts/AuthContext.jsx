import React, { createContext, useContext, useState } from 'react';

const AuthContext = createContext();
export const useAuth = () => useContext(AuthContext);

export const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(() => {
    const stored = localStorage.getItem('auth_user');
    return stored ? JSON.parse(stored) : null;
  });

  // TODO: validate credentials against real API when backend is available
  const login = (email, _password) => {
    // Simulación: cualquier credencial es válida
    const u = { name: email.split('@')[0], email };
    setUser(u);
    localStorage.setItem('auth_user', JSON.stringify(u));
    return true;
  };

  // TODO: validate credentials against real API when backend is available
  const register = (name, email, _password) => {
    const u = { name, email };
    setUser(u);
    localStorage.setItem('auth_user', JSON.stringify(u));
    return true;
  };

  const logout = () => {
    setUser(null);
    localStorage.removeItem('auth_user');
  };

  return (
    <AuthContext.Provider value={{ user, login, register, logout }}>
      {children}
    </AuthContext.Provider>
  );
};
