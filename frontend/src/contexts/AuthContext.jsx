import React, { createContext, useContext, useState } from 'react';

const AuthContext = createContext();
export const useAuth = () => useContext(AuthContext);

const API_BASE = import.meta.env.VITE_API_URL ?? '';

export const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(() => {
    const stored = localStorage.getItem('auth_user');
    return stored ? JSON.parse(stored) : null;
  });

  const login = async (email, password) => {
    try {
      const res = await fetch(`${API_BASE}/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password }),
      });
      if (!res.ok) return false;
      const { token, uid } = await res.json();
      const u = { name: email.split('@')[0], email, uid };
      setUser(u);
      localStorage.setItem('auth_user', JSON.stringify(u));
      localStorage.setItem('auth_token', token);
      return true;
    } catch {
      // Fallback para demo sin backend
      const u = { name: email.split('@')[0], email };
      setUser(u);
      localStorage.setItem('auth_user', JSON.stringify(u));
      return true;
    }
  };

  const register = async (name, email, password) => {
    try {
      const res = await fetch(`${API_BASE}/auth/register`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, email, password }),
      });
      if (!res.ok) return false;
      return await login(email, password);
    } catch {
      // Fallback para demo sin backend
      const u = { name, email };
      setUser(u);
      localStorage.setItem('auth_user', JSON.stringify(u));
      return true;
    }
  };

  const logout = () => {
    setUser(null);
    localStorage.removeItem('auth_user');
    localStorage.removeItem('auth_token');
  };

  return (
    <AuthContext.Provider value={{ user, login, register, logout }}>
      {children}
    </AuthContext.Provider>
  );
};
