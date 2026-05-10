// ============================================================
// AuthContext.jsx — Firebase Authentication context
// ============================================================

import React, { createContext, useContext, useState, useEffect } from 'react';
import { auth } from '../firebase';
import {
  signInWithEmailAndPassword,
  createUserWithEmailAndPassword,
  updateProfile,
  signOut,
  onAuthStateChanged,
} from 'firebase/auth';

const AuthContext = createContext();
export const useAuth = () => useContext(AuthContext);

export const AuthProvider = ({ children }) => {
  const [user, setUser]       = useState(null);
  const [loading, setLoading] = useState(true);

  // Escuchar cambios de auth de Firebase
  useEffect(() => {
    const unsubscribe = onAuthStateChanged(auth, async (firebaseUser) => {
      if (firebaseUser) {
        // Guardar token para que los services lo puedan usar
        const token = await firebaseUser.getIdToken();
        localStorage.setItem('auth_token', token);
        setUser({
          uid: firebaseUser.uid,
          name: firebaseUser.displayName || firebaseUser.email.split('@')[0],
          email: firebaseUser.email,
        });
      } else {
        localStorage.removeItem('auth_token');
        setUser(null);
      }
      setLoading(false);
    });
    return unsubscribe;
  }, []);

  // Refrescar token cada 50 minutos (Firebase tokens expiran a los 60 min)
  useEffect(() => {
    const interval = setInterval(async () => {
      const currentUser = auth.currentUser;
      if (currentUser) {
        const token = await currentUser.getIdToken(true);
        localStorage.setItem('auth_token', token);
      }
    }, 50 * 60 * 1000);
    return () => clearInterval(interval);
  }, []);

  const login = async (email, password) => {
    try {
      await signInWithEmailAndPassword(auth, email, password);
      return { success: true };
    } catch (e) {
      const msg = firebaseErrorMsg(e.code);
      return { success: false, error: msg };
    }
  };

  const register = async (name, email, password) => {
    try {
      const cred = await createUserWithEmailAndPassword(auth, email, password);
      await updateProfile(cred.user, { displayName: name });
      // Forzar refresh para que onAuthStateChanged pille el displayName
      const token = await cred.user.getIdToken(true);
      localStorage.setItem('auth_token', token);
      setUser({
        uid: cred.user.uid,
        name: name,
        email: cred.user.email,
      });
      return { success: true };
    } catch (e) {
      const msg = firebaseErrorMsg(e.code);
      return { success: false, error: msg };
    }
  };

  const logout = async () => {
    await signOut(auth);
    localStorage.removeItem('auth_token');
    setUser(null);
  };

  return (
    <AuthContext.Provider value={{ user, loading, login, register, logout }}>
      {children}
    </AuthContext.Provider>
  );
};

/** Traduce errores de Firebase a mensajes en español. */
function firebaseErrorMsg(code) {
  switch (code) {
    case 'auth/email-already-in-use':       return 'Este email ya está registrado.';
    case 'auth/invalid-email':              return 'Email no válido.';
    case 'auth/weak-password':              return 'La contraseña debe tener al menos 6 caracteres.';
    case 'auth/user-not-found':             return 'No existe una cuenta con este email.';
    case 'auth/wrong-password':             return 'Contraseña incorrecta.';
    case 'auth/invalid-credential':         return 'Credenciales incorrectas.';
    case 'auth/too-many-requests':          return 'Demasiados intentos. Espera un momento.';
    default:                                return 'Error de autenticación. Inténtalo de nuevo.';
  }
}
