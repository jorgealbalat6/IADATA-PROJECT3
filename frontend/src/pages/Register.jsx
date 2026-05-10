import React, { useState } from 'react';
import { useAuth } from '../contexts/AuthContext';

const Register = ({ onGoLogin }) => {
  const { register } = useAuth();
  const [name, setName]             = useState('');
  const [email, setEmail]           = useState('');
  const [password, setPassword]     = useState('');
  const [confirm, setConfirm]       = useState('');
  const [error, setError]           = useState('');
  const [loading, setLoading]       = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!name || !email || !password || !confirm) { setError('Completa todos los campos.'); return; }
    if (password !== confirm) { setError('Las contraseñas no coinciden.'); return; }
    if (password.length < 6) { setError('La contraseña debe tener al menos 6 caracteres.'); return; }
    setLoading(true);
    setError('');
    const result = await register(name, email, password);
    if (!result.success) {
      setError(result.error);
    }
    setLoading(false);
  };

  return (
    <div className="auth-page">
      <div className="auth-card">
        <div className="auth-logo">🏠</div>
        <p className="auth-brand">StayCast</p>
        <p className="auth-brand-sub">Occupancy Prediction App</p>
        <h2 className="auth-title">Regístrate</h2>

        <form onSubmit={handleSubmit} className="auth-form">
          <div className="auth-input-wrapper">
            <span className="auth-input-icon">👤</span>
            <input
              className="auth-input"
              type="text"
              placeholder="Nombre"
              value={name}
              onChange={e => setName(e.target.value)}
              required
            />
          </div>
          <div className="auth-input-wrapper">
            <span className="auth-input-icon">✉</span>
            <input
              className="auth-input"
              type="email"
              placeholder="Correo Electrónico"
              value={email}
              onChange={e => setEmail(e.target.value)}
              required
            />
          </div>
          <div className="auth-input-wrapper">
            <span className="auth-input-icon">🔒</span>
            <input
              className="auth-input"
              type="password"
              placeholder="Contraseña (mín. 6 caracteres)"
              value={password}
              onChange={e => setPassword(e.target.value)}
              required
            />
          </div>
          <div className="auth-input-wrapper">
            <span className="auth-input-icon">🔒</span>
            <input
              className="auth-input"
              type="password"
              placeholder="Confirmar Contraseña"
              value={confirm}
              onChange={e => setConfirm(e.target.value)}
              required
            />
          </div>

          {error && <p className="auth-error">{error}</p>}

          <button type="submit" className="auth-btn" disabled={loading}>
            {loading ? 'Creando cuenta...' : 'Crear Cuenta'}
          </button>
        </form>

        <p className="auth-link-text" style={{ marginTop: 14 }}>
          ¿Ya tienes cuenta?{' '}
          <button className="auth-link-btn" onClick={onGoLogin}>Inicia sesión</button>
        </p>
      </div>
    </div>
  );
};

export default Register;
