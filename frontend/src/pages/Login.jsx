import React, { useState } from 'react';
import { useAuth } from '../contexts/AuthContext';

const Login = ({ onGoRegister }) => {
  const { login } = useAuth();
  const [email, setEmail]       = useState('');
  const [password, setPassword] = useState('');
  const [error, setError]       = useState('');
  const [loading, setLoading]   = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!email || !password) { setError('Completa todos los campos.'); return; }
    setLoading(true);
    setError('');
    const result = await login(email, password);
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
        <h2 className="auth-title">Iniciar Sesión</h2>

        <form onSubmit={handleSubmit} className="auth-form">
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
              placeholder="Contraseña"
              value={password}
              onChange={e => setPassword(e.target.value)}
              required
            />
          </div>

          {error && <p className="auth-error">{error}</p>}

          <button type="submit" className="auth-btn" disabled={loading}>
            {loading ? 'Entrando...' : 'Entrar'}
          </button>
        </form>

        <p className="auth-link-text">
          ¿No tienes cuenta?{' '}
          <button className="auth-link-btn" onClick={onGoRegister}>Regístrate</button>
        </p>
      </div>
    </div>
  );
};

export default Login;
