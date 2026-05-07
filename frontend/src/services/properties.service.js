// ============================================================
// properties.service.js
// GET  /apartments        → load all registered properties
// POST /apartments        → register a new property
// Falls back to in-memory mock when backend is unavailable.
// ============================================================

const API_BASE = import.meta.env.VITE_API_URL ?? '';

// ── Mock store (usado solo si no hay backend disponible) ───
let mockStore = [];

/** Shared request helper with 5 s timeout + JWT auth. */
const apiFetch = async (path, options = {}) => {
  const token = localStorage.getItem('auth_token');
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 5000);
  try {
    const res = await fetch(`${API_BASE}${path}`, {
      headers: {
        'Content-Type': 'application/json',
        ...(token ? { 'Authorization': `Bearer ${token}` } : {}),
      },
      signal: controller.signal,
      ...options,
    });
    clearTimeout(timer);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } finally {
    clearTimeout(timer);
  }
};

// ── Public API ─────────────────────────────────────────────

/**
 * Load all registered properties.
 *
 * Expected API call:
 *   GET {API_BASE}/apartments
 *   Headers: { Content-Type: application/json }
 *
 * Expected response (200 OK):
 *   [
 *     {
 *       id: "abc123",
 *       name: "Apartamento Eixample",
 *       neighbourhood: "Eixample",
 *       room_type: "Entire home/apt",
 *       accommodates: 4,
 *       bedrooms: 2,
 *       beds: 3,
 *       number_of_reviews: 87,
 *       review_scores_rating: 4.7,
 *       created_at: "2026-01-15T10:00:00.000Z"
 *     },
 *     ...
 *   ]
 *
 * @returns {Promise<Property[]>}
 */
export const fetchProperties = async () => {
  try {
    return await apiFetch('/apartments');
  } catch {
    // TODO: remove mock fallback once backend is available
    return [...mockStore];
  }
};

/**
 * Register a new property.
 *
 * Expected API call:
 *   POST {API_BASE}/apartments
 *   Headers: { Content-Type: application/json }
 *   Body (JSON):
 *   {
 *     name: "Apartamento Eixample",
 *     neighbourhood: "Eixample",
 *     room_type: "Entire home/apt",
 *     accommodates: 4,
 *     bedrooms: 2,
 *     beds: 3,
 *     number_of_reviews: 87,
 *     review_scores_rating: 4.7
 *   }
 *
 * Expected response (201 Created):
 *   {
 *     id: "abc123",           ← assigned by the backend
 *     created_at: "2026-...", ← assigned by the backend
 *     ...same fields as body
 *   }
 *
 * @param {Omit<Property, 'id' | 'created_at'>} data
 * @returns {Promise<Property>}
 */
export const createProperty = async (data) => {
  try {
    return await apiFetch('/apartments', {
      method: 'POST',
      body: JSON.stringify(data),
    });
  } catch {
    // TODO: remove mock fallback once backend is available
    const newProp = {
      ...data,
      id: `mock-${Date.now()}`,
      created_at: new Date().toISOString(),
    };
    mockStore = [...mockStore, newProp];
    return newProp;
  }
};

/**
 * Delete a property by id.
 *
 * Expected API call:
 *   DELETE {API_BASE}/apartments/{id}
 *   Headers: { Content-Type: application/json }
 *
 * Expected response (204 No Content) — empty body.
 *
 * @param {string} id
 * @returns {Promise<void>}
 */
export const deleteProperty = async (id) => {
  try {
    await apiFetch(`/apartments/${id}`, { method: 'DELETE' });
  } catch {
    // TODO: remove mock fallback once backend is available
    mockStore = mockStore.filter(p => p.id !== id);
  }
};
