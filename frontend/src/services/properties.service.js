// ============================================================
// properties.service.js
// GET  /properties        → load all registered properties
// POST /properties        → register a new property
// Falls back to in-memory mock when backend is unavailable.
// ============================================================

const API_BASE = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';

// ── Mock store (used when backend is unreachable) ──────────
let mockStore = [
  {
    id: 'mock-1',
    name: 'Apartamento Eixample Centro',
    neighbourhood: 'Eixample',
    room_type: 'Entire home/apt',
    accommodates: 4,
    bedrooms: 2,
    beds: 3,
    number_of_reviews: 87,
    review_scores_rating: 4.7,
    created_at: '2026-01-15T10:00:00.000Z',
  },
  {
    id: 'mock-2',
    name: 'Habitación Born',
    neighbourhood: 'Born',
    room_type: 'Private room',
    accommodates: 2,
    bedrooms: 1,
    beds: 1,
    number_of_reviews: 34,
    review_scores_rating: 4.3,
    created_at: '2026-02-20T09:30:00.000Z',
  },
];

/** Shared request helper with 5 s timeout. */
const apiFetch = async (path, options = {}) => {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 5000);
  try {
    const res = await fetch(`${API_BASE}${path}`, {
      headers: { 'Content-Type': 'application/json' },
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
 *   GET {API_BASE}/properties
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
    return await apiFetch('/properties');
  } catch {
    // TODO: remove mock fallback once backend is available
    return [...mockStore];
  }
};

/**
 * Register a new property.
 *
 * Expected API call:
 *   POST {API_BASE}/properties
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
    return await apiFetch('/properties', {
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
 *   DELETE {API_BASE}/properties/{id}
 *   Headers: { Content-Type: application/json }
 *
 * Expected response (204 No Content) — empty body.
 *
 * @param {string} id
 * @returns {Promise<void>}
 */
export const deleteProperty = async (id) => {
  try {
    await apiFetch(`/properties/${id}`, { method: 'DELETE' });
  } catch {
    // TODO: remove mock fallback once backend is available
    mockStore = mockStore.filter(p => p.id !== id);
  }
};
