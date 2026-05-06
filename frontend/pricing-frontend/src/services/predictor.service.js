// ============================================================
// predictor.service.js
// Calls POST /predict. Falls back to a deterministic mock
// when the backend is unavailable (demo / offline mode).
// ============================================================

const API_BASE = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';

/** Deterministic mock calculation based on input features. */
const mockPredict = (req) => {
  let p = 0.38;

  if (req.is_weekend) p += 0.13;
  if (req.is_holiday) p += 0.16;

  if (req.temp_mean >= 18 && req.temp_mean <= 28) p += 0.07;
  else if (req.temp_mean >= 10)                   p += 0.03;
  else if (req.temp_mean < 5 || req.temp_mean > 35) p -= 0.06;

  if (req.review_scores_rating >= 4.7)      p += 0.10;
  else if (req.review_scores_rating >= 4.0) p += 0.05;

  if (req.accommodates >= 6)      p += 0.06;
  else if (req.accommodates >= 4) p += 0.03;

  if (req.number_of_reviews > 100) p += 0.07;
  else if (req.number_of_reviews > 50) p += 0.04;

  const nbBonus = {
    Eixample: 0.08, Born: 0.10, Barceloneta: 0.12,
    Gràcia: 0.07, Centro: 0.07, Malasaña: 0.06,
  };
  p += nbBonus[req.neighbourhood] ?? 0.04;

  if (req.room_type === 'Entire home/apt') p += 0.05;

  return Math.min(Math.max(p, 0.04), 0.97);
};

/**
 * Calls the real API to get an occupancy prediction.
 *
 * Expected API call:
 *   POST {API_BASE}/predict
 *   Headers: { Content-Type: application/json }
 *   Body (JSON):
 *   {
 *     neighbourhood:        "Eixample",
 *     room_type:            "Entire home/apt",
 *     accommodates:         2,
 *     bedrooms:             1,
 *     beds:                 1,
 *     number_of_reviews:    20,
 *     review_scores_rating: 4.5,
 *     date:                 "2026-05-06",
 *     is_weekend:           0,          ← 0 or 1
 *     is_holiday:           0,          ← 0 or 1
 *     temp_mean:            20          ← degrees Celsius
 *   }
 *
 * Expected response (200 OK):
 *   {
 *     probability: 0.73   ← float between 0 and 1
 *   }
 *
 * On any failure (timeout, network error, non-2xx status),
 * falls back automatically to the deterministic mock.
 *
 * @param {import('../models/prediction').PredictionRequest} request
 * @returns {Promise<{ probability: number }>}
 */
export const predictOccupancy = async (request) => {
  try {
    const controller = new AbortController();
    const timer      = setTimeout(() => controller.abort(), 5000);

    const res = await fetch(`${API_BASE}/predict`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(request),
      signal:  controller.signal,
    });
    clearTimeout(timer);

    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch {
    // Simulate network latency for demo purposes
    await new Promise(r => setTimeout(r, 950));
    return { probability: mockPredict(request) };
  }
};
