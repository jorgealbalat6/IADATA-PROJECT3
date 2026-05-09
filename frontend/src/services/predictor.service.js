// ============================================================
// predictor.service.js
// Calls POST /predict. Falls back to a deterministic mock
// when the backend is unavailable (demo / offline mode).
// ============================================================

// Con el proxy de Vite configurado, la base queda vacía en dev
const API_BASE = import.meta.env.VITE_API_URL ?? '';

/** Deterministic mock calculation based on input features. */
const mockPredict = (req) => {
  let p = 0.38;

  if (req.instant_bookable) p += 0.10;

  if (req.listing_price < 50)       p += 0.08;
  else if (req.listing_price < 100) p += 0.04;
  else if (req.listing_price > 200) p -= 0.06;

  if (req.minimum_nights <= 1) p += 0.05;
  else if (req.minimum_nights > 7) p -= 0.05;

  if (req.review_scores_rating >= 4.7)      p += 0.10;
  else if (req.review_scores_rating >= 4.0) p += 0.05;

  if (req.accommodates >= 6)      p += 0.06;
  else if (req.accommodates >= 4) p += 0.03;

  if (req.number_of_reviews > 100) p += 0.07;
  else if (req.number_of_reviews > 50) p += 0.04;

  const nbBonus = {
    'la Barceloneta': 0.12,
    'el Barri Gòtic': 0.10,
    "la Dreta de l'Eixample": 0.09,
    'Sant Antoni': 0.08,
    'el Raval': 0.06,
    'la Vila de Gràcia': 0.07,
    'el Poblenou': 0.07,
  };
  p += nbBonus[req.neighbourhood] ?? 0.04;

  if (req.room_type === 'Entire home/apt') p += 0.05;

  return Math.min(Math.max(p, 0.04), 0.97);
};

/**
 * Calls the real API to get an occupancy prediction.
 *
 * POST /predict  (requiere JWT en Authorization: Bearer <token>)
 * Body: { date, neighbourhood, room_type, accommodates,
 *         listing_price, minimum_nights, number_of_reviews,
 *         review_scores_rating, instant_bookable }
 * Response: { probability: 0.73 }
 *
 * @param {import('../models/prediction').PredictionRequest} request
 * @returns {Promise<{ probability: number }>}
 */
export const predictOccupancy = async (request) => {
  const token = localStorage.getItem('auth_token');
  try {
    const controller = new AbortController();
    const timer      = setTimeout(() => controller.abort(), 10000);

    const body = {
      apartment_id:         request.apartment_id,
      is_simulation:        request.is_simulation,
      date:                 request.date,
      neighbourhood:        request.neighbourhood,
      room_type:            request.room_type,
      accommodates:         request.accommodates,
      listing_price:        request.listing_price,
      minimum_nights:       request.minimum_nights,
      number_of_reviews:    request.number_of_reviews,
      review_scores_rating: request.review_scores_rating,
      instant_bookable:     request.instant_bookable
    };

    const res = await fetch(`${API_BASE}/predict`, {
      method:  'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(token ? { 'Authorization': `Bearer ${token}` } : {}),
      },
      body:   JSON.stringify(body),
      signal: controller.signal,
    });
    clearTimeout(timer);

    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch {
    await new Promise(r => setTimeout(r, 950));
    return { probability: mockPredict(request) };
  }
};
