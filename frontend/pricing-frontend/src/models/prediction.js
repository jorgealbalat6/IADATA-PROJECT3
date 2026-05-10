// ============================================================
// prediction.js  — Domain models & pure helpers
// ============================================================

export const NEIGHBOURHOODS = [
  'Eixample', 'Gràcia', 'Barceloneta', 'Born', 'Sarrià-Sant Gervasi',
  'Sant Martí', 'Sants-Montjuïc', 'Horta-Guinardó', 'Nou Barris',
  'Centro', 'Retiro', 'Chamberí', 'Malasaña', 'La Latina', 'Lavapiés',
];

export const ROOM_TYPES = [
  'Entire home/apt',
  'Private room',
  'Shared room',
  'Hotel room',
];

/** Maps demand color keys to hex values. Single source of truth. */
export const DEMAND_COLORS = {
  success: '#16a34a',
  warning: '#d97706',
  danger:  '#dc2626',
  primary: '#1e3a5f',
};

/**
 * Demand level derived from probability.
 * @param {number} probability 0–1
 * @returns {{ level: string, color: string, label: string }}
 */
export const getDemandLevel = (probability) => {
  if (probability >= 0.70) return { level: 'High',   color: 'success', label: 'Alta Demanda'  };
  if (probability >= 0.40) return { level: 'Medium', color: 'warning', label: 'Demanda Media' };
  return                          { level: 'Low',    color: 'danger',  label: 'Baja Demanda'  };
};

/**
 * Estimated model confidence percentage.
 * @param {number} probability 0–1
 * @returns {string}
 */
export const getModelConfidence = (probability) => `${Math.round(85 + probability * 10)}%`;

/**
 * Human-readable explanation of the prediction.
 * @param {Object} request
 * @param {number} probability
 * @returns {string}
 */
export const getInterpretation = (request, probability) => {
  const factors = [];
  if (request.is_weekend)                                      factors.push('fin de semana');
  if (request.is_holiday)                                      factors.push('día festivo');
  if (request.temp_mean >= 18 && request.temp_mean <= 28)     factors.push('temperatura agradable');
  if (request.review_scores_rating >= 4.5)                    factors.push('alta valoración');
  if (request.accommodates >= 4)                               factors.push('alta capacidad');
  if (request.number_of_reviews > 50)                         factors.push('muchas reseñas');

  const demand = getDemandLevel(probability);
  if (factors.length === 0) return `${demand.label}. No se detectan factores destacados.`;
  return `${demand.label} debido a: ${factors.join(', ')}.`;
};

/** Creates fresh default form values evaluated at call time. */
export const createDefaultRequest = () => {
  const today = new Date();
  return {
    neighbourhood:        'Eixample',
    room_type:            'Entire home/apt',
    accommodates:         2,
    bedrooms:             1,
    beds:                 1,
    number_of_reviews:    20,
    review_scores_rating: 4.5,
    date:                 today.toISOString().split('T')[0],
    is_weekend:           [0, 6].includes(today.getDay()) ? 1 : 0,
    temp_mean:            20,
    is_holiday:           0,
  };
};
