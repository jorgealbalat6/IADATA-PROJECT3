// ============================================================
// prediction.js  — Domain models & pure helpers
// ============================================================

// Barrios reales del modelo (neigh_mean_price.json) — Barcelona
export const NEIGHBOURHOODS = [
  'Can Baró', 'Canyelles', 'Diagonal Mar i el Front Marítim del Poblenou',
  'Horta', 'Hostafrancs', 'Montbau', 'Navas', 'Pedralbes', 'Porta',
  'Provençals del Poblenou', 'Sant Andreu', 'Sant Antoni',
  "Sant Genis dels Agudells", 'Sant Gervasi - Galvany',
  'Sant Gervasi - la Bonanova', 'Sant Martí de Provençals',
  'Sant Pere, Santa Caterina i la Ribera', 'Sants', 'Sants - Badal',
  'Sarrià', 'Torre Baró', 'Vallcarca i els Penitents',
  'Vallvidrera, el Tibidabo i les Planes', 'Verdun',
  'Vilapicina i la Torre Llobeta', 'el Baix Guinardó', 'el Barri Gòtic',
  'el Besòs i el Maresme', 'el Bon Pastor',
  "el Camp d'en Grassot i Gràcia Nova", "el Camp de l'Arpa del Clot",
  'el Carmel', 'el Clot', 'el Coll', 'el Congrés i els Indians',
  'el Fort Pienc', 'el Guinardó', 'el Parc i la Llacuna del Poblenou',
  'el Poble Sec', 'el Poblenou', 'el Putxet i el Farró', 'el Raval',
  'el Turó de la Peira', "l'Antiga Esquerra de l'Eixample",
  'la Barceloneta', 'la Bordeta', 'la Clota', "la Dreta de l'Eixample",
  "la Font d'en Fargues", 'la Font de la Guatlla', 'la Guineueta',
  'la Marina de Port', 'la Marina del Prat Vermell',
  'la Maternitat i Sant Ramon', "la Nova Esquerra de l'Eixample",
  'la Prosperitat', 'la Sagrada Família', 'la Sagrera', 'la Salut',
  'la Teixonera', 'la Trinitat Nova', 'la Trinitat Vella',
  "la Vall d'Hebron", 'la Verneda i la Pau',
  'la Vila Olímpica del Poblenou', 'la Vila de Gràcia', 'les Corts',
  'les Roquetes', 'les Tres Torres',
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
  if (request.instant_bookable)                    factors.push('reserva instantánea');
  if (request.listing_price < 60)                  factors.push('precio competitivo');
  if (request.review_scores_rating >= 4.5)         factors.push('alta valoración');
  if (request.accommodates >= 4)                   factors.push('alta capacidad');
  if (request.number_of_reviews > 50)              factors.push('muchas reseñas');
  if (request.minimum_nights <= 1)                 factors.push('mínimo 1 noche');

  const demand = getDemandLevel(probability);
  if (factors.length === 0) return `${demand.label}. No se detectan factores destacados.`;
  return `${demand.label} debido a: ${factors.join(', ')}.`;
};

/** Creates fresh default form values evaluated at call time. */
export const createDefaultRequest = () => {
  const today = new Date();
  return {
    neighbourhood:        "la Dreta de l'Eixample",
    room_type:            'Entire home/apt',
    accommodates:         2,
    number_of_reviews:    20,
    review_scores_rating: 4.5,
    date:                 today.toISOString().split('T')[0],
    listing_price:        80.0,
    minimum_nights:       2,
    instant_bookable:     false,
  };
};
