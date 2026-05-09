// ============================================================
// property.js  — Domain model for registered properties
// ============================================================

import { NEIGHBOURHOODS, ROOM_TYPES } from './prediction';

export { NEIGHBOURHOODS, ROOM_TYPES };

/**
 * @typedef {Object} Property
 * @property {string}  id                   - Unique identifier (from backend)
 * @property {string}  name                 - Display name / title of the listing
 * @property {string}  neighbourhood        - Neighbourhood key
 * @property {string}  room_type            - Room type key
 * @property {number}  accommodates         - Max guests
 * @property {number}  bedrooms             - Number of bedrooms
 * @property {number}  beds                 - Number of beds
 * @property {number}  number_of_reviews    - Total reviews count
 * @property {string}  [created_at]         - ISO timestamp from backend
 */

/** Creates a blank property form ready for the register form. */
export const createDefaultProperty = () => ({
  name:                 '',
  neighbourhood:        NEIGHBOURHOODS[0],
  room_type:            ROOM_TYPES[0],
  accommodates:         2,
  bedrooms:             1,
  beds:                 1,
  number_of_reviews:    0,
  listing_price:        80,
  minimum_nights:       2,
  instant_bookable:     false,
});
