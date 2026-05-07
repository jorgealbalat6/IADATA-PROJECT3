import numpy as np
import pandas as pd
import json


def load_neigh_mean_price(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def build_listing_features(listing: dict, neigh_mean_price: dict) -> dict:
    price = listing['listing_price']
    accommodates = listing['accommodates'] or 1

    return {
        'neighbourhood_cleansed': listing['neighbourhood_cleansed'],
        'room_type':              listing['room_type'],
        'accommodates':           accommodates,
        'log_price':              np.log1p(price),
        'price_per_person':       price / accommodates,
        'price_vs_neigh_mean':    price / neigh_mean_price.get(
                                      listing['neighbourhood_cleansed'], price),
        'log_min_nights':         np.log1p(listing['minimum_nights']),
        'log_reviews':            np.log1p(listing['number_of_reviews']),
        'review_scores_rating':   listing.get('review_scores_rating') or 0,
        'has_reviews':            1 if listing['number_of_reviews'] > 0 else 0,
        'instant_bookable':       int(listing['instant_bookable']),
    }


def build_full_row(context_row: dict, listing_features: dict) -> pd.DataFrame:
    row = {**context_row, **listing_features}
    return pd.DataFrame([row])