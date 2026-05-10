import pandas as pd
import numpy as np

def create_neighborhood_momentum_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula variables de 'Momentum' a nivel de barrio para mitigar el Cold Start.
    Aplica shift(2) estructural para garantizar resiliencia ETL de 48h.
    """
    print("[ColdStart] Calculando inercia de barrio (Lags y Derivada de Reviews)")
    
    # 1. Asegurar orden cronológico para el cálculo por piso
    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'])
    df_sorted = df.sort_values(['listing_id', 'date']).copy()

    # 2. La Derivada: Nuevas reviews diarias por listing_id
    df_sorted['reviews_nuevas'] = df_sorted.groupby('listing_id')['number_of_reviews'].diff().fillna(0)
    df_sorted['reviews_nuevas'] = df_sorted['reviews_nuevas'].clip(lower=0)

    # 3. Agrupar la foto RAW del barrio en el día T
    neighborhood_daily = df_sorted.groupby(['neighbourhood_cleansed', 'date']).agg({
        'is_occupied': 'mean',
        'reviews_nuevas': 'sum'
    }).rename(columns={
        'is_occupied': 'occ_barrio_raw',
        'reviews_nuevas': 'reviews_barrio_raw'
    }).reset_index()

    neighborhood_daily = neighborhood_daily.sort_values(['neighbourhood_cleansed', 'date'])

    # A partir de esta línea, "raw_delayed" simula lo que el ETL ve con 48h de retraso.  
    neighborhood_daily['occ_barrio_raw_delayed'] = neighborhood_daily.groupby('neighbourhood_cleansed')['occ_barrio_raw'].shift(2)
    neighborhood_daily['reviews_barrio_raw_delayed'] = neighborhood_daily.groupby('neighbourhood_cleansed')['reviews_barrio_raw'].shift(2)

    # 5. Calcular Momentum sobre la realidad retrasada
    neighborhood_daily['occ_barrio_lag7'] = neighborhood_daily.groupby('neighbourhood_cleansed')['occ_barrio_raw_delayed'].shift(5) # 2 + 5 = 7 días de lag real
    neighborhood_daily['occ_barrio_lag14'] = neighborhood_daily.groupby('neighbourhood_cleansed')['occ_barrio_raw_delayed'].shift(12) # 2 + 12 = 14 días de lag real
    
    # Rolling 14 días de reviews (Calculado sobre la columna que ya tiene el shift de 2 días)
    neighborhood_daily['reviews_barrio_momentum'] = neighborhood_daily.groupby('neighbourhood_cleansed')['reviews_barrio_raw_delayed'].transform(
        lambda x: x.rolling(window=14, min_periods=1).sum())

    # 6. Join final solo de las variables calculadas (excluimos los RAW para evitar Leakage)
    cols_to_join = ['occ_barrio_raw_delayed', 'occ_barrio_lag7', 'occ_barrio_lag14', 'reviews_barrio_momentum']
    
    df_out = df.merge(
        neighborhood_daily[['neighbourhood_cleansed', 'date'] + cols_to_join],
        on=['neighbourhood_cleansed', 'date'],
        how='left')
    
    print(f"[ColdStart] Features inyectadas con éxito: {cols_to_join}")
    return df_out