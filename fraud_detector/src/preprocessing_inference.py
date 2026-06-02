import pandas as pd
import numpy as np
import logging
from geopy.distance import great_circle

logger = logging.getLogger(__name__)

def run_preproc(input_df):
    """
    Предобработка для inference (без обучения)
    Применяет только необходимые преобразования к данным
    """
    logger.info("Starting inference preprocessing...")
    
    # 1. Удаляем ненужные колонки, если они есть
    cols_to_drop = ['name_1', 'name_2', 'street', 'post_code']
    for col in cols_to_drop:
        if col in input_df.columns:
            input_df = input_df.drop(columns=[col])
            logger.debug(f"Dropped column: {col}")
    
    # 2. Преобразуем transaction_time в признаки
    if 'transaction_time' in input_df.columns:
        logger.debug("Adding time features...")
        input_df['transaction_time'] = pd.to_datetime(input_df['transaction_time'])
        dt = input_df['transaction_time'].dt
        input_df['hour'] = dt.hour
        input_df['year'] = dt.year
        input_df['month'] = dt.month
        input_df['day_of_month'] = dt.day
        input_df['day_of_week'] = dt.dayofweek
        input_df = input_df.drop(columns=['transaction_time'])
    
    # 3. Создаем категориальные признаки (как строки)
    categorical_cols = ['gender', 'merch', 'cat_id', 'one_city', 'us_state', 'jobs']
    for col in categorical_cols:
        if col in input_df.columns:
            # Преобразуем в строку и заполняем пропуски
            input_df[col] = input_df[col].fillna('unknown').astype(str)
            input_df[f'{col}_cat'] = input_df[col]
            input_df = input_df.drop(columns=[col])
    
    # 4. Вычисляем расстояние между координатами
    if all(col in input_df.columns for col in ['lat', 'lon', 'merchant_lat', 'merchant_lon']):
        logger.debug("Calculating distances...")
        input_df['distance'] = input_df.apply(
            lambda x: great_circle(
                (x['lat'], x['lon']), 
                (x['merchant_lat'], x['merchant_lon'])
            ).km,
            axis=1
        )
        input_df = input_df.drop(columns=['lat', 'lon', 'merchant_lat', 'merchant_lon'])
    
    # 5. Логарифмируем числовые признаки
    numeric_cols = ['amount', 'population_city']
    for col in numeric_cols:
        if col in input_df.columns:
            input_df[f'{col}_log'] = np.log(input_df[col] + 1)
            input_df = input_df.drop(columns=[col])
    
    # Логарифмируем расстояние
    if 'distance' in input_df.columns:
        input_df['distance_log'] = np.log(input_df['distance'] + 1)
        input_df = input_df.drop(columns=['distance'])
    
    # 6. Преобразуем hour, year, month, day_of_month, day_of_week в строки для категориальных признаков
    time_features = ['hour', 'year', 'month', 'day_of_month', 'day_of_week']
    for col in time_features:
        if col in input_df.columns:
            input_df[col] = input_df[col].astype(str)
            # Добавляем mean_enc (заглушка, так как нет train данных)
            # Для inference используем значение по умолчанию
            input_df[f'{col}_mean_enc'] = 0.0
    
    # 7. Заполняем пропуски
    input_df = input_df.fillna(0)
    
    logger.info(f"Preprocessing completed. Output shape: {input_df.shape}")
    logger.info(f"Columns: {list(input_df.columns)}")
    
    return input_df

def load_train_data():
    """Заглушка для совместимости - не используется в inference"""
    logger.info("Inference mode: no training data needed")
    return None
