import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)
model_th = 0.75

def make_pred(input_df, model):
    """
    Make predictions using the loaded model
    
    Args:
        input_df: DataFrame with preprocessed features
        model: Loaded CatBoost model
    
    Returns:
        tuple: (predictions_binary, predictions_proba)
    """
    try:
        # Получаем вероятности
        predictions_proba = model.predict_proba(input_df)[:, 1]
        
        # Бинаризуем по порогу
        predictions_binary = (predictions_proba > model_th).astype(int)
        
        logger.info(f"Prediction complete. Shape: {predictions_binary.shape}")
        logger.info(f"Prediction distribution: 0: {sum(predictions_binary==0)}, 1: {sum(predictions_binary==1)}")
        logger.info(f"Prediction probabilities - min: {predictions_proba.min():.4f}, max: {predictions_proba.max():.4f}, mean: {predictions_proba.mean():.4f}")
        
        return predictions_binary, predictions_proba
        
    except Exception as e:
        logger.error(f"Error in make_pred: {e}")
        raise
