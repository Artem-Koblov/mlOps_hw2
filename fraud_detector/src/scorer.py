import logging
import os

import pandas as pd
from catboost import CatBoostClassifier


# Настройка логгера
logger = logging.getLogger(__name__)

logger.info('Importing pretrained model...')

# Import model
os.environ["CUDA_VISIBLE_DEVICES"] = "-1"
model = CatBoostClassifier()
model.load_model('./models/my_catboost.cbm')

# Define optimal threshold
THRESHOLD = 0.5
logger.info('Pretrained model imported successfully...')


# Make prediction
def make_pred(dt, source_info="kafka"):
    y_proba = model.predict_proba(dt)[:, 1]

    # Calculate score
    submission = pd.DataFrame({
        'score': y_proba,
        'fraud_flag': (y_proba > THRESHOLD) * 1,
    })
    logger.info(f'Prediction complete for data from {source_info}')

    # Return proba for positive class
    return submission, y_proba
