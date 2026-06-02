import json
import logging
import os
import sys

import pandas as pd
from confluent_kafka import Consumer
from confluent_kafka import Producer
from prometheus_client import start_http_server, Summary, Counter, Histogram, Gauge

sys.path.append(os.path.abspath('./src'))  # noqa: PTH100
from preprocessing_inference import run_preproc
from scorer import make_pred


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# Set kafka configuration file
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")

# Определяем метрики с лейблами us_state и merch
PROCESSING_TIME = Summary('transaction_processing_seconds', 'Время обработки транзакции', ['us_state', 'merch'])
TRANSACTION_COUNT = Counter('transactions_total', 'Общее количество обработанных транзакций', ['us_state', 'merch'])

# Создаем более детальную гистограмму для распределения скоров
# Используем линейные бакеты с шагом 0.02 от 0 до 1 (50 бакетов)
FRAUD_SCORE = Histogram('fraud_score', 'Распределение скоров мошенничества',
                       ['us_state', 'merch'],
                       buckets=[i/50.0 for i in range(51)])  # [0.0, 0.02, 0.04, ..., 0.98, 1.0]

FRAUD_RATIO = Gauge('fraud_ratio', 'Соотношение мошеннических транзакций к общему числу', ['us_state', 'merch'])

# Метрика для barplot с долей фрода по категориям (последние 1000)
FRAUD_RATE_BY_CATEGORY = Gauge('fraud_rate_by_category', 'Доля фродовых транзакций по категориям', 
                               ['category', 'us_state', 'merch'])


class ProcessingService:
    def __init__(self):
        logger.info("Initializing ProcessingService (Kafka mode)...")
        
        # Загрузка модели
        self.model = None
        self.model_path = 'models/my_catboost.cbm'
        self.load_model()
        
        # Kafka конфигурация
        self.consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'ml-scorer',
            'auto.offset.reset': 'earliest',
        }
        self.producer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        }
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([TRANSACTIONS_TOPIC])
        self.producer = Producer(self.producer_config)
        
        # Счетчики для метрик с разбивкой по штатам и мерчам
        self.transaction_stats = {}  # {(us_state, merch): {'total': 0, 'fraud': 0, 'scores': []}}
        
        # История для последних 1000 транзакций (для barplot по категориям)
        self.recent_transactions = []  # список последних 1000 транзакций {'cat_id', 'fraud_flag', 'us_state', 'merch'}
        
        # Запуск HTTP-сервера для Prometheus
        start_http_server(8000)
        logger.info("Prometheus метрики доступны на порту 8000")

    def load_model(self):
        """Загрузка модели CatBoost"""
        try:
            from catboost import CatBoostClassifier
            self.model = CatBoostClassifier()
            self.model.load_model(self.model_path)
            logger.info("Model loaded successfully")
            logger.info(f"Model expects {len(self.model.feature_names_)} features")
        except Exception as e:
            logger.error(f"Error loading model: {e}")
            raise

    def update_recent_transactions(self, cat_id, fraud_flag, us_state, merch):
        """Обновление списка последних 1000 транзакций и пересчет метрик по категориям"""
        self.recent_transactions.append({
            'cat_id': cat_id,
            'fraud_flag': fraud_flag,
            'us_state': us_state,
            'merch': merch
        })
        
        # Оставляем только последние 1000
        if len(self.recent_transactions) > 1000:
            self.recent_transactions.pop(0)
        
        # Пересчитываем долю фрода по каждой категории для последних 1000 транзакций
        fraud_by_category = {}
        category_count = {}
        
        for txn in self.recent_transactions:
            key = (txn['cat_id'], txn['us_state'], txn['merch'])
            fraud_by_category[key] = fraud_by_category.get(key, 0) + txn['fraud_flag']
            category_count[key] = category_count.get(key, 0) + 1
        
        # Обновляем метрики
        for (cat_id, us_state, merch), fraud_count in fraud_by_category.items():
            total = category_count.get((cat_id, us_state, merch), 1)
            fraud_rate = fraud_count / total if total > 0 else 0
            FRAUD_RATE_BY_CATEGORY.labels(category=str(cat_id), us_state=us_state, merch=merch).set(fraud_rate)

    def process_message(self, msg):
        try:
            # Десериализация JSON
            data = json.loads(msg.value().decode('utf-8'))

            # Извлекаем ID и данные
            transaction_id = data['transaction_id']
            transaction_data = data['data']
            input_df = pd.DataFrame([transaction_data])
            
            # Извлекаем лейблы для метрик
            us_state = transaction_data.get('us_state', 'unknown')
            merch = transaction_data.get('merch', 'unknown')
            cat_id = transaction_data.get('cat_id', 'unknown')

            # Препроцессинг и предсказание
            processed_df = run_preproc(input_df)
            
            # Проверяем соответствие признаков модели
            expected_features = self.model.feature_names_
            missing_features = set(expected_features) - set(processed_df.columns)
            extra_features = set(processed_df.columns) - set(expected_features)
            
            if missing_features:
                logger.warning(f"Missing {len(missing_features)} features, adding with default values")
                for feat in missing_features:
                    processed_df[feat] = 0
            
            if extra_features:
                logger.warning(f"Extra {len(extra_features)} features, dropping")
                processed_df = processed_df.drop(columns=list(extra_features))
            
            # Убеждаемся, что порядок колонок совпадает
            processed_df = processed_df[expected_features]

            # Получаем предсказания
            predictions_binary, y_proba = make_pred(processed_df, self.model)

            # Порог для определения мошенничества (0.75)
            fraud_flag = 1 if y_proba[0] > 0.75 else 0

            # Обновляем метрики с лейблами
            PROCESSING_TIME.labels(us_state=us_state, merch=merch).time()
            TRANSACTION_COUNT.labels(us_state=us_state, merch=merch).inc()
            FRAUD_SCORE.labels(us_state=us_state, merch=merch).observe(y_proba[0])
            
            # Обновляем локальные счетчики для FRAUD_RATIO
            key = (us_state, merch)
            if key not in self.transaction_stats:
                self.transaction_stats[key] = {'total': 0, 'fraud': 0}
            
            self.transaction_stats[key]['total'] += 1
            if fraud_flag == 1:
                self.transaction_stats[key]['fraud'] += 1
            
            # Обновляем соотношение мошеннических транзакций
            total = self.transaction_stats[key]['total']
            fraud = self.transaction_stats[key]['fraud']
            FRAUD_RATIO.labels(us_state=us_state, merch=merch).set(fraud / total if total > 0 else 0)

            # Обновляем историю для barplot по категориям
            self.update_recent_transactions(cat_id, fraud_flag, us_state, merch)

            # Формируем результаты с дополнительными полями
            result = {
                'transaction_id': transaction_id,
                'score': float(y_proba[0]),
                'fraud_flag': fraud_flag,
                'us_state': us_state,
                'merch': merch,
                'cat_id': cat_id
            }

            # Отправка результата в топик scoring
            self.producer.produce(
                SCORING_TOPIC,
                value=json.dumps([result]),  # Отправляем как список для совместимости
            )
            self.producer.flush()
            
            logger.info(f"Processed transaction {transaction_id}: us_state={us_state}, merch={merch}, score={y_proba[0]:.3f}, fraud_flag={fraud_flag}")
            return True
        except Exception as e:
            logger.exception(f"Error processing message: {e}")
            return False

    def process_messages(self):
        logger.info(f"Starting to process messages from topic {TRANSACTIONS_TOPIC}")
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue
            
            self.process_message(msg)


if __name__ == "__main__":
    logger.info('Starting Kafka ML scoring service...')
    service = ProcessingService()
    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info('Service stopped by user')