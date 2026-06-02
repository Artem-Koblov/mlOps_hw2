import json
import logging
import os

import psycopg2
from confluent_kafka import Consumer
from prometheus_client import start_http_server, Summary, Counter, Histogram, Gauge

# Настройка логгирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_WRITE_TIME = Summary('scoring_db_write_seconds', 'Время записи скоринга в БД', ['us_state', 'merch'])
SCORING_COUNT = Counter('scorings_total', 'Общее количество записанных скорингов', ['us_state', 'merch'])
FRAUD_SCORE_HISTOGRAM = Histogram('scoring_fraud_score', 'Распределение записанных скоров мошенничества', 
                                ['us_state', 'merch'],
                                buckets=[i/50.0 for i in range(51)])
FRAUD_COUNT = Counter('fraud_detected_total', 'Количество обнаруженных мошеннических транзакций', ['us_state', 'merch'])
FRAUD_RATE_BY_CATEGORY = Gauge('fraud_rate_by_category', 'Доля фродовых транзакций по категориям', 
                               ['category', 'us_state', 'merch'])


def get_db_config():
    return {
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
        "database": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }


def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scores (
                id SERIAL PRIMARY KEY,
                transaction_id TEXT NOT NULL,
                score FLOAT NOT NULL,
                fraud_flag INT NOT NULL,
                us_state TEXT,
                merch TEXT,
                cat_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        logger.info("Таблица 'scores' проверена или создана.")


def update_fraud_rate_by_category(conn, us_state, merch):
    """Обновляет метрику доли фрода по категориям на основе последних 1000 транзакций"""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT cat_id, 
                       COUNT(*) as total,
                       SUM(fraud_flag) as fraud_count
                FROM (
                    SELECT cat_id, fraud_flag
                    FROM scores
                    WHERE (us_state = %s OR %s IS NULL)
                      AND (merch = %s OR %s IS NULL)
                    ORDER BY created_at DESC
                    LIMIT 1000
                ) AS recent
                WHERE cat_id IS NOT NULL
                GROUP BY cat_id
            """, (us_state, us_state, merch, merch))
            
            results = cur.fetchall()
            
            for cat_id, total, fraud_count in results:
                fraud_rate = fraud_count / total if total > 0 else 0
                FRAUD_RATE_BY_CATEGORY.labels(
                    category=str(cat_id), 
                    us_state=us_state if us_state else 'all', 
                    merch=merch if merch else 'all'
                ).set(fraud_rate)
            
            logger.debug(f"Обновлены метрики по категориям: {len(results)} категорий")
    except Exception as e:
        logger.error(f"Ошибка обновления метрик по категориям: {e}")


def insert_score(conn, data):
    # Извлекаем лейблы
    us_state = data.get("us_state", "unknown")
    merch = data.get("merch", "unknown")
    cat_id = data.get("cat_id", "unknown")
    
    # Используем контекстный менеджер для измерения времени с лейблами
    with DB_WRITE_TIME.labels(us_state=us_state, merch=merch).time():
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO scores (transaction_id, score, fraud_flag, us_state, merch, cat_id)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (
                data["transaction_id"], 
                data["score"], 
                data["fraud_flag"],
                us_state,
                merch, 
                cat_id
            ))
            conn.commit()
    
    # Обновляем метрики с лейблами
    SCORING_COUNT.labels(us_state=us_state, merch=merch).inc()
    FRAUD_SCORE_HISTOGRAM.labels(us_state=us_state, merch=merch).observe(data["score"])
    
    if data["fraud_flag"] == 1:
        FRAUD_COUNT.labels(us_state=us_state, merch=merch).inc()
    
    logger.debug(f"Записано в БД: {data['transaction_id']}, us_state={us_state}, merch={merch}, score={data['score']:.3f}")


def run_consumer():
    # Запуск HTTP-сервера для Prometheus
    start_http_server(8001)
    logger.info("Prometheus метрики доступны на порту 8001")
    
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    scoring_topic = os.getenv("KAFKA_SCORING_TOPIC")

    consumer_config = {
        'bootstrap.servers': kafka_bootstrap_servers,
        'group.id': 'scoring-writer',
        'auto.offset.reset': 'earliest',
    }

    logger.info(f"Подключение к Kafka: {kafka_bootstrap_servers}, топик: {scoring_topic}")
    consumer = Consumer(consumer_config)
    consumer.subscribe([scoring_topic])

    db_config = get_db_config()
    conn = psycopg2.connect(**db_config)
    create_table(conn)
    
    message_counter = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                value_str = msg.value().decode('utf-8')
                logger.debug(f"Получено сообщение: {value_str}")
                data_list = json.loads(value_str)

                if isinstance(data_list, list):
                    for data in data_list:
                        required_keys = ['transaction_id', 'score', 'fraud_flag']
                        if not all(k in data for k in required_keys):
                            logger.error(f"Некорректный формат элемента: {data}")
                            continue
                        insert_score(conn, data)
                        message_counter += 1
                        
                        if message_counter % 10 == 0:
                            us_states = ['CA', 'NY', 'TX', 'FL', 'IL']
                            for us_state in us_states:
                                for merch in ['Amazon', 'Walmart', 'Target', 'BestBuy']:
                                    update_fraud_rate_by_category(conn, us_state, merch)
                            
                else:
                    logger.error(f"Ожидался список, получен: {type(data_list)}")

            except json.JSONDecodeError as je:
                logger.exception(f"Ошибка декодирования JSON: {je}")
            except Exception as e:
                logger.exception(f"Ошибка обработки сообщения: {e}")

    except KeyboardInterrupt:
        logger.info("Потребитель остановлен пользователем.")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    logger.info("Запуск потребителя и писателя в БД...")
    run_consumer()