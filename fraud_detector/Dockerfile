FROM python:3.11-slim

WORKDIR /app

# Создание директории для логов
RUN mkdir -p /app/logs && \
    touch /app/logs/service.log && \
    chmod -R 777 /app/logs  # Права на запись для всех пользователей

# Установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir prometheus_client==0.16.0

# Копирование исходного кода
COPY . .

# Экспозиция порта для Prometheus
EXPOSE 8000

CMD ["python", "./app/app.py"]