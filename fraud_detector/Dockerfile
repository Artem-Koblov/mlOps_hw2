FROM python:3.12-slim

WORKDIR /app

# Создание директории для логов
RUN mkdir -p /app/logs && \
    touch /app/logs/service.log && \
    chmod -R 777 /app/logs

# Установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование исходного кода
COPY app/ ./app/
COPY src/ ./src/
COPY models/ ./models/

# Установка PYTHONPATH
ENV PYTHONPATH=/app

# Точки монтирования
VOLUME /app/input
VOLUME /app/output

CMD ["python", "/app/app/app.py"]
