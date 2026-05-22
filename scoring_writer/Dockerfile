FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir prometheus_client==0.16.0
COPY . .

# Экспозиция порта для Prometheus
EXPOSE 8001

CMD ["python", "app.py"]