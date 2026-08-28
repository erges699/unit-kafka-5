FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Приложения для работы с Kafka по SSL
COPY producer.py consumer.py models.py ./

CMD ["python", "producer.py"]