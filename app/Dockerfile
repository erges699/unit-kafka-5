FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py single_consumer.py batch_consumer.py models.py ./

CMD ["python", "producer.py"]