"""
Модуль SingleMessageConsumer.

Читает сообщения из Kafka по одному за poll().
Offset коммитится автоматически (enable.auto.commit=True).
"""

import logging
from typing import Any

from confluent_kafka import Consumer

from models import KafkaMessage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] SingleMessageConsumer: %(message)s",
)
logger = logging.getLogger("single_consumer")

BOOTSTRAP_SERVERS = "kafka-0:9092"
TOPIC = "kafka_1_topic"
GROUP_ID = "single-message-consumer-group"


def main() -> None:
    conf: dict[str, Any] = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        # auto.offset.reset (str):
        #   Определяет поведение, если нет сохранённого offset'а.
        #   "earliest" — начать с самого первого сообщения.
        #   "latest"   — начать с новых сообщений (пропустить существующие).
        "auto.offset.reset": "earliest",
        # enable.auto.commit (bool):
        #   Включает автоматический коммит offset'ов.
        #   При True offset коммитится каждые auto.commit.interval.ms.
        #   Подходит для сценария "прочитал одно сообщение — обработал".
        "enable.auto.commit": True,
        # auto.commit.interval.ms (int):
        #   Интервал автоматического коммита offset'ов (в миллисекундах).
        #   По умолчанию 5000 (5 секунд).
        "auto.commit.interval.ms": 5000,
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])
    logger.info("Consumer started, subscribed to topic '%s'", TOPIC)

    consumer_conf = {
        "bootstrap.servers": "localhost:9093",
        "group.id": "consumer-ssl-group",
        "auto.offset.reset": "earliest",
        "security.protocol": "SSL",
        "ssl.ca.location": "ca.crt",  # Сертификат центра сертификации
        "ssl.certificate.location": "kafka-1-creds/kafka-1.crt",  # Сертификат клиента Kafka
        "ssl.key.location": "kafka-1-creds/kafka-1.key",  # Приватный ключ для клиента Kafka
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe(["ssl-topic"])
    try:
        while True:
            message = consumer.poll(0.1)
            if message is None:
                continue
            if message.error():
                print(f"Ошибка: {message.error()}")
                continue
            key = message.key().decode("utf-8")
            value = message.value().decode("utf-8")
            print(f"Получено сообщение: {key=}, {value=}, offset={message.offset()}")
    finally:
        consumer.close()

    try:
        while True:
            # poll(timeout=1.0):
            #   Читает ОДНО сообщение из Kafka.
            #   timeout — максимальное время ожидания (сек).
            #   Если сообщений нет, возвращает None.
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            # Десериализация через класс KafkaMessage
            message = KafkaMessage.deserialize(msg.value())
            if message is None:
                logger.error(
                    "Skipping message [partition=%d, offset=%d] "
                    "due to deserialization error",
                    msg.partition(),
                    msg.offset(),
                )
                continue

            logger.info(
                "Received: %s [partition=%d, offset=%d]",
                message,
                msg.partition(),
                msg.offset(),
            )

            # Offset коммитится автоматически (enable.auto.commit=True)
            # При ошибке обработки сообщение будет прочитано повторно
            # после перезапуска (At Least Once на стороне консьюмера).

    except KeyboardInterrupt:
        logger.info("Consumer shutting down...")
    finally:
        consumer.close()
        logger.info("Consumer stopped.")


if __name__ == "__main__":
    main()