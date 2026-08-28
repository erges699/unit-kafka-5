"""
Модуль BatchMessageConsumer.

Читает сообщения из Kafka пачками (минимум 10 сообщений за poll).
Offset коммитится вручную после обработки всей пачки.
"""

import logging
from typing import Any

from confluent_kafka import Consumer

from models import KafkaMessage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] BatchMessageConsumer: %(message)s",
)
logger = logging.getLogger("batch_consumer")

BOOTSTRAP_SERVERS = "kafka-0:9092"
TOPIC = "kafka_1_topic"
GROUP_ID = "batch-message-consumer-group"

BATCH_SIZE = 10
POLL_TIMEOUT = 1.0


def main() -> None:
    conf: dict[str, Any] = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        # auto.offset.reset (str):
        #   "earliest" — начать с самого первого сообщения,
        #   если offset не сохранён.
        "auto.offset.reset": "earliest",
        # enable.auto.commit (bool):
        #   False — отключаем автоматический коммит.
        #   Offset будем коммитить вручную после обработки пачки.
        "enable.auto.commit": False,
        # fetch.min.bytes (int):
        #   Минимальный объём данных (в байтах) для одного fetch-запроса.
        #   Значение 1024 означает, что брокер будет ждать,
        #   пока не накопится минимум 1 КБ данных, прежде чем ответить.
        #   Это позволяет накапливать сообщения в пачки.
        "fetch.min.bytes": 1024,
        # fetch.wait.max.ms (int):
        #   Максимальное время ожидания (в миллисекундах),
        #   которое брокер может ждать накопления fetch.min.bytes.
        #   Если за 5000 мс не накопилось достаточно данных,
        #   брокер вернёт то, что есть (возможно, пустой ответ).
        "fetch.wait.max.ms": 5000,
        # max.poll.interval.ms (int):
        #   Максимальное время между вызовами poll().
        #   Если консьюмер не вызывает poll() дольше этого времени,
        #   он считается мёртвым, и его партиции перераспределяются.
        "max.poll.interval.ms": 300000,
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])
    logger.info("Batch consumer started, subscribed to topic '%s'", TOPIC)

    try:
        while True:
            # Накопление пачки сообщений
            batch: list[Any] = []
            while len(batch) < BATCH_SIZE:
                msg = consumer.poll(timeout=POLL_TIMEOUT)

                if msg is None:
                    # Если уже есть сообщения в пачке — продолжаем
                    # накопление до BATCH_SIZE или до следующей итерации
                    if batch:
                        continue
                    # Если пачка пуста — ждём следующего poll
                    break

                if msg.error():
                    logger.error("Consumer error: %s", msg.error())
                    continue

                batch.append(msg)

            if not batch:
                continue

            logger.info(
                "Received batch of %d message(s)",
                len(batch),
            )

            # Обрабатываем каждое сообщение в пачке
            for msg in batch:
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
                    "Processed: %s [partition=%d, offset=%d]",
                    message,
                    msg.partition(),
                    msg.offset(),
                )

            # Ручной коммит offset'ов после обработки всей пачки.
            # commit(asynchronous=False):
            #   Синхронный коммит — блокирует выполнение до подтверждения.
            #   Если коммит не удался, при следующем запуске консьюмер
            #   перечитает сообщения заново (At Least Once).
            try:
                consumer.commit(asynchronous=False)
                logger.info("Offsets committed successfully")
            except Exception as e:
                logger.error("Failed to commit offsets: %s", e)

    except KeyboardInterrupt:
        logger.info("Batch consumer shutting down...")
    finally:
        consumer.close()
        logger.info("Batch consumer stopped.")


if __name__ == "__main__":
    main()