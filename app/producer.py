"""
Модуль продюсера Kafka.

Отправляет сообщения в топик kafka_1_topic с гарантией доставки At Least Once.
Использует асинхронную модель push с callback-подтверждением.
"""

import logging
import time
from typing import Any

from confluent_kafka import Producer

from models import KafkaMessage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("producer")

BOOTSTRAP_SERVERS = "kafka-0:9092"
TOPIC = "kafka_1_topic"


def delivery_report(err: Any, msg: Any) -> None:
    """
    Callback для подтверждения доставки сообщения
    (асинхронная модель push).

    Вызывается после того, как брокер подтвердил (или отклонил) запись.

    Параметры:
        err: Ошибка (None, если доставка успешна).
        msg: Объект сообщения с информацией о партиции и offset.
    """
    if err is not None:
        logger.error("Failed to deliver message: %s", err)
    elif msg is not None:
        logger.info(
            "Message delivered to %s [%d] at offset %d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def main() -> None:
    conf: dict[str, Any] = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "producer-instance",
        # ------------------------------------------------------------
        # Гарантия доставки At Least Once («Как минимум один раз»)
        # ------------------------------------------------------------
        # acks (str):
        #   Определяет, сколько реплик должны подтвердить запись.
        #   Возможные значения:
        #     - "0"  (none):   Без подтверждения. Самая высокая
        #                       пропускная способность, возможна потеря данных.
        #     - "1"  (leader): Подтверждение только от лидера. Баланс
        #                       производительности и надёжности.
        #     - "all" (-1):    Подтверждение от всех in-sync реплик.
        #                       Максимальная надёжность (At Least Once).
        "acks": "all",
        # retries (int):
        #   Количество повторных попыток отправки при временных ошибках
        #   (например, LeaderNotAvailable, NotEnoughReplicas).
        #   Значение 5 означает до 5 повторных попыток перед ошибкой.
        "retries": 5,
        # enable.idempotence (bool):
        #   Идемпотентный режим продюсера.
        #   При включении Kafka присваивает каждому сообщению
        #   уникальный Producer ID и Sequence Number.
        #   Это предотвращает дублирование сообщений при повторных
        #   отправках (retries), обеспечивая Exactly-Once семантику
        #   в рамках одной сессии продюсера.
        #   Требует acks=all и retries > 0.
        "enable.idempotence": True,
    }
    producer = Producer(conf)
    logger.info(
        "Producer started with delivery guarantees: "
        "acks=%s, retries=%s, idempotence=%s",
        conf["acks"],
        conf["retries"],
        conf["enable.idempotence"],
    )
    logger.info("Sending messages to topic '%s'", TOPIC)

    producer_conf = {
        "bootstrap.servers": "localhost:9093",
        "security.protocol": "SSL",
        "ssl.ca.location": "ca.crt",  # Сертификат центра сертификации
        "ssl.certificate.location": "kafka-1-creds/kafka-1.crt",  # Сертификат клиента Kafka
        "ssl.key.location": "kafka-1-creds/kafka-1.key",  # Приватный ключ для клиента Kafka
        # Настройки SASL-аутентификации
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "admin",
        "sasl.password": "admin-secret",
    }
    producer = Producer(producer_conf)
    key = f"key-{uuid.uuid4()}"
    value = "SSL message"
    producer.produce(
        "ssl-topic",
        key=key,
        value=value,
    )
    """   
    value = "SASL/PLAIN"
    producer.produce(
        "sasl-plain-topic",
        key=key,
        value=value,
    )
   """
    producer.flush()
    print(f"Отправлено сообщение: {key=}, {value=}") 

    msg_counter = 0
    try:
        while True:
            # Создаём объект сообщения и сериализуем его
            message = KafkaMessage(
                msg_id=msg_counter,
                data=f"Message #{msg_counter}",
            )
            logger.info("Sending: %s", message)

            value = message.serialize()
            if value is None:
                logger.error(
                    "Skipping message id=%d due to serialization error",
                    msg_counter,
                )
                msg_counter += 1
                continue

            # Асинхронная отправка (push-модель) с callback
            # producer.produce() не блокирует выполнение —
            # сообщение ставится во внутреннюю очередь,
            # а фактическая отправка происходит в фоновом потоке.
            # Callback delivery_report вызывается после подтверждения брокера.
            producer.produce(
                topic=TOPIC,
                value=value,
                callback=delivery_report,
            )

            # Триггерим фактические отправки
            # producer.poll(0) обрабатывает события доставки
            # и вызывает callback'и для завершённых отправок.
            producer.poll(0)

            msg_counter += 1
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Producer shutting down...")
    except BufferError:
        logger.warning("Producer queue is full; flushing and retrying...")
        producer.flush()
    finally:
        # Ожидаем доставки всех оставшихся сообщений перед выходом
        producer.flush()
        logger.info("Producer stopped.")


if __name__ == "__main__":
    main()