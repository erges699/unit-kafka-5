"""
SSL-консьюмер Kafka.

Читает сообщения по защищённому SSL/mTLS соединению.
Аутентифицируется сертификатом клиента CN=consumer.

Демонстрация ACL:
  - topic-1: чтение разрешено (ACL выдан для User:*)
  - topic-2: чтение ЗАПРЕЩЕНО (ACL на чтение не выдавался) ->
    будет получена ошибка авторизации, что подтверждает работу прав доступа.
"""

import logging
import time
from typing import Any

from confluent_kafka import Consumer, KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] Consumer: %(message)s",
)
logger = logging.getLogger("consumer")

CERT_DIR = "/app/certs"
BOOTSTRAP_SERVERS = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
GROUP_ID = "ssl-consumer-group"

# Чтение разрешено
TOPIC_ALLOWED = "topic-1"
# Чтение запрещено (демонстрация ACL)
TOPIC_DENIED = "topic-2"


def ssl_conf() -> dict[str, str]:
    """Настройки SSL/mTLS для консьюмера (сертификат клиента CN=consumer)."""
    return {
        "security.protocol": "SSL",
        "ssl.ca.location": f"{CERT_DIR}/ca.crt",
        "ssl.certificate.location": f"{CERT_DIR}/consumer.crt",
        "ssl.key.location": f"{CERT_DIR}/consumer.key",
    }


def main() -> None:
    conf: dict[str, Any] = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        **ssl_conf(),
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_ALLOWED, TOPIC_DENIED])
    logger.info(
        "Consumer started (SSL), subscribed to %s (allowed) and %s (denied)",
        TOPIC_ALLOWED, TOPIC_DENIED,
    )

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                # Ошибки авторизации на topic-2 попадают сюда
                if msg.error().code() == KafkaError.TOPIC_AUTHORIZATION_FAILED:
                    logger.warning(
                        "Доступ запрещён к топику '%s' — ACL корректно блокирует чтение",
                        TOPIC_DENIED,
                    )
                elif msg.error().code() == KafkaError.GROUP_AUTHORIZATION_FAILED:
                    logger.warning(
                        "Доступ запрещён к группе — ACL корректно блокирует чтение"
                    )
                else:
                    logger.error("Consumer error: %s", msg.error())
                continue

            if msg.topic() == TOPIC_ALLOWED:
                logger.info(
                    "Получено из %s: key=%s value=%s [partition=%d offset=%d]",
                    msg.topic(), msg.key(), msg.value().decode("utf-8"),
                    msg.partition(), msg.offset(),
                )
            time.sleep(0.2)
    except KeyboardInterrupt:
        logger.info("Consumer shutting down...")
    finally:
        consumer.close()
        logger.info("Consumer stopped.")


if __name__ == "__main__":
    main()