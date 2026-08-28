"""
SSL-продюсер Kafka.

Отправляет сообщения в топики topic-1 и topic-2 по защищённому
SSL/mTLS соединению. Аутентифицируется сертификатом клиента CN=producer.
"""

import logging
import time
from typing import Any

from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] Producer: %(message)s",
)
logger = logging.getLogger("producer")

CERT_DIR = "/app/certs"
# Внутренний (INTERNAL, SSL) лисенер брокеров — используется внутри Docker-сети
BOOTSTRAP_SERVERS = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
TOPICS = ["topic-1", "topic-2"]


def ssl_conf() -> dict[str, str]:
    """Настройки SSL/mTLS для продюсера (используются сертификаты клиента)."""
    return {
        "security.protocol": "SSL",
        # Доверенный CA
        "ssl.ca.location": f"{CERT_DIR}/ca.crt",
        # Клиентский сертификат (CN=producer) и его ключ
        "ssl.certificate.location": f"{CERT_DIR}/producer.crt",
        "ssl.key.location": f"{CERT_DIR}/producer.key",
    }


def delivery_report(err: Any, msg: Any) -> None:
    """Callback подтверждения доставки сообщения."""
    if err is not None:
        logger.error("Failed to deliver: %s", err)
    elif msg is not None:
        logger.info(
            "Delivered to %s [%d] @ offset %d",
            msg.topic(), msg.partition(), msg.offset(),
        )


def main() -> None:
    conf: dict[str, Any] = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "ssl-producer",
        # At Least Once / Exactly Once (идемпотентность)
        "acks": "all",
        "retries": 5,
        "enable.idempotence": True,
        **ssl_conf(),
    }
    producer = Producer(conf)
    logger.info("Producer started (SSL), targets=%s", TOPICS)

    counter = 0
    try:
        while True:
            for topic in TOPICS:
                value = f"secure-message-{counter}"
                producer.produce(
                    topic=topic,
                    key=str(counter),
                    value=value,
                    callback=delivery_report,
                )
            producer.poll(0)
            counter += 1
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Producer shutting down...")
    finally:
        producer.flush()
        logger.info("Producer stopped.")


if __name__ == "__main__":
    main()