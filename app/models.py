"""
Модуль модели сообщения KafkaMessage.

Предоставляет класс для представления сообщения,
а также методы сериализации (в JSON) и десериализации (из JSON).
"""

import json
import time
import logging

logger = logging.getLogger(__name__)


class KafkaMessage:
    """
    Класс сообщения для Kafka.

    Содержит идентификатор, текстовые данные и временную метку.
    Сериализация/десериализация выполняется в формат JSON.

    Атрибуты:
        msg_id (int): Уникальный идентификатор сообщения.
        data (str): Текстовое содержимое сообщения.
        timestamp (float): Временная метка создания (Unix time).
    """

    def __init__(self, msg_id: int, data: str, timestamp: float | None = None):
        """
        Инициализация сообщения.

        Параметры:
            msg_id: Уникальный идентификатор сообщения.
            data: Текстовые данные сообщения.
            timestamp: Временная метка. Если не указана,
                       используется текущее время.
        """
        self.msg_id = msg_id
        self.data = data
        self.timestamp = timestamp if timestamp is not None else time.time()

    def serialize(self) -> bytes | None:
        """
        Сериализует объект KafkaMessage в JSON-байты для отправки в Kafka.

        Формат JSON:
            {
                "id": <msg_id>,
                "data": "<data>",
                "timestamp": <timestamp>
            }

        Возвращает:
            bytes: Сериализованное сообщение в UTF-8.
            None: Если произошла ошибка сериализации.

        Примечание:
            В случае ошибки (например, некорректные типы данных)
            сообщение логируется, и возвращается None.
        """
        try:
            payload = {
                "id": self.msg_id,
                "data": self.data,
                "timestamp": self.timestamp,
            }
            result = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            logger.info(
                "Serialized message: id=%d, data='%s', size=%d bytes",
                self.msg_id, self.data, len(result),
            )
            return result
        except (TypeError, ValueError, OverflowError) as e:
            logger.error(
                "Serialization error for message id=%d: %s",
                self.msg_id, e,
            )
            return None

    @staticmethod
    def deserialize(raw: bytes | None) -> "KafkaMessage | None":
        """
        Десериализует JSON-байты из Kafka в объект KafkaMessage.

        Параметры:
            raw: Байтовое представление JSON (из Kafka).

        Возвращает:
            KafkaMessage: Восстановленный объект сообщения.
            None: Если произошла ошибка десериализации.

        Примечание:
            Обрабатываются следующие ошибки:
            - json.JSONDecodeError: некорректный JSON
            - KeyError: отсутствует обязательное поле (id, data)
            - UnicodeDecodeError: проблемы с кодировкой
            - TypeError: неверный тип данных
        """
        if raw is None:
            logger.error("Deserialization error: received None data")
            return None

        try:
            payload = json.loads(raw.decode("utf-8"))
            msg = KafkaMessage(
                msg_id=payload["id"],
                data=payload["data"],
                timestamp=payload.get("timestamp", time.time()),
            )
            logger.info(
                "Deserialized message: id=%d, data='%s'",
                msg.msg_id, msg.data,
            )
            return msg
        except (
            json.JSONDecodeError,
            KeyError,
            UnicodeDecodeError,
            TypeError,
        ) as e:
            logger.error(
                "Deserialization error: %s, raw data: %s",
                e, raw,
            )
            return None

    def __repr__(self) -> str:
        return (
            f"KafkaMessage(id={self.msg_id}, data='{self.data}', "
            f"timestamp={self.timestamp})"
        )