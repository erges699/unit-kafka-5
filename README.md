# Kafka-кластер (KRaft) с SSL/mTLS и ACL

Защищённый кластер Apache Kafka из трёх брокеров в Docker Compose
с взаимной TLS-аутентификацией, двумя топиками и правами доступа (ACL).

## Архитектура

- **Kafka 3.7** (KRaft, без ZooKeeper), образ `bitnamilegacy/kafka:3.7`
- Три брокера: `kafka-1` (broker + controller), `kafka-2`, `kafka-3` (broker)
- Все соединения зашифрованы по TLS (mTLS, `ssl.client.auth=required`)

| Лисенер      | Протокол | Порт (в сети) | Назначение                    |
|--------------|----------|---------------|-------------------------------|
| `INTERNAL`   | SSL      | 9092          | межброкерное / внутреннее     |
| `CONTROLLER` | PLAINTEXT| 9093          | KRaft-кворум (на kafka-1)     |
| `EXTERNAL`   | SSL      | 9094/95/96    | внешние клиенты               |

## Принципалы (из CN сертификата)

| Клиент  | CN        | Роль                                   |
|---------|-----------|----------------------------------------|
| admin   | `admin`   | супер-пользователь (настройка)         |
| producer| `producer`| отправка сообщений                     |
| consumer| `consumer`| чтение сообщений                       |

`ssl.principal.mapping.rules` преобразует DN сертификата `CN=...` в короткий
принципал (`User:producer`, `User:consumer`).

## Топики и ACL

| Топик    | Продюсеры            | Консьюмеры                                             |
|----------|----------------------|--------------------------------------------------------|
| topic-1  | разрешено            | разрешено (ALLOW для `User:*`)                         |
| topic-2  | разрешено            | **запрещено** (DENY Read/Describe для `User:consumer`) |

Модель авторизации: `allow.everyone.if.no.acl.found=true` (разрешено по
умолчанию) + явные DENY-ACL для ограничений (см. `scripts/setup_cluster.sh`).
Такой подход не блокирует внутренние KRaft-запросы и упрощает кластеризацию.

## Структура проекта

```
docker-compose.yaml          # кластер: 3 брокера (SSL/KRaft), setup, producer, consumer
scripts/
  generate_certs.sh          # генерация CA, keystore/truststore брокеров и клиентов
  setup_cluster.sh           # создание топиков topic-1/topic-2 и ACL
ca.crt ca.key ca.pem ca.srl  # корневой центр сертификации
kafka-1-creds/               # сертификаты/keystore/truststore брокера kafka-1
kafka-2-creds/               # сертификаты/keystore/truststore брокера kafka-2
kafka-3-creds/               # сертификаты/keystore/truststore брокера kafka-3
client-creds/                # сертификаты клиентов admin/producer/consumer + truststore
app/
  Dockerfile requirements.txt
  producer.py                # SSL-продюсер (topic-1, topic-2)
  consumer.py                # SSL-консьюмер (topic-1 разрешён, topic-2 запрещён)
  models.py
```

Каталоги `kafka-*-creds/` и `client-creds/` содержат все сертификаты и ключи,
необходимые для запуска сервисов на другом компьютере. Для полного
воспроизведения можно перегенерировать их скриптом `scripts/generate_certs.sh`.

Пароль всех keystore/truststore: `kafka-password`.

## Запуск

```bash
# 1. (опционально) перегенерировать сертификаты с нуля
bash scripts/generate_certs.sh

# 2. поднять кластер и сервис настройки (топики + ACL)
docker compose up -d --build kafka-1 kafka-2 kafka-3 setup

# 3. дождаться завершения настройки
docker compose logs -f setup

# 4. запустить продюсер и консьюмер
docker compose up -d --no-deps producer consumer

# 5. наблюдать за логами
docker compose logs -f producer consumer
```

## Проверка результата

- **Продюсер** доставляет сообщения в `topic-1` и `topic-2` (SSL, принципал `producer`).
- **Консьюмер** успешно читает `topic-1`, а для `topic-2` получает отказ
  авторизации: «Доступ запрещён к топику 'topic-2' — ACL корректно блокирует чтение».

Дополнительно можно выполнить вручную:

```bash
# список топиков (admin — супер-пользователь)
docker exec kafka-setup bash -c '
  cat > /tmp/a.properties <<EOF
security.protocol=SSL
ssl.truststore.location=/certs/truststore.jks
ssl.truststore.password=kafka-password
ssl.keystore.location=/certs/admin.keystore.p12
ssl.keystore.type=PKCS12
ssl.keystore.password=kafka-password
ssl.key.password=kafka-password
EOF
  kafka-topics.sh --bootstrap-server kafka-1:9092 --command-config /tmp/a.properties --list
  kafka-acls.sh --bootstrap-server kafka-1:9092 --command-config /tmp/a.properties --list
'
```

Остановка: `docker compose down` (с удалением данных: `docker compose down -v`).