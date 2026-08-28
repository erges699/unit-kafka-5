#!/usr/bin/env bash
# ============================================================================
# Настройка кластера: создание топиков topic-1, topic-2 и применение ACL.
#
# Выполняется в контейнере (сервис `setup`) с использованием сертификата admin
# (супер-пользователь, определён в KAFKA_CFG_SUPER_USERS=User:admin).
#
# Модель авторизации: allow.everyone.if.no.acl.found=true (разрешено по умолчанию)
# + явные DENY-ACL для ограничений.
#
# Логика прав доступа (см. задание):
#   topic-1 — доступен и продюсерам, и консьюмерам (явные ALLOW для User:*)
#   topic-2 — только продюсеры могут писать;
#             консьюмер НЕ имеет доступа к чтению (явный DENY Read/Describe)
# ============================================================================

set -euo pipefail

CERT_DIR=/certs
PASS=kafka-password
BOOTSTRAP=kafka-1:9092
CONFIG=/tmp/admin.properties

cat > "$CONFIG" <<EOF
security.protocol=SSL
ssl.truststore.location=$CERT_DIR/truststore.jks
ssl.truststore.password=$PASS
ssl.keystore.location=$CERT_DIR/admin.keystore.p12
ssl.keystore.type=PKCS12
ssl.keystore.password=$PASS
ssl.key.password=$PASS
EOF

TOPIC_TOOL=(kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config "$CONFIG")
ACL_TOOL=(kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config "$CONFIG")

echo "Ожидание готовности брокеров..."
until "${TOPIC_TOOL[@]}" --list >/dev/null 2>&1; do
  sleep 3
done
echo "Брокеры готовы."

echo ">>> Создание топиков"
for topic in topic-1 topic-2; do
  echo "  -- создаю $topic (partitions=3, replication-factor=3)"
  "${TOPIC_TOOL[@]}" --create --if-not-exists \
    --topic "$topic" --partitions 3 --replication-factor 3
done

echo ">>> Настройка ACL"

echo "  -- topic-1: открыт для всех (продюсеры и консьюмеры)"
"${ACL_TOOL[@]}" --add \
  --allow-principal 'User:*' \
  --operation Read --operation Write --operation Describe \
  --topic topic-1

echo "  -- topic-1: чтение из любой группы консьюмеров"
"${ACL_TOOL[@]}" --add \
  --allow-principal 'User:*' \
  --operation Read \
  --group '*'

echo "  -- topic-2: запись разрешена продюсеру (User:producer)"
"${ACL_TOOL[@]}" --add \
  --allow-principal 'User:producer' \
  --operation Write --operation Describe \
  --topic topic-2

echo "  -- topic-2: ЗАПРЕТ чтения и описания для консьюмера (User:consumer)"
"${ACL_TOOL[@]}" --add \
  --deny-principal 'User:consumer' \
  --operation Read --operation Describe \
  --topic topic-2

echo ">>> Проверка ACL"
"${ACL_TOOL[@]}" --list

echo ">>> Настройка завершена."