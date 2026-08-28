#!/usr/bin/env bash
# ============================================================================
# Генерация всех сертификатов, Keystore и Truststore для кластера Kafka (SSL/mTLS)
#
# Что создаётся:
#   - самоподписанный корневой CA  (ca.key, ca.crt)
#   - для каждого брокера (kafka-1/2/3):
#       приватный ключ, CSR, подписанный сертификат,
#       PKCS12 Keystore, JKS Truststore (с CA) и файлы паролей (_creds)
#   - для каждого клиента (admin/producer/consumer):
#       приватный ключ, сертификат, PKCS12 Keystore для Java-инструментов
#   - общий Truststore для клиентов (client-creds/truststore.jks)
#
# Запуск:  bash scripts/generate_certs.sh
# Скрипт можно перенести на другой компьютер и перегенерировать всё заново.
# ============================================================================

set -euo pipefail

PASS="kafka-password"
CA_CN="yandex-practice-kafka-ca"
DAYS=3650
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$BASE_DIR"

BROKERS=(kafka-1 kafka-2 kafka-3)
CLIENTS=(admin producer consumer)

mkdir -p kafka-1-creds kafka-2-creds kafka-3-creds client-creds

echo ">>> 1. Корневой центр сертификации (CA)"
if [[ ! -f ca.key || ! -f ca.crt ]]; then
  openssl req -new -x509 \
    -keyout ca.key -out ca.crt -days "$DAYS" -nodes \
    -subj "/C=RU/O=Yandex/OU=Practice/L=Moscow/CN=$CA_CN"
fi
cp ca.crt ca.pem
echo "    CA готов: ca.key, ca.crt, ca.pem"

echo ">>> 2. Сертификаты брокеров"
for b in "${BROKERS[@]}"; do
  D="$b-creds"
  echo "    -- брокер $b"

  openssl genrsa -out "$D/$b.key" 2048

  openssl req -new \
    -key "$D/$b.key" -out "$D/$b.csr" \
    -subj "/C=RU/O=Yandex/OU=Practice/L=Moscow/CN=$b"

  cat > "$D/ext-broker.cnf" <<EOF
subjectAltName=DNS:$b,DNS:$b-external,DNS:localhost
extendedKeyUsage=serverAuth,clientAuth
keyUsage=digitalSignature,keyEncipherment
EOF

  openssl x509 -req \
    -in "$D/$b.csr" \
    -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out "$D/$b.crt" -days "$DAYS" -sha256 \
    -extfile "$D/ext-broker.cnf"

  # Промежуточный PKCS12 Keystore брокера (сертификат + ключ)
  openssl pkcs12 -export \
    -in "$D/$b.crt" -inkey "$D/$b.key" -certfile ca.crt -name "$b" \
    -out "$D/kafka.$b.keystore.pkcs12" -passout pass:"$PASS"

  # JKS Keystore брокера (импорт из PKCS12) — имя требуется Bitnami Kafka
  rm -f "$D/kafka.keystore.jks"
  keytool -importkeystore \
    -srckeystore "$D/kafka.$b.keystore.pkcs12" -srcstoretype PKCS12 \
    -srcstorepass "$PASS" \
    -destkeystore "$D/kafka.keystore.jks" -deststoretype JKS \
    -deststorepass "$PASS" -noprompt

  # JKS Truststore брокера (содержит доверенный CA) — имя требуется Bitnami Kafka
  rm -f "$D/kafka.truststore.jks"
  keytool -importcert -alias ca -file ca.crt \
    -keystore "$D/kafka.truststore.jks" -storetype JKS \
    -storepass "$PASS" -noprompt

  # Псевдонимы для справки/совместимости
  cp "$D/kafka.truststore.jks" "$D/kafka.$b.truststore.jks"

  # Файлы паролей для Bitnami Kafka
  echo "$PASS" > "$D/${b}_keystore_creds"
  echo "$PASS" > "$D/${b}_sslkey_creds"
  echo "$PASS" > "$D/${b}_truststore_creds"

  echo "    Keystore (JKS) и Truststore для $b созданы (пароль: $PASS)"
done

echo ">>> 3. Сертификаты клиентов (admin / producer / consumer)"
for c in "${CLIENTS[@]}"; do
  echo "    -- клиент $c"

  openssl genrsa -out "client-creds/$c.key" 2048

  openssl req -new \
    -key "client-creds/$c.key" -out "client-creds/$c.csr" \
    -subj "/C=RU/O=Yandex/OU=Practice/L=Moscow/CN=$c"

  cat > "client-creds/ext-client.cnf" <<EOF
extendedKeyUsage=clientAuth
EOF

  openssl x509 -req \
    -in "client-creds/$c.csr" \
    -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out "client-creds/$c.crt" -days "$DAYS" -sha256 \
    -extfile "client-creds/ext-client.cnf"

  # PKCS12 Keystore клиента (для Java-инструментов: kafka-topics.sh, kafka-acls.sh)
  openssl pkcs12 -export \
    -in "client-creds/$c.crt" -inkey "client-creds/$c.key" -certfile ca.crt -name "$c" \
    -out "client-creds/$c.keystore.p12" -passout pass:"$PASS"

  echo "    Сертификат и Keystore для $c созданы"
done

# Общий Truststore для клиентов (доверенный CA)
rm -f client-creds/truststore.jks
keytool -importcert -alias ca -file ca.crt \
  -keystore client-creds/truststore.jks -storetype JKS \
  -storepass "$PASS" -noprompt

# Копия CA для клиентских приложений (librdkafka работает с PEM)
cp ca.crt client-creds/ca.crt

rm -f client-creds/ext-client.cnf

echo ">>> Готово. Все сертификаты, Keystore и Truststore созданы."