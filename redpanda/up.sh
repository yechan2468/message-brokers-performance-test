PRODUCER_COUNT=${1:-1}
PARTITION_COUNT=${2:-3}
CONSUMER_COUNT=${3:-1}
DELIVERY_MODE=${4}


if [ "$DELIVERY_MODE" = "EXACTLY_ONCE" ]; then
    KAFKA_ACKS_SETTING="-1" 
    KAFKA_ENABLE_IDEMPOTENCE_SETTING="true" 
    KAFKA_RETRIES_SETTING="2147483647" 
    KAFKA_CONSUMER_ISOLATION_LEVEL_SETTING="read_committed"
    KAFKA_TRANSACTIONAL_ID_SETTING="producer-tx-$$" 

elif [ "$DELIVERY_MODE" = "AT_MOST_ONCE" ]; then
    KAFKA_ACKS_SETTING="0"
    KAFKA_ENABLE_IDEMPOTENCE_SETTING="false"
    KAFKA_RETRIES_SETTING="0" # 재시도 방지
    KAFKA_CONSUMER_ISOLATION_LEVEL_SETTING="read_uncommitted"

elif [ "$DELIVERY_MODE" = "AT_LEAST_ONCE" ]; then
    KAFKA_ACKS_SETTING="-1"
    KAFKA_ENABLE_IDEMPOTENCE_SETTING="false"
    KAFKA_RETRIES_SETTING="2147483647" 
    KAFKA_CONSUMER_ISOLATION_LEVEL_SETTING="read_uncommitted"
else
    echo "Error: Invalid DELIVERY_MODE specified: $DELIVERY_MODE. Must be one of: EXACTLY_ONCE, AT_MOST_ONCE, AT_LEAST_ONCE"
    exit 1
fi 


DELIVERY_MODE=${DELIVERY_MODE} \
PARTITION_COUNT=${PARTITION_COUNT} \
docker compose \
  --env-file ../.env \
  --env-file ./.env \
  down --volumes --remove-orphans
docker volume rm redpanda_redpanda-volume


DELIVERY_MODE=${DELIVERY_MODE} \
KAFKA_ACKS=${KAFKA_ACKS_SETTING} \
KAFKA_RETRIES=${KAFKA_RETRIES_SETTING} \
KAFKA_ENABLE_IDEMPOTENCE=${KAFKA_ENABLE_IDEMPOTENCE_SETTING} \
KAFKA_PRODUCER_TRANSACTIONAL_ID=${KAFKA_TRANSACTIONAL_ID_SETTING} \
KAFKA_CONSUMER_ISOLATION_LEVEL=${KAFKA_CONSUMER_ISOLATION_LEVEL_SETTING} \
PARTITION_COUNT=${PARTITION_COUNT} \
docker compose \
  --env-file ../.env \
  --env-file ./.env \
  up -d \
  --build \
  --force-recreate \
  --scale producer=${PRODUCER_COUNT} \
  --scale consumer=${CONSUMER_COUNT}


echo "producer count: ${PRODUCER_COUNT}"
echo "partition count: ${PARTITION_COUNT}"
echo "consumer count: ${CONSUMER_COUNT}"
echo "delivery mode: ${DELIVERY_MODE}"
echo "$(date '+%Y-%m-%d %H:%M:%S') docker compose up done"
