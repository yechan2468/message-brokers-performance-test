PRODUCER_COUNT=${1:-1}
PARTITION_COUNT=${2:-3}
CONSUMER_COUNT=${3:-1}
DELIVERY_MODE=${4:-AT_LEAST_ONCE}


if [ "$DELIVERY_MODE" = "EXACTLY_ONCE" ]; then
    KAFKA_ACKS_SETTING="-1" # acks='all' (Leader와 모든 ISR의 복제를 대기, default)
    KAFKA_ENABLE_IDEMPOTENCE_SETTING="true" # 메시지를 중복 없이 정확히 한 번만 보장
    KAFKA_CONSUMER_ISOLATION_LEVEL_SETTING="read_committed"
    KAFKA_RETRIES_SETTING="2147483647" 
    KAFKA_TRANSACTIONAL_ID_SETTING="producer-tx-$$" # 트랜잭션 ID

elif [ "$DELIVERY_MODE" = "AT_MOST_ONCE" ]; then
    KAFKA_ACKS_SETTING="0" # acks='0' (브로커로부터의 응답 대기 없이 전송, 실패 시 유실 가능)
    KAFKA_ENABLE_IDEMPOTENCE_SETTING="false"
    KAFKA_CONSUMER_ISOLATION_LEVEL_SETTING="read_uncommitted"
    KAFKA_RETRIES_SETTING="0"

elif [ "$DELIVERY_MODE" = "AT_LEAST_ONCE" ]; then
    KAFKA_ACKS_SETTING="-1"  # acks='all' (Leader와 모든 ISR의 복제를 대기, default)
    KAFKA_ENABLE_IDEMPOTENCE_SETTING="false"
    KAFKA_CONSUMER_ISOLATION_LEVEL_SETTING="read_uncommitted"
    KAFKA_RETRIES_SETTING="2147483647" # 메시지 전송 실패 시 무한히 재시도. 중복 가능
else
    echo "Error: Invalid DELIVERY_MODE specified: $DELIVERY_MODE.  Must be one of: EXACTLY_ONCE, AT_MOST_ONCE, AT_LEAST_ONCE"
    exit 1
fi


DELIVERY_MODE=${DELIVERY_MODE} \
PARTITION_COUNT=${PARTITION_COUNT} \
docker compose \
  --env-file ../.env \
  --env-file ./.env \
  down --volumes --remove-orphans
docker volume rm kafka_kafka-data


DELIVERY_MODE=${DELIVERY_MODE} \
KAFKA_ACKS=${KAFKA_ACKS_SETTING} \
KAFKA_RETRIES=${KAFKA_RETRIES_SETTING} \
KAFKA_ENABLE_IDEMPOTENCE=${KAFKA_ENABLE_IDEMPOTENCE_SETTING} \
KAFKA_PRODUCER_TRANSACTIONAL_ID=${KAFKA_TRANSACTIONAL_ID_SETTING} \
KAFKA_CONSUMER_ISOLATION_LEVEL=${KAFKA_CONSUMER_ISOLATION_LEVEL_SETTING} \
PARTITION_COUNT=${PARTITION_COUNT} \
PRODUCER_COUNT=${PRODUCER_COUNT} \
CONSUMER_COUNT=${CONSUMER_COUNT} \
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
