PRODUCER_COUNT=${1:-1}
CONSUMER_COUNT=${2:-1}
PARTITION_COUNT=${3:-3} 
DELIVERY_MODE=${4:-AT_LEAST_ONCE} # [추가] DELIVERY_MODE를 4번째 인수로 받음


if [ "$DELIVERY_MODE" = "AT_MOST_ONCE" ]; then
    RABBITMQ_CONFIRM_MODE_SETTING="false" # 유실 가능
    RABBITMQ_AUTO_ACK_SETTING="true" # 중복 방지

elif [ "$DELIVERY_MODE" = "AT_LEAST_ONCE" ]; then
    RABBITMQ_CONFIRM_MODE_SETTING="true" # 유실 방지
    RABBITMQ_AUTO_ACK_SETTING="false" # 처리 실패 시 재전송, 중복 가능
else
    echo "Error: Invalid DELIVERY_MODE specified: $DELIVERY_MODE. Must be one of: AT_LEAST_ONCE, AT_MOST_ONCE"
    exit 1
fi 

DELIVERY_MODE=${DELIVERY_MODE} \
PARTITION_COUNT=${PARTITION_COUNT} \
docker compose \
  --env-file ../.env \
  --env-file ./.env \
  down --volumes --remove-orphans
docker volume rm rabbitmq_rabbitmq-data


DELIVERY_MODE=${DELIVERY_MODE} \
RABBITMQ_CONFIRM_MODE=${RABBITMQ_CONFIRM_MODE_SETTING} \
RABBITMQ_AUTO_ACK=${RABBITMQ_AUTO_ACK_SETTING} \
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
echo "partition count (queues): ${PARTITION_COUNT}"
echo "consumer count: ${CONSUMER_COUNT}"
echo "delivery mode: ${DELIVERY_MODE}"

echo "$(date '+%Y-%m-%d %H:%M:%S') docker compose up done"
