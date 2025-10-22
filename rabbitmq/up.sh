PRODUCER_COUNT=${1:-1}
CONSUMER_COUNT=${2:-1}
PARTITION_COUNT=${3:-3} 

PARTITION_COUNT=${PARTITION_COUNT} \
docker compose \
  --env-file ../.env \
  --env-file ./.env \
  down --volumes --remove-orphans


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

echo "$(date '+%Y-%m-%d %H:%M:%S') docker compose up done"
