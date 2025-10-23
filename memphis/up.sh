PRODUCER_COUNT=${1:-1}
PARTITION_COUNT=${2:-3}
CONSUMER_COUNT=${3:-1}


PARTITION_COUNT=${PARTITION_COUNT} \
docker compose \
  --env-file ../.env \
  --env-file ./.env \
  down --volumes --remove-orphans
docker volume rm memphis_memphis-postgres-data


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
echo "$(date '+%Y-%m-%d %H:%M:%S') docker compose up done"
