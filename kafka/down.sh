docker compose \
  --env-file ../.env \
  --env-file ./.env \
  down --volumes --remove-orphans
docker volume rm kafka_kafka-data

echo "$(date '+%Y-%m-%d %H:%M:%S') docker compose down done"