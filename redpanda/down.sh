docker compose \
  --env-file ../.env \
  --env-file ./.env \
  down --volumes --remove-orphans
docker volume rm redpanda_redpanda-volume

echo "$(date '+%Y-%m-%d %H:%M:%S') docker compose down done"